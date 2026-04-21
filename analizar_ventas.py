"""
Pipeline de Análisis de Ventas — Carlos Isla y Cía
NET-LogistK ISLA

Uso:
    python analizar_ventas.py <archivo.xlsx> <periodo>
    python analizar_ventas.py Estadisticas_Ventas_2026-04.xlsx 2026-04

Genera en Firestore bajo indicadores/comercial:
    - indicadores/comercial/ventas_analytics/{periodo}                  → totales + sucursales
    - indicadores/comercial/ventas_analytics/{periodo}/vendedores/{id}  → detalle por vendedor
    - indicadores/comercial/clientes_analytics/{periodo}                → métricas de clientes
    - indicadores/comercial/articulos_analytics/{periodo}               → métricas de artículos
    - indicadores/comercial/config/adn-weights                          → pesos del score
"""

import sys
import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from typing import Dict, List, Tuple, Any, Optional
from config import (
    PV_SUCURSAL, PV_EXCLUIR, SUCURSALES_CODSUC, SUFIJO_SUCURSAL, FACTURAS, NOTAS_CREDITO, NOTAS_DEBITO,
    CANAL_WEB, FAMILIA_FLETE, SCORE_WEIGHTS, SCORE_METHOD, MIN_TXNS_SCORE,
    BADGES, UMBRALES, FIREBASE_CREDENTIALS_PATH, ROOT_COL, AREA_DOC, SUBCOLLECTIONS
)

# ============================================================================
# CARGA Y VALIDACIÓN
# ============================================================================

def cargar_xlsx(path: str) -> pd.DataFrame:
    """Carga el XLSX de ventas y valida columnas mínimas."""
    print(f"📂 Cargando {path}...")
    df = pd.read_excel(path)
    print(f"  ✓ {len(df):,} filas, {len(df.columns)} columnas")

    required = [
        'Fec. Comp.', 'Nom Sis TCOM', 'Número', 'Cod SUC', 'Cliente',
        'Nombre Cliente', 'Vendedor', 'Nombre vendedor', 'Vendedor Cliente',
        'Nombre Vendedor Cliente', 'Nombre rubro', 'Grupo Articulo',
        'Nombre familia', 'Cantidad Vendida', 'Precio', 'Neto', 'Costo',
        'Kgs', 'Dto/Rec', 'Nom DEP', 'Zona Reparto', 'Porc Comision',
        'Nombre marca', 'Nombre Grupo', 'Clasificador Comprobante'
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"  ✗ Columnas faltantes: {missing}")
        sys.exit(1)

    print(f"  ✓ Validación de columnas OK")
    return df


def limpiar_datos(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia y normaliza los datos."""
    print("🧹 Limpiando datos...")

    # Normalizar Vendedor (int→string) y Vendedor Cliente (trim)
    df['Vendedor'] = df['Vendedor'].astype(str).str.strip()
    df['Vendedor Cliente'] = df['Vendedor Cliente'].astype(str).str.strip()

    # Trim Nom DEP (tiene espacios al inicio)
    df['Nom DEP'] = df['Nom DEP'].astype(str).str.strip()

    # Trim nombre vendedor
    df['Nombre vendedor'] = df['Nombre vendedor'].astype(str).str.strip()
    df['Nombre Vendedor Cliente'] = df['Nombre Vendedor Cliente'].astype(str).str.strip()

    # Normalizar Nombre Grupo (tiene prefijos inconsistentes)
    df['Nombre Grupo'] = df['Nombre Grupo'].fillna('SIN GRUPO')
    df['Nombre marca'] = df['Nombre marca'].fillna('Sin marca')

    # Fecha
    df['Fec. Comp.'] = pd.to_datetime(df['Fec. Comp.'])

    # Neto y Costo numéricos
    df['Neto'] = pd.to_numeric(df['Neto'], errors='coerce').fillna(0)
    df['Costo'] = pd.to_numeric(df['Costo'], errors='coerce').fillna(0)
    df['Kgs'] = pd.to_numeric(df['Kgs'], errors='coerce').fillna(0)
    df['Cantidad Vendida'] = pd.to_numeric(df['Cantidad Vendida'], errors='coerce').fillna(0)
    df['Dto/Rec'] = pd.to_numeric(df['Dto/Rec'], errors='coerce').fillna(0)

    # Extraer Punto de Venta (PV) del campo Número
    # Formato: "A00045-00140473" → PV = 45
    df['PV'] = pd.to_numeric(
        df['Número'].astype(str).str.extract(r'(\d+)-')[0],
        errors='coerce'
    ).fillna(0).astype(int)

    # Excluir PVs de movimientos internos (ajustes, cheques rechazados, etc.)
    filas_antes = len(df)
    df = df[~df['PV'].isin(PV_EXCLUIR)]
    excluidas = filas_antes - len(df)
    if excluidas > 0:
        print(f"  → Excluidas {excluidas} filas de PVs internos {PV_EXCLUIR}")

    # Asignar sucursal desde PV (coincide con reporte Nexion)
    df['sucursal'] = df['PV'].map(PV_SUCURSAL)
    # Fallback: si PV no está mapeado, usar Cod SUC
    sin_suc = df['sucursal'].isna()
    if sin_suc.any():
        df.loc[sin_suc, 'sucursal'] = df.loc[sin_suc, 'Cod SUC'].map(SUCURSALES_CODSUC)
        df['sucursal'] = df['sucursal'].fillna('SIN ASIGNAR')
        pv_sin_mapear = df[sin_suc]['PV'].unique()
        print(f"  ⚠ PVs sin mapear (usando Cod SUC como fallback): {pv_sin_mapear}")

    # Clasificar tipo comprobante
    df['es_factura'] = df['Nom Sis TCOM'].isin(FACTURAS)
    df['es_nc'] = df['Nom Sis TCOM'].isin(NOTAS_CREDITO)
    df['es_nd'] = df['Nom Sis TCOM'].isin(NOTAS_DEBITO)
    df['es_flete'] = df['Nombre familia'].str.strip().str.upper() == FAMILIA_FLETE.upper()
    df['es_web'] = df['Clasificador Comprobante'] == CANAL_WEB

    nulls_dep = df['Nom DEP'].isna().sum()
    nulls_zona = df['Zona Reparto'].isna().sum()
    print(f"  ✓ Normalizado. Nulls: Nom DEP={nulls_dep}, Zona Reparto={nulls_zona}")
    return df


# ============================================================================
# ANÁLISIS DE VENDEDORES (ADN)
# ============================================================================

def calcular_metricas_vendedor(df: pd.DataFrame) -> List[Dict]:
    """Calcula todas las métricas por vendedor."""
    print("📊 Calculando métricas por vendedor...")

    # Todos los comprobantes (FAC + ND + NC)
    # NC ya tiene Neto negativo desde Nexion, se incluye en el cálculo
    df_todos = df[df['es_factura'] | df['es_nd'] | df['es_nc']]
    df_solo_fac = df[df['es_factura'] | df['es_nd']]  # Solo para contar txns de factura
    df_nc = df[df['es_nc']]

    vendedores = []
    grupos = df_todos.groupby('Vendedor')

    for vid, gv in grupos:
        nombre = gv['Nombre vendedor'].iloc[0]
        sucursal = gv['sucursal'].mode().iloc[0] if not gv['sucursal'].mode().empty else ''

        # Neto REAL = FAC + ND + NC (NC ya viene negativo)
        neto = gv['Neto'].sum()
        costo = gv['Costo'].sum()  # NC costo ya es negativo
        margen_abs = neto - costo
        margen_pct = (margen_abs / neto * 100) if neto > 0 else 0
        markup = (neto / costo) if costo > 0 else 0  # Ventas/Costo (ej: 1.30 = 30% recargo)

        # Txns: contar solo facturas (no NC como transacciones)
        gv_fac = gv[gv['es_factura'] | gv['es_nd']]
        txns = gv_fac['Número'].nunique()
        clientes = gv['Cliente'].nunique()
        ticket_prom = neto / txns if txns > 0 else 0
        kgs = gv['Kgs'].sum()
        dias_activos = gv['Fec. Comp.'].dt.date.nunique()
        rubros_count = gv[gv['es_factura']]['Nombre rubro'].nunique()
        dto_promedio = gv_fac['Dto/Rec'].mean() if len(gv_fac) > 0 else 0

        # NC del vendedor (separado para diagnóstico)
        nc_vendedor = gv[gv['es_nc']]
        nc_count = nc_vendedor['Número'].nunique()
        nc_monto = nc_vendedor['Neto'].sum()  # Negativo

        # Fletes (solo de facturas, no de NC)
        flete_lineas = gv_fac[gv_fac['es_flete']]
        flete_count = len(flete_lineas)          # Cantidad de envíos (líneas de flete)
        flete_neto = flete_lineas['Neto'].sum()  # $ total cobrado en fletes
        flete_per_kg = (flete_neto / kgs) if kgs > 0 else 0  # $/kg — MÉTRICA PRINCIPAL
        flete_pct_neto = (flete_neto / neto * 100) if neto > 0 else 0  # Flete como % de facturación
        facturas_con_flete = flete_lineas['Número'].nunique()
        total_facturas = txns
        pct_facturas_con_flete = (facturas_con_flete / total_facturas * 100) if total_facturas > 0 else 0

        # Web
        web_lineas = gv_fac[gv_fac['es_web']]
        web_neto = web_lineas['Neto'].sum()
        web_txns = web_lineas['Número'].nunique()
        web_pct = (web_neto / neto * 100) if neto > 0 else 0

        # HHI Concentración por cliente
        cli_netos = gv.groupby('Cliente')['Neto'].sum()
        total_cli_neto = cli_netos.sum()
        if total_cli_neto > 0:
            shares = (cli_netos / total_cli_neto * 100) ** 2
            hhi = shares.sum()
        else:
            hhi = 0

        # Rubros top (top 8)
        rubros_top = dict(
            gv.groupby('Nombre rubro')['Neto'].sum()
            .sort_values(ascending=False).head(8)
        )

        # Marcas top (top 5)
        marcas_top = dict(
            gv.groupby('Nombre marca')['Neto'].sum()
            .sort_values(ascending=False).head(5)
        )

        # Grupos de cliente
        grupos_cliente = dict(
            gv.groupby('Nombre Grupo')['Neto'].sum()
            .sort_values(ascending=False)
        )

        # Top clientes (top 10)
        top_clients = []
        cli_detail = gv.groupby(['Cliente', 'Nombre Cliente']).agg(
            neto=('Neto', 'sum'), txns=('Número', 'nunique')
        ).sort_values('neto', ascending=False).head(10).reset_index()
        for _, row in cli_detail.iterrows():
            top_clients.append({
                'Cliente': row['Cliente'],
                'Nombre Cliente': row['Nombre Cliente'],
                'neto': float(row['neto']),
                'txns': int(row['txns']),
            })

        # Cross-sell: Vendedor vs Vendedor Cliente (incluye NC para neto real)
        cross = calcular_cross_sell(df_todos, vid, nombre)

        # Cross incoming (otros vendedores que venden a MIS clientes)
        cross_incoming = calcular_cross_incoming(df_todos, vid)

        # Monthly breakdown (pasa todo + NC separado)
        monthly = calcular_monthly(gv_fac, nc_vendedor)

        vendedores.append({
            'id': str(vid),
            'nombre': nombre,
            'sucursal': sucursal,
            'neto': float(neto),
            'costo': float(costo),
            'margen_pct': round(float(margen_pct), 1),
            'margen_abs': float(margen_abs),
            'markup': round(float(markup), 2),
            'txns': int(txns),
            'clientes': int(clientes),
            'ticket_prom': float(ticket_prom),
            'dias_activos': int(dias_activos),
            'dto_promedio': round(float(dto_promedio), 1),
            'kgs': round(float(kgs), 1),
            'rubros_count': int(rubros_count),
            'nc_count': int(nc_count),
            'nc_monto': float(nc_monto),
            'flete_count': int(flete_count),
            'flete_neto': float(flete_neto),
            'flete_per_kg': round(float(flete_per_kg), 1),
            'flete_pct_neto': round(float(flete_pct_neto), 2),
            'facturas_con_flete': int(facturas_con_flete),
            'total_facturas': int(total_facturas),
            'pct_facturas_con_flete': round(float(pct_facturas_con_flete), 1),
            'web_neto': float(web_neto),
            'web_txns': int(web_txns),
            'web_pct': round(float(web_pct), 1),
            'hhi': round(float(hhi), 0),
            'rubros_top': {k: float(v) for k, v in rubros_top.items()},
            'marcas_top': {k: float(v) for k, v in marcas_top.items()},
            'grupos_cliente': {k: float(v) for k, v in grupos_cliente.items()},
            'top_clients': top_clients,
            'cross': cross,
            'cross_incoming': cross_incoming,
            'monthly': monthly,
        })

    print(f"  ✓ {len(vendedores)} vendedores procesados")
    return vendedores


def calcular_cross_sell(df_fac: pd.DataFrame, vid: str, vnombre: str) -> Dict:
    """
    Calcula cross-sell: cuánto vende el vendedor a clientes PROPIOS
    vs clientes asignados a OTROS vendedores.
    """
    mis_ventas = df_fac[df_fac['Vendedor'] == vid]

    own = mis_ventas[mis_ventas['Vendedor Cliente'] == vid]
    others = mis_ventas[mis_ventas['Vendedor Cliente'] != vid]

    own_neto = float(own['Neto'].sum())
    others_neto = float(others['Neto'].sum())
    total = own_neto + others_neto
    pct_own = (own_neto / total * 100) if total > 0 else 100

    # Detalle: a qué vendedores les está vendiendo
    selling_to = []
    if len(others) > 0:
        by_vc = others.groupby(['Vendedor Cliente', 'Nombre Vendedor Cliente']).agg(
            txns=('Número', 'nunique'), neto=('Neto', 'sum')
        ).sort_values('neto', ascending=False).head(10).reset_index()
        for _, row in by_vc.iterrows():
            selling_to.append({
                'vc': str(row['Vendedor Cliente']),
                'vc_nombre': row['Nombre Vendedor Cliente'],
                'txns': int(row['txns']),
                'neto': float(row['neto']),
            })

    return {
        'own_neto': own_neto,
        'others_neto': others_neto,
        'pct_own': round(pct_own, 1),
        'selling_to': selling_to,
    }


def calcular_cross_incoming(df_fac: pd.DataFrame, vid: str) -> List[Dict]:
    """
    Calcula qué otros vendedores venden a MIS clientes
    (clientes donde Vendedor Cliente == vid pero Vendedor != vid).
    """
    incoming = df_fac[
        (df_fac['Vendedor Cliente'] == vid) &
        (df_fac['Vendedor'] != vid)
    ]
    if len(incoming) == 0:
        return []

    by_v = incoming.groupby(['Vendedor', 'Nombre vendedor']).agg(
        txns=('Número', 'nunique'), neto=('Neto', 'sum')
    ).sort_values('neto', ascending=False).head(10).reset_index()

    result = []
    for _, row in by_v.iterrows():
        result.append({
            'v': str(row['Vendedor']),
            'v_nombre': row['Nombre vendedor'],
            'txns': int(row['txns']),
            'neto': float(row['neto']),
        })
    return result


def calcular_monthly(gv_fac: pd.DataFrame, gv_nc: pd.DataFrame) -> Dict:
    """Agrupa métricas por mes para evolución."""
    meses_map = {1:'ENE',2:'FEB',3:'MAR',4:'ABR',5:'MAY',6:'JUN',
                 7:'JUL',8:'AGO',9:'SEP',10:'OCT',11:'NOV',12:'DIC'}
    result = {}

    for mes_num, label in meses_map.items():
        mv = gv_fac[gv_fac['Fec. Comp.'].dt.month == mes_num]
        if len(mv) == 0:
            continue

        nc_mes = gv_nc[gv_nc['Fec. Comp.'].dt.month == mes_num] if len(gv_nc) > 0 else pd.DataFrame()

        neto = mv['Neto'].sum()
        costo = mv['Costo'].sum()
        markup = (neto / costo) if costo > 0 else 0
        txns = mv['Número'].nunique()
        clientes = mv['Cliente'].nunique()
        dias = mv['Fec. Comp.'].dt.date.nunique()
        kgs = mv['Kgs'].sum()
        ticket = neto / txns if txns > 0 else 0

        flete_mv = mv[mv['es_flete']]
        flete_count = len(flete_mv)
        flete_neto = flete_mv['Neto'].sum()
        flete_per_kg = (flete_neto / kgs) if kgs > 0 else 0

        web_mv = mv[mv['es_web']]
        web_neto = web_mv['Neto'].sum()
        web_txns = web_mv['Número'].nunique()

        result[label] = {
            'neto': float(neto),
            'costo': float(costo),
            'markup': round(float(markup), 2),
            'txns': int(txns),
            'clientes': int(clientes),
            'ticket_prom': float(ticket),
            'dias': int(dias),
            'kgs': round(float(kgs), 1),
            'flete_count': int(flete_count),
            'flete_neto': float(flete_neto),
            'flete_per_kg': round(float(flete_per_kg), 1),
            'web_neto': float(web_neto),
            'web_txns': int(web_txns),
        }
    return result


# ============================================================================
# SCORE ADN
# ============================================================================

def calcular_score_adn(vendedores: List[Dict]) -> List[Dict]:
    """Calcula score ADN por percentiles y asigna badges."""
    print("🎯 Calculando Score ADN por percentiles...")

    # Solo vendedores con >= MIN_TXNS_SCORE transacciones
    elegibles = [v for v in vendedores if v['txns'] >= MIN_TXNS_SCORE]
    inactivos = [v for v in vendedores if v['txns'] < MIN_TXNS_SCORE]

    if not elegibles:
        print("  ⚠ No hay vendedores elegibles (todos con <10 txns)")
        return vendedores

    # Extraer arrays para percentiles
    netos = np.array([v['neto'] for v in elegibles])
    margenes = np.array([v['margen_pct'] for v in elegibles])
    clientes_arr = np.array([v['clientes'] for v in elegibles])
    rubros_arr = np.array([v['rubros_count'] for v in elegibles])
    dias_arr = np.array([v['dias_activos'] for v in elegibles])

    # Calidad: ratio NC/txns (menor es mejor → invertir)
    nc_ratios = np.array([v['nc_count'] / v['txns'] if v['txns'] > 0 else 0 for v in elegibles])

    def percentil_rank(arr):
        """Calcula el percentil de cada valor (0-100)."""
        from scipy.stats import rankdata
        ranks = rankdata(arr, method='average')
        return ((ranks - 1) / max(len(ranks) - 1, 1)) * 100

    p_facturacion = percentil_rank(netos)
    p_rentabilidad = percentil_rank(margenes)
    p_cartera = percentil_rank(clientes_arr)
    p_diversidad = percentil_rank(rubros_arr)
    p_actividad = percentil_rank(dias_arr)
    p_calidad = 100 - percentil_rank(nc_ratios)  # Invertido: menor NC = mejor

    w = SCORE_WEIGHTS
    for i, v in enumerate(elegibles):
        comp = {
            'facturacion': round(float(p_facturacion[i]), 0),
            'rentabilidad': round(float(p_rentabilidad[i]), 0),
            'cartera': round(float(p_cartera[i]), 0),
            'diversidad': round(float(p_diversidad[i]), 0),
            'actividad': round(float(p_actividad[i]), 0),
            'calidad': round(float(p_calidad[i]), 0),
        }
        score = (
            comp['facturacion'] * w['facturacion'] +
            comp['rentabilidad'] * w['rentabilidad'] +
            comp['cartera'] * w['cartera'] +
            comp['diversidad'] * w['diversidad'] +
            comp['actividad'] * w['actividad'] +
            comp['calidad'] * w['calidad']
        )
        v['adn_score'] = round(score)
        v['score_components'] = comp

        # Badge
        if score >= 65:
            v['badge'] = 'estrella'
            v['badge_label'] = 'Estrella'
        elif score >= 45:
            v['badge'] = 'estable'
            v['badge_label'] = 'Estable'
        elif score >= 25:
            v['badge'] = 'atencion'
            v['badge_label'] = 'Atención'
        else:
            v['badge'] = 'riesgo'
            v['badge_label'] = 'En riesgo'

    # Inactivos
    for v in inactivos:
        v['adn_score'] = 0
        v['score_components'] = {k: 0 for k in SCORE_WEIGHTS.keys()}
        v['badge'] = 'inactivo'
        v['badge_label'] = 'Inactivo'

    all_v = elegibles + inactivos
    estrellas = sum(1 for v in all_v if v['badge'] == 'estrella')
    riesgo = sum(1 for v in all_v if v['badge'] in ('riesgo', 'atencion'))
    print(f"  ✓ {len(elegibles)} elegibles, {len(inactivos)} inactivos")
    print(f"  ✓ ★ Estrellas: {estrellas} | ⚠ Críticos: {riesgo}")
    return all_v


# ============================================================================
# PATTERNS, RISK FACTORS, RECOMMENDATIONS
# ============================================================================

def asignar_patterns_y_riesgos(vendedores: List[Dict]) -> List[Dict]:
    """Detecta patrones, factores de riesgo y genera recomendaciones."""
    print("🔍 Detectando patterns y risk factors...")
    u = UMBRALES

    for v in vendedores:
        patterns = []
        risks = []
        recs = []

        if v['txns'] < MIN_TXNS_SCORE:
            v['patterns'] = ['Inactivo']
            v['risk_factors'] = []
            v['recommendations'] = ['Evaluar permanencia o reasignación']
            continue

        # Patterns
        if v['margen_pct'] >= 35:
            patterns.append('Alta rentabilidad')
        if v['margen_pct'] < 20:
            patterns.append('Margen bajo')
        if v['clientes'] <= 5:
            patterns.append('Concentrado')
        if v['flete_per_kg'] >= u['flete_alto_per_kg']:
            patterns.append('Cobra bien el flete')
        if v['flete_per_kg'] < u['flete_bajo_per_kg'] and v['kgs'] > u['flete_bajo_kgs_min']:
            patterns.append('No cobra flete')
        if v['web_pct'] > 5:
            patterns.append('Canal WEB')
        if v['hhi'] > u['hhi_alto']:
            patterns.append('Alto riesgo concentración')
        if v['rubros_count'] >= 10:
            patterns.append('Diversificado')
        if v['rubros_count'] <= 4:
            patterns.append('Especializado')
        if v['nc_count'] > u['nc_frecuentes_min']:
            patterns.append('Devoluciones frecuentes')

        # Constante (si tiene multi-mes y no varía mucho)
        meses = v.get('monthly', {})
        if len(meses) >= 2:
            netos_mes = [m['neto'] for m in meses.values()]
            if len(netos_mes) >= 2 and min(netos_mes) > 0:
                cv = np.std(netos_mes) / np.mean(netos_mes)
                if cv < 0.3:
                    patterns.append('Constante')

        # Risk factors
        if v['margen_pct'] < u['margen_bajo_pct'] and v['neto'] > u['margen_bajo_neto_min']:
            risks.append('Margen bajo con volumen alto')
        if v['clientes'] <= u['concentracion_clientes_max'] and v['neto'] > u['concentracion_neto_min']:
            risks.append('Alta concentración de clientes')
        if v['hhi'] > u['hhi_alto']:
            risks.append(f'HHI alto ({int(v["hhi"])})')
        if v['nc_count'] > u['nc_frecuentes_min']:
            risks.append(f'NC frecuentes ({v["nc_count"]})')
        if v['flete_per_kg'] < u['flete_bajo_per_kg'] and v['kgs'] > u['flete_bajo_kgs_min']:
            risks.append(f'Flete bajo (${v["flete_per_kg"]:.1f}/kg)')

        # Recommendations
        if v['flete_per_kg'] < u['capacitar_flete_per_kg'] and v['kgs'] > u['flete_bajo_kgs_min']:
            recs.append('Revisar cobro de flete')
        if v['rubros_count'] <= u['ampliar_mix_rubros']:
            recs.append('Ampliar mix de productos')
        if v['clientes'] <= u['diversificar_clientes'] and v['neto'] > u['concentracion_neto_min']:
            recs.append('Diversificar cartera de clientes')
        if v['margen_pct'] < u['revisar_descuentos_margen']:
            recs.append('Revisar política de descuentos')
        if v['cross']['pct_own'] < u['reasignar_territorio_pct_own'] and v['txns'] > 20:
            recs.append('Reasignar territorio')
        if v['web_pct'] > u['referente_web_pct']:
            recs.append('Referente canal WEB')

        v['patterns'] = patterns
        v['risk_factors'] = risks
        v['recommendations'] = recs

    print(f"  ✓ Patterns y recomendaciones asignados")
    return vendedores


# ============================================================================
# ANÁLISIS DE SUCURSALES
# ============================================================================

def calcular_sucursales(df: pd.DataFrame, vendedores: List[Dict]) -> List[Dict]:
    """Métricas agregadas por sucursal."""
    print("🏢 Calculando métricas por sucursal...")

    # Incluir NC para neto real (NC ya viene negativo)
    df_todos = df[df['es_factura'] | df['es_nd'] | df['es_nc']]
    df_solo_fac = df[df['es_factura'] | df['es_nd']]
    sucursales_data = []

    for suc_nombre, gv in df_todos.groupby('sucursal'):
        neto = gv['Neto'].sum()
        costo = gv['Costo'].sum()  # NC costo ya es negativo
        margen_pct = ((neto - costo) / neto * 100) if neto > 0 else 0
        margen_abs = neto - costo
        markup = (neto / costo) if costo > 0 else 0
        txns = gv[gv['es_factura'] | gv['es_nd']]['Número'].nunique()
        clientes = gv['Cliente'].nunique()
        vendedores_count = gv['Vendedor'].nunique()
        kgs = gv['Kgs'].sum()

        # Fletes (solo de facturas)
        gv_fac = gv[gv['es_factura'] | gv['es_nd']]
        flete = gv_fac[gv_fac['es_flete']]
        flete_neto = flete['Neto'].sum()
        flete_count = len(flete)
        flete_per_kg = (flete_neto / kgs) if kgs > 0 else 0

        # Web (solo facturas)
        web = gv_fac[gv_fac['es_web']]
        web_neto = web['Neto'].sum()
        web_txns = web['Número'].nunique()

        # Rubros top
        rubros_top = dict(
            gv.groupby('Nombre rubro')['Neto'].sum()
            .sort_values(ascending=False).head(8)
        )

        # Top vendedores de la sucursal
        vs_suc = [v for v in vendedores if v['sucursal'] == suc_nombre]
        vs_suc.sort(key=lambda x: x['neto'], reverse=True)
        top_vendedores = [
            {'id': v['id'], 'nombre': v['nombre'], 'neto': v['neto']}
            for v in vs_suc[:5]
        ]

        sucursales_data.append({
            'nombre': suc_nombre,
            'cod_suc': suc_nombre,
            'neto': float(neto),
            'costo': float(costo),
            'margen_pct': round(float(margen_pct), 1),
            'margen_abs': float(margen_abs),
            'markup': round(float(markup), 2),
            'txns': int(txns),
            'clientes': int(clientes),
            'vendedores': int(vendedores_count),
            'kgs': round(float(kgs), 1),
            'flete_neto': float(flete_neto),
            'flete_count': int(flete_count),
            'flete_per_kg': round(float(flete_per_kg), 1),
            'web_neto': float(web_neto),
            'web_txns': int(web_txns),
            'rubros_top': {k: float(v) for k, v in rubros_top.items()},
            'top_vendedores': top_vendedores,
        })

    sucursales_data.sort(key=lambda x: x['neto'], reverse=True)
    print(f"  ✓ {len(sucursales_data)} sucursales procesadas")
    return sucursales_data


# ============================================================================
# ANÁLISIS DE CLIENTES
# ============================================================================

def calcular_clientes(df: pd.DataFrame) -> Dict:
    """Métricas de clientes para dashboard de clientes."""
    print("👥 Calculando métricas de clientes...")

    # Incluir NC para neto real
    df_todos = df[df['es_factura'] | df['es_nd'] | df['es_nc']]

    cli = df_todos.groupby(['Cliente', 'Nombre Cliente']).agg(
        neto=('Neto', 'sum'),
        costo=('Costo', 'sum'),
        txns=('Número', 'nunique'),
        rubros=('Nombre rubro', 'nunique'),
        kgs=('Kgs', 'sum'),
        vendedores=('Vendedor', 'nunique'),
        dias_compra=('Fec. Comp.', lambda x: x.dt.date.nunique()),
        primera_compra=('Fec. Comp.', 'min'),
        ultima_compra=('Fec. Comp.', 'max'),
        grupo=('Nombre Grupo', 'first'),
        zona=('Zona Reparto', 'first'),
        sucursal=('sucursal', 'first'),
        vendedor_asignado=('Vendedor Cliente', 'first'),
        nombre_vendedor_asignado=('Nombre Vendedor Cliente', 'first'),
    ).reset_index()

    cli['margen_pct'] = ((cli['neto'] - cli['costo']) / cli['neto'] * 100).fillna(0)
    cli['ticket_prom'] = (cli['neto'] / cli['txns']).fillna(0)

    # Pareto
    cli_sorted = cli.sort_values('neto', ascending=False)
    cli_sorted['cum_pct'] = cli_sorted['neto'].cumsum() / cli_sorted['neto'].sum() * 100
    n80 = int((cli_sorted['cum_pct'] <= 80).sum())
    total_cli = len(cli)

    # Segmentación ABC
    cli_sorted['segmento'] = 'C'
    cli_sorted.loc[cli_sorted['cum_pct'] <= 80, 'segmento'] = 'A'
    cli_sorted.loc[
        (cli_sorted['cum_pct'] > 80) & (cli_sorted['cum_pct'] <= 95), 'segmento'
    ] = 'B'

    # KPIs
    total_neto = float(cli['neto'].sum())
    total_txns = int(cli['txns'].sum())
    avg_ticket = float(cli['ticket_prom'].mean())
    avg_margen = float(cli['margen_pct'].mean())
    mono_rubro = int((cli['rubros'] == 1).sum())
    multi_vendedor = int((cli['vendedores'] > 1).sum())

    # Top 20 clientes
    top20 = []
    for _, row in cli_sorted.head(20).iterrows():
        top20.append({
            'cliente': str(row['Cliente']),
            'nombre': row['Nombre Cliente'],
            'neto': float(row['neto']),
            'margen_pct': round(float(row['margen_pct']), 1),
            'txns': int(row['txns']),
            'rubros': int(row['rubros']),
            'kgs': round(float(row['kgs']), 1),
            'segmento': row['segmento'],
            'grupo': str(row['grupo']),
            'zona': str(row.get('zona', '')),
            'vendedor_asignado': str(row['nombre_vendedor_asignado']),
        })

    # Distribución por segmento crediticio
    por_grupo = dict(
        cli.groupby('grupo')['neto'].sum().sort_values(ascending=False)
    )

    # Distribución por zona
    por_zona = dict(
        cli.groupby('zona')['neto'].sum().sort_values(ascending=False).head(15)
    )

    # Distribución por sucursal
    por_sucursal = dict(
        cli.groupby('sucursal')['neto'].sum().sort_values(ascending=False)
    )

    # Segmentación ABC resumen
    abc_resumen = {}
    for seg in ['A', 'B', 'C']:
        s = cli_sorted[cli_sorted['segmento'] == seg]
        abc_resumen[seg] = {
            'count': int(len(s)),
            'neto': float(s['neto'].sum()),
            'pct_neto': round(float(s['neto'].sum() / total_neto * 100), 1) if total_neto > 0 else 0,
        }

    result = {
        'total_clientes': int(total_cli),
        'total_neto': total_neto,
        'total_txns': total_txns,
        'pareto_n80': n80,
        'pareto_pct': round(n80 / total_cli * 100, 1) if total_cli > 0 else 0,
        'avg_ticket': round(avg_ticket, 0),
        'avg_margen': round(avg_margen, 1),
        'mono_rubro': mono_rubro,
        'multi_vendedor': multi_vendedor,
        'abc_resumen': abc_resumen,
        'top20': top20,
        'por_grupo': {k: float(v) for k, v in por_grupo.items()},
        'por_zona': {k: float(v) for k, v in por_zona.items()},
        'por_sucursal': {k: float(v) for k, v in por_sucursal.items()},
    }

    print(f"  ✓ {total_cli} clientes. Pareto: {n80} ({result['pareto_pct']}%) generan 80%")
    return result


# ============================================================================
# ANÁLISIS DE ARTÍCULOS
# ============================================================================

def calcular_articulos(df: pd.DataFrame) -> Dict:
    """Métricas de artículos para dashboard de artículos/mix."""
    print("📦 Calculando métricas de artículos...")

    # Incluir NC para neto real
    df_todos = df[df['es_factura'] | df['es_nd'] | df['es_nc']]
    df_todos_no_flete = df_todos[~df_todos['es_flete']]

    # Por rubro
    rubros = df_todos_no_flete.groupby('Nombre rubro').agg(
        neto=('Neto', 'sum'), costo=('Costo', 'sum'),
        kgs=('Kgs', 'sum'), txns=('Número', 'nunique'),
        clientes=('Cliente', 'nunique'), cantidad=('Cantidad Vendida', 'sum'),
    ).reset_index()
    rubros['margen_pct'] = ((rubros['neto'] - rubros['costo']) / rubros['neto'] * 100).fillna(0)
    rubros = rubros.sort_values('neto', ascending=False)

    top_rubros = []
    for _, row in rubros.head(20).iterrows():
        top_rubros.append({
            'nombre': row['Nombre rubro'],
            'neto': float(row['neto']),
            'margen_pct': round(float(row['margen_pct']), 1),
            'kgs': round(float(row['kgs']), 1),
            'txns': int(row['txns']),
            'clientes': int(row['clientes']),
        })

    # Por familia
    familias = df_todos_no_flete.groupby('Nombre familia').agg(
        neto=('Neto', 'sum'), costo=('Costo', 'sum'), kgs=('Kgs', 'sum'),
    ).reset_index()
    familias['margen_pct'] = ((familias['neto'] - familias['costo']) / familias['neto'] * 100).fillna(0)
    familias = familias.sort_values('neto', ascending=False)

    por_familia = []
    for _, row in familias.iterrows():
        por_familia.append({
            'nombre': row['Nombre familia'].strip(),
            'neto': float(row['neto']),
            'margen_pct': round(float(row['margen_pct']), 1),
            'kgs': round(float(row['kgs']), 1),
        })

    # Por grupo artículo
    grupos = df_todos_no_flete.groupby('Grupo Articulo').agg(
        neto=('Neto', 'sum'), costo=('Costo', 'sum'),
    ).reset_index()
    grupos['margen_pct'] = ((grupos['neto'] - grupos['costo']) / grupos['neto'] * 100).fillna(0)
    grupos = grupos.sort_values('neto', ascending=False)

    por_grupo = []
    for _, row in grupos.head(15).iterrows():
        por_grupo.append({
            'nombre': row['Grupo Articulo'],
            'neto': float(row['neto']),
            'margen_pct': round(float(row['margen_pct']), 1),
        })

    # Por marca
    marcas = df_todos_no_flete.groupby('Nombre marca').agg(
        neto=('Neto', 'sum'), costo=('Costo', 'sum'),
    ).reset_index()
    marcas['margen_pct'] = ((marcas['neto'] - marcas['costo']) / marcas['neto'] * 100).fillna(0)
    marcas = marcas.sort_values('neto', ascending=False)

    top_marcas = []
    for _, row in marcas.head(15).iterrows():
        top_marcas.append({
            'nombre': row['Nombre marca'],
            'neto': float(row['neto']),
            'margen_pct': round(float(row['margen_pct']), 1),
        })

    total_neto = float(df_todos_no_flete['Neto'].sum())
    total_costo = float(df_todos_no_flete['Costo'].sum())
    margen_global = ((total_neto - total_costo) / total_neto * 100) if total_neto > 0 else 0

    result = {
        'total_articulos': int(df_todos_no_flete['Artículo'].nunique()),
        'total_rubros': int(df_todos_no_flete['Nombre rubro'].nunique()),
        'total_familias': int(df_todos_no_flete['Nombre familia'].nunique()),
        'total_marcas': int(df_todos_no_flete['Nombre marca'].nunique()),
        'total_neto': total_neto,
        'margen_global': round(margen_global, 1),
        'total_kgs': round(float(df_todos_no_flete['Kgs'].sum()), 1),
        'top_rubros': top_rubros,
        'por_familia': por_familia,
        'por_grupo': por_grupo,
        'top_marcas': top_marcas,
    }

    print(f"  ✓ {result['total_articulos']} artículos, {result['total_rubros']} rubros, {result['total_familias']} familias")
    return result


# ============================================================================
# SUBIDA A FIREBASE
# ============================================================================

def subir_a_firebase(periodo: str, vendedores: List[Dict], sucursales: List[Dict],
                     clientes: Dict, articulos: Dict, df: pd.DataFrame):
    """Sube todos los resultados a Firestore."""
    print("🔥 Subiendo a Firestore...")

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        print("  ✗ firebase-admin no instalado. pip install firebase-admin")
        print("  → Guardando JSON local como fallback...")
        guardar_json_local(periodo, vendedores, sucursales, clientes, articulos, df)
        return

    if not os.path.exists(FIREBASE_CREDENTIALS_PATH):
        print(f"  ✗ {FIREBASE_CREDENTIALS_PATH} no encontrado")
        print("  → Guardando JSON local como fallback...")
        guardar_json_local(periodo, vendedores, sucursales, clientes, articulos, df)
        return

    # Inicializar Firebase
    if not firebase_admin._apps:
        cred = credentials.Certificate(FIREBASE_CREDENTIALS_PATH)
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    timestamp = datetime.now(timezone.utc).isoformat()

    df_todos = df[df['es_factura'] | df['es_nd'] | df['es_nc']]
    df_solo_fac = df[df['es_factura'] | df['es_nd']]
    dias = df_todos['Fec. Comp.'].dt.date.nunique()
    total_neto = float(df_todos['Neto'].sum())
    total_costo = float(df_todos['Costo'].sum())

    # 1. Documento principal: indicadores/comercial/ventas_analytics/{periodo}
    doc_principal = {
        'periodo': periodo,
        'timestamp': timestamp,
        'fecha_min': df['Fec. Comp.'].min().isoformat(),
        'fecha_max': df['Fec. Comp.'].max().isoformat(),
        'dias': dias,
        'total_neto': total_neto,
        'total_costo': total_costo,
        'total_margen_pct': round((total_neto - total_costo) / total_neto * 100, 1) if total_neto > 0 else 0,
        'total_markup': round(total_neto / total_costo, 2) if total_costo > 0 else 0,
        'total_clientes': int(df_todos['Cliente'].nunique()),
        'total_vendedores': int(df_todos['Vendedor'].nunique()),
        'total_txns': int(df_solo_fac['Número'].nunique()),
        'total_kgs': round(float(df_todos['Kgs'].sum()), 1),
        'sucursales': sorted(df['sucursal'].unique().tolist()),
        'score_weights': SCORE_WEIGHTS,
        'score_method': SCORE_METHOD,
        'sucursales_data': sucursales,
    }

    # Referencia base: indicadores/comercial
    area_ref = db.collection(ROOT_COL).document(AREA_DOC)

    doc_ref = area_ref.collection(SUBCOLLECTIONS['ventas']).document(periodo)
    doc_ref.set(doc_principal)
    print(f"  ✓ indicadores/comercial/ventas_analytics/{periodo} → doc principal")

    # 2. Subcolección: vendedores
    batch_size = 0
    batch = db.batch()
    for v in vendedores:
        v_ref = doc_ref.collection(SUBCOLLECTIONS['vendedores']).document(v['id'])
        v['updated_at'] = timestamp
        batch.set(v_ref, v)
        batch_size += 1
        if batch_size >= 450:
            batch.commit()
            batch = db.batch()
            batch_size = 0
    if batch_size > 0:
        batch.commit()
    print(f"  ✓ indicadores/comercial/ventas_analytics/{periodo}/vendedores → {len(vendedores)} docs")

    # 3. Clientes analytics
    clientes['periodo'] = periodo
    clientes['timestamp'] = timestamp
    area_ref.collection(SUBCOLLECTIONS['clientes']).document(periodo).set(clientes)
    print(f"  ✓ indicadores/comercial/clientes_analytics/{periodo}")

    # 4. Artículos analytics
    articulos['periodo'] = periodo
    articulos['timestamp'] = timestamp
    area_ref.collection(SUBCOLLECTIONS['articulos']).document(periodo).set(articulos)
    print(f"  ✓ indicadores/comercial/articulos_analytics/{periodo}")

    # 5. Config de pesos (solo si no existe)
    config_ref = area_ref.collection(SUBCOLLECTIONS['config']).document('adn-weights')
    if not config_ref.get().exists:
        config_ref.set(SCORE_WEIGHTS)
        print(f"  ✓ indicadores/comercial/config/adn-weights creado")
    else:
        print(f"  → indicadores/comercial/config/adn-weights ya existe (no sobreescrito)")

    print(f"\n✅ Subida completa a Firestore")


def guardar_json_local(periodo: str, vendedores: List[Dict], sucursales: List[Dict],
                       clientes: Dict, articulos: Dict, df: pd.DataFrame):
    """Guarda resultados como JSON local (fallback sin Firebase)."""
    output_dir = f"output_{periodo}"
    os.makedirs(output_dir, exist_ok=True)

    df_todos = df[df['es_factura'] | df['es_nd'] | df['es_nc']]
    df_solo_fac = df[df['es_factura'] | df['es_nd']]
    dias = df_todos['Fec. Comp.'].dt.date.nunique()
    total_neto = float(df_todos['Neto'].sum())
    total_costo = float(df_todos['Costo'].sum())

    doc_principal = {
        'periodo': periodo,
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'dias': dias,
        'total_neto': total_neto,
        'total_costo': total_costo,
        'total_margen_pct': round((total_neto - total_costo) / total_neto * 100, 1) if total_neto > 0 else 0,
        'total_markup': round(total_neto / total_costo, 2) if total_costo > 0 else 0,
        'total_clientes': int(df_todos['Cliente'].nunique()),
        'total_vendedores': int(df_todos['Vendedor'].nunique()),
        'total_txns': int(df_solo_fac['Número'].nunique()),
        'total_kgs': round(float(df_todos['Kgs'].sum()), 1),
        'score_weights': SCORE_WEIGHTS,
        'score_method': SCORE_METHOD,
        'sucursales': sorted(df['sucursal'].unique().tolist()),
        'sucursales_data': sucursales,
        'vendedores': vendedores,
    }

    with open(f"{output_dir}/ventas_analytics.json", 'w', encoding='utf-8') as f:
        json.dump(doc_principal, f, ensure_ascii=False, indent=2, default=str)

    with open(f"{output_dir}/clientes_analytics.json", 'w', encoding='utf-8') as f:
        json.dump(clientes, f, ensure_ascii=False, indent=2, default=str)

    with open(f"{output_dir}/articulos_analytics.json", 'w', encoding='utf-8') as f:
        json.dump(articulos, f, ensure_ascii=False, indent=2, default=str)

    print(f"  ✓ JSONs guardados en {output_dir}/")


# ============================================================================
# RESUMEN EN CONSOLA
# ============================================================================

def imprimir_resumen(vendedores: List[Dict], sucursales: List[Dict],
                     clientes: Dict, articulos: Dict, df: pd.DataFrame):
    """Imprime un resumen de los resultados para verificación."""
    df_todos = df[df['es_factura'] | df['es_nd'] | df['es_nc']]

    print("\n" + "=" * 60)
    print("RESUMEN DEL ANÁLISIS")
    print("=" * 60)

    total_neto = df_todos['Neto'].sum()
    total_costo = df_todos['Costo'].sum()
    margen = (total_neto - total_costo) / total_neto * 100 if total_neto > 0 else 0

    print(f"\n📊 GLOBALES:")
    print(f"  Facturación: ${total_neto:,.0f}")
    print(f"  Margen: {margen:.1f}% | Markup: {total_neto/total_costo:.2f}x (${total_neto - total_costo:,.0f})")
    print(f"  Transacciones: {df_todos['Número'].nunique():,}")
    print(f"  Clientes: {df_todos['Cliente'].nunique():,}")
    print(f"  Vendedores: {df_todos['Vendedor'].nunique()}")
    print(f"  Kgs: {df_todos['Kgs'].sum():,.0f}")
    print(f"  Días: {df_todos['Fec. Comp.'].dt.date.nunique()}")

    elegibles = [v for v in vendedores if v['txns'] >= MIN_TXNS_SCORE]
    if elegibles:
        top5 = sorted(elegibles, key=lambda v: v['adn_score'], reverse=True)[:5]
        print(f"\n🏆 TOP 5 VENDEDORES (ADN):")
        for i, v in enumerate(top5, 1):
            print(f"  {i}. [{v['adn_score']}] {v['nombre']} ({v['badge_label']}) "
                  f"— ${v['neto']:,.0f} | {v['margen_pct']:.1f}% | mk {v['markup']:.2f}x | {v['clientes']} cli | "
                  f"{v['kgs']:,.0f} kg | {v['flete_per_kg']:.1f} $/kg flete")

    print(f"\n🏢 SUCURSALES:")
    for s in sucursales[:5]:
        print(f"  {s['nombre']}: ${s['neto']:,.0f} | {s['margen_pct']:.1f}% | mk {s['markup']:.2f}x | "
              f"{s['kgs']:,.0f} kg | {s['flete_per_kg']:.1f} $/kg flete | {s['vendedores']} vend")

    print(f"\n👥 CLIENTES:")
    print(f"  Total: {clientes['total_clientes']} | "
          f"Pareto 80%: {clientes['pareto_n80']} ({clientes['pareto_pct']}%)")
    abc = clientes['abc_resumen']
    for seg in ['A', 'B', 'C']:
        s = abc[seg]
        print(f"  Segmento {seg}: {s['count']} clientes → {s['pct_neto']}% facturación")

    print(f"\n📦 ARTÍCULOS:")
    print(f"  {articulos['total_articulos']} artículos | {articulos['total_rubros']} rubros | "
          f"Margen global: {articulos['margen_global']}%")
    for r in articulos['top_rubros'][:5]:
        print(f"  {r['nombre']}: ${r['neto']:,.0f} ({r['margen_pct']:.1f}%)")


# ============================================================================
# MAIN
# ============================================================================

def main():
    if len(sys.argv) < 3:
        print("Uso: python analizar_ventas.py <archivo.xlsx> <periodo>")
        print("Ejemplo: python analizar_ventas.py Ventas_2026-04.xlsx 2026-04")
        sys.exit(1)

    archivo = sys.argv[1]
    periodo = sys.argv[2]

    if not os.path.exists(archivo):
        print(f"✗ Archivo no encontrado: {archivo}")
        sys.exit(1)

    print("=" * 60)
    print(f"ANÁLISIS DE VENTAS — {periodo}")
    print("=" * 60)

    # 1. Cargar y limpiar
    df = cargar_xlsx(archivo)
    df = limpiar_datos(df)

    # 2. Calcular métricas de vendedores
    vendedores = calcular_metricas_vendedor(df)

    # 3. Score ADN
    vendedores = calcular_score_adn(vendedores)

    # 4. Patterns, risks, recommendations
    vendedores = asignar_patterns_y_riesgos(vendedores)

    # 5. Sucursales
    sucursales = calcular_sucursales(df, vendedores)

    # 6. Clientes
    clientes = calcular_clientes(df)

    # 7. Artículos
    articulos = calcular_articulos(df)

    # 8. Resumen
    imprimir_resumen(vendedores, sucursales, clientes, articulos, df)

    # 9. Subir a Firebase (o JSON local)
    subir_a_firebase(periodo, vendedores, sucursales, clientes, articulos, df)

    print("\n🎉 Análisis completado exitosamente")


if __name__ == '__main__':
    main()
