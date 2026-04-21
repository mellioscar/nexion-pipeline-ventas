"""
proyectado_ventas.py
Proyección de ventas del mes en curso — Carlos Isla y Cía.
NET-LogistK ISLA

Fórmula:
    Proyectado = (Venta Acumulada ÷ Días Hábiles Transcurridos)
                 × Total Días Hábiles del Mes

Pesos por tipo de día:
    Lunes–Viernes sin feriado            → 1.0
    Sábado sin feriado                   → 0.5
    Domingo                              → 0.0
    Feriado con trabajado: false         → 0.0
    Feriado con trabajado: true          → peso natural del día (1.0 ó 0.5)

Firestore (lectura):
    feriados/{YYYY}/dias/{MM-DD}
        nombre:    "Nombre del feriado"
        trabajado: true | false

Firestore (escritura):
    indicadores/comercial/ventas_proyectado/{YYYY-MM}
    (mismo árbol que ventas_analytics para consistencia con NET-LogistK ISLA)
"""

import calendar
from datetime import datetime, timezone, date
from typing import Dict, List, Set
import pandas as pd

from config import ROOT_COL, AREA_DOC


# ─────────────────────────────────────────────────────────────────────────────
# FERIADOS
# ─────────────────────────────────────────────────────────────────────────────

def cargar_feriados(db, anio: int) -> Dict[date, bool]:
    """
    Lee los feriados del año desde Firestore.
    Retorna {fecha: trabajado}
        trabajado=False → peso 0.0 (feriado no trabajado)
        trabajado=True  → peso natural del día (feriado trabajado)

    Si la colección no existe, retorna {} sin romper el pipeline.
    """
    feriados: Dict[date, bool] = {}
    try:
        docs = (
            db.collection("feriados")
            .document(str(anio))
            .collection("dias")
            .stream()
        )
        for doc in docs:
            try:
                fecha     = date.fromisoformat(f"{anio}-{doc.id}")
                trabajado = bool((doc.to_dict() or {}).get("trabajado", False))
                feriados[fecha] = trabajado
            except (ValueError, AttributeError):
                pass

        if feriados:
            no_trab   = sum(1 for t in feriados.values() if not t)
            trabajados = sum(1 for t in feriados.values() if t)
            print(f"  ✓ Feriados {anio}: {no_trab} no trabajados, "
                  f"{trabajados} trabajados (cuentan con peso normal)")
        else:
            print(f"  ⚠ Sin feriados en feriados/{anio}/dias/ — "
                  f"proyectado calculado sin descuentos de feriados.")
    except Exception as e:
        print(f"  ⚠ No se pudo leer feriados: {e}")
    return feriados


# ─────────────────────────────────────────────────────────────────────────────
# PESO DE CADA DÍA
# ─────────────────────────────────────────────────────────────────────────────

def peso_dia(d: date, feriados: Dict[date, bool]) -> float:
    """
    Domingo siempre                      → 0.0
    Feriado no trabajado (False)         → 0.0
    Feriado trabajado (True)             → peso natural (1.0 ó 0.5)
    Sábado normal                        → 0.5
    Lunes–Viernes normal                 → 1.0
    """
    dow = d.weekday()   # 0=Lun … 6=Dom
    if dow == 6:
        return 0.0

    peso_natural = 0.5 if dow == 5 else 1.0

    if d in feriados:
        return peso_natural if feriados[d] else 0.0

    return peso_natural


# ─────────────────────────────────────────────────────────────────────────────
# CÁLCULO DE DÍAS HÁBILES
# ─────────────────────────────────────────────────────────────────────────────

def dias_habiles_mes_completo(anio: int, mes: int,
                               feriados: Dict[date, bool]) -> float:
    _, ultimo = calendar.monthrange(anio, mes)
    return sum(peso_dia(date(anio, mes, d), feriados) for d in range(1, ultimo + 1))


def dias_habiles_transcurridos(anio: int, mes: int,
                                hasta_dia: int,
                                feriados: Dict[date, bool]) -> float:
    _, ultimo = calendar.monthrange(anio, mes)
    hasta_dia = min(hasta_dia, ultimo)
    return sum(peso_dia(date(anio, mes, d), feriados) for d in range(1, hasta_dia + 1))


# ─────────────────────────────────────────────────────────────────────────────
# FÓRMULA
# ─────────────────────────────────────────────────────────────────────────────

def proyectar(acumulado: float,
              hab_transcurridos: float,
              hab_totales: float) -> float:
    if hab_transcurridos <= 0:
        return 0.0
    return (acumulado / hab_transcurridos) * hab_totales


# ─────────────────────────────────────────────────────────────────────────────
# CÁLCULO COMPLETO
# ─────────────────────────────────────────────────────────────────────────────

def calcular_proyectado(df: pd.DataFrame,
                         vendedores: List[Dict],
                         periodo: str,
                         db) -> Dict:
    """
    Calcula la proyección del mes en curso.

    Args:
        df:         DataFrame ya procesado por limpiar_datos()
        vendedores: Lista de dicts de calcular_metricas_vendedor()
        periodo:    'YYYY-MM'
        db:         Cliente Firestore (inicializado por subir_a_firebase)
    """
    print("\n📈 Calculando proyectado de ventas...")

    anio, mes = map(int, periodo.split("-"))

    # Día hasta el que hay datos (el export puede llegar con 1 día de atraso)
    fecha_max_datos = df["Fec. Comp."].max()
    if pd.notna(fecha_max_datos):
        fecha_max_datos = fecha_max_datos.date()
        dia_hasta = fecha_max_datos.day \
            if (fecha_max_datos.year == anio and fecha_max_datos.month == mes) \
            else calendar.monthrange(anio, mes)[1]
    else:
        dia_hasta = datetime.now().day
        fecha_max_datos = date(anio, mes, dia_hasta)

    # Feriados desde Firestore
    feriados = cargar_feriados(db, anio)

    # Días hábiles
    hab_totales       = dias_habiles_mes_completo(anio, mes, feriados)
    hab_transcurridos = dias_habiles_transcurridos(anio, mes, dia_hasta, feriados)
    _, total_dias_mes = calendar.monthrange(anio, mes)

    print(f"  Datos hasta      : {fecha_max_datos} (día {dia_hasta}/{total_dias_mes})")
    print(f"  Días háb. transc.: {hab_transcurridos:.2f} / {hab_totales:.2f} totales")

    # ── Acumulado real ────────────────────────────────────────────────────────
    df_v             = df[df["es_factura"] | df["es_nd"] | df["es_nc"]]
    neto_acum        = float(df_v["Neto"].sum())
    costo_acum       = float(df_v["Costo"].sum())
    kgs_acum         = float(df_v["Kgs"].sum())
    txns_acum        = int(df[df["es_factura"] | df["es_nd"]]["Número"].nunique())
    clientes_activos = int(df_v["Cliente"].nunique())
    margen_acum      = neto_acum - costo_acum
    margen_pct_acum  = round(margen_acum / neto_acum * 100, 1) if neto_acum else 0.0
    velocidad_diaria = neto_acum / hab_transcurridos if hab_transcurridos > 0 else 0.0

    # ── Proyectado global ─────────────────────────────────────────────────────
    neto_proy   = proyectar(neto_acum,  hab_transcurridos, hab_totales)
    costo_proy  = proyectar(costo_acum, hab_transcurridos, hab_totales)
    kgs_proy    = proyectar(kgs_acum,   hab_transcurridos, hab_totales)
    txns_proy   = int(proyectar(txns_acum, hab_transcurridos, hab_totales))
    margen_proy = neto_proy - costo_proy
    margen_pct_proy = round(margen_proy / neto_proy * 100, 1) if neto_proy else 0.0

    print(f"  Neto acumulado   : ${neto_acum/1e6:.2f}M")
    print(f"  Velocidad/día    : ${velocidad_diaria/1e3:.1f}K")
    print(f"  Neto proyectado  : ${neto_proy/1e6:.2f}M")

    # ── Por sucursal ──────────────────────────────────────────────────────────
    por_sucursal = {}
    for suc, grp in df_v.groupby("sucursal"):
        n = float(grp["Neto"].sum())
        c = float(grp["Costo"].sum())
        k = float(grp["Kgs"].sum())
        por_sucursal[str(suc)] = {
            "neto_acumulado":    round(n, 2),
            "neto_proyectado":   round(proyectar(n, hab_transcurridos, hab_totales), 2),
            "kgs_acumulado":     round(k, 1),
            "kgs_proyectado":    round(proyectar(k, hab_transcurridos, hab_totales), 1),
            "margen_acumulado":  round(n - c, 2),
            "margen_proyectado": round(
                proyectar(n, hab_transcurridos, hab_totales)
                - proyectar(c, hab_transcurridos, hab_totales), 2
            ),
            "participacion_pct": round(n / neto_acum * 100, 1) if neto_acum else 0.0,
            "velocidad_diaria":  round(n / hab_transcurridos, 2) if hab_transcurridos else 0.0,
        }

    # ── Por vendedor ──────────────────────────────────────────────────────────
    proyectado_vendedores = []
    for v in vendedores:
        n = v.get("neto",  0.0)
        c = v.get("costo", 0.0)
        k = v.get("kgs",   0.0)
        n_proy = proyectar(n, hab_transcurridos, hab_totales)
        c_proy = proyectar(c, hab_transcurridos, hab_totales)
        proyectado_vendedores.append({
            "id":                v["id"],
            "nombre":            v.get("nombre", ""),
            "sucursal":          v.get("sucursal", ""),
            "neto_acumulado":    round(n, 2),
            "neto_proyectado":   round(n_proy, 2),
            "kgs_acumulado":     round(k, 1),
            "kgs_proyectado":    round(proyectar(k, hab_transcurridos, hab_totales), 1),
            "margen_acumulado":  round(n - c, 2),
            "margen_proyectado": round(n_proy - c_proy, 2),
            "velocidad_diaria":  round(n / hab_transcurridos, 2) if hab_transcurridos else 0.0,
            "participacion_pct": round(n / neto_acum * 100, 1) if neto_acum else 0.0,
            "score_adn":         v.get("score_adn"),
            "badge":             v.get("badge"),
            "ranking_acumulado": v.get("ranking"),
        })

    proyectado_vendedores.sort(key=lambda x: x["neto_proyectado"], reverse=True)
    for i, pv in enumerate(proyectado_vendedores, 1):
        pv["ranking_proyectado"] = i

    # ── Detalle calendario del mes ────────────────────────────────────────────
    detalle_dias = []
    for d in range(1, total_dias_mes + 1):
        dia_obj = date(anio, mes, d)
        dow     = dia_obj.weekday()
        es_fer  = dia_obj in feriados
        detalle_dias.append({
            "dia":          d,
            "fecha":        dia_obj.isoformat(),
            "dow":          ["Lun","Mar","Mié","Jue","Vie","Sáb","Dom"][dow],
            "peso":         peso_dia(dia_obj, feriados),
            "es_feriado":   es_fer,
            "trabajado":    feriados.get(dia_obj) if es_fer else None,
            "transcurrido": d <= dia_hasta,
        })

    # ── Feriados del mes para el documento ───────────────────────────────────
    feriados_mes = sorted(
        {"fecha": d.isoformat(), "trabajado": t, "peso": peso_dia(d, feriados)}
        for d, t in feriados.items()
        if d.year == anio and d.month == mes
    , key=lambda x: x["fecha"])

    return {
        "periodo":   periodo,
        "timestamp": datetime.now(timezone.utc).isoformat(),

        # Contexto temporal
        "dia_hasta":                   dia_hasta,
        "fecha_hasta":                 fecha_max_datos.isoformat(),
        "total_dias_mes":              total_dias_mes,
        "pct_mes_transcurrido":        round(dia_hasta / total_dias_mes * 100, 1),
        "dias_habiles_transcurridos":  round(hab_transcurridos, 2),
        "dias_habiles_totales_mes":    round(hab_totales, 2),
        "feriados_del_mes":            feriados_mes,

        # Acumulado real
        "neto_acumulado":       round(neto_acum, 2),
        "costo_acumulado":      round(costo_acum, 2),
        "margen_acumulado":     round(margen_acum, 2),
        "margen_pct_acumulado": margen_pct_acum,
        "kgs_acumulado":        round(kgs_acum, 1),
        "txns_acumulado":       txns_acum,
        "clientes_activos":     clientes_activos,
        "velocidad_diaria":     round(velocidad_diaria, 2),

        # Proyectado al cierre
        "neto_proyectado":       round(neto_proy, 2),
        "costo_proyectado":      round(costo_proy, 2),
        "margen_proyectado":     round(margen_proy, 2),
        "margen_pct_proyectado": margen_pct_proy,
        "kgs_proyectado":        round(kgs_proy, 1),
        "txns_proyectado":       txns_proy,

        "por_sucursal": por_sucursal,
        "vendedores":   proyectado_vendedores,
        "detalle_dias": detalle_dias,
    }


# ─────────────────────────────────────────────────────────────────────────────
# SUBIR A FIRESTORE — mismo árbol que ventas_analytics
# ─────────────────────────────────────────────────────────────────────────────

def subir_proyectado(resultado: Dict, db):
    """
    Sube a indicadores/comercial/ventas_proyectado/{periodo}
    Mismo ROOT_COL y AREA_DOC que analizar_ventas.py para consistencia.
    """
    ref = (
        db.collection(ROOT_COL)
        .document(AREA_DOC)
        .collection("ventas_proyectado")
        .document(resultado["periodo"])
    )
    ref.set(resultado)
    print(f"  ✓ {ROOT_COL}/{AREA_DOC}/ventas_proyectado/{resultado['periodo']} → OK")
