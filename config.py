"""
Configuración del pipeline de análisis de ventas
Carlos Isla y Cía - NET-LogistK ISLA
"""

# ============================================================================
# MAPEO DE SUCURSALES POR PUNTO DE VENTA (PV)
# ============================================================================
# El PV se extrae del campo "Número" (ej: "A00045-00140473" → PV=45)
# Este mapeo coincide con el reporte de sucursales de Nexion

PV_SUCURSAL = {
    44: 'NQN Suc.centro',      # CASA CENTRAL → va con NQN Centro en reportes
    45: 'SAN JUAN',            # DPM
    46: 'RUTA 151',            # CIPOLLETTI Ruta 151
    47: 'NEUQUEN',             # NQN principal
    48: 'ESMERALDA',           # CIPO ESMERALDA
    49: 'PLOTTIER',            # PLOTTIER
    56: 'VTAS WEB',            # REDES (ventas online)
    58: 'VILLA REGINA',        # VILLA REGINA
    59: 'NEUQUEN',             # NEUQUEN (se agrupa con PV 47)
    60: 'NQN Suc.centro',      # NQN CENTRO
    61: 'CUTRAL-CO',           # CCO
    63: 'CENTENARIO',          # CENTENARIO
    114: 'VTAS WEB',           # ROCA-WEB (se agrupa con PV 56)
}

# PVs a excluir (movimientos internos: ajustes, cheques rechazados, impuestos)
PV_EXCLUIR = [0]

# Fallback: si un PV no está mapeado, usar Cod SUC
SUCURSALES_CODSUC = {
    'DPM': 'SAN JUAN',
    'NQN': 'NEUQUEN',
    'CIPO2': 'RUTA 151',
    'CCO': 'CUTRAL-CO',
    'CENTRAL': 'VTAS WEB',
    'CIPO': 'ESMERALDA',
    'REGINA': 'VILLA REGINA',
    'CENTENARIO': 'CENTENARIO',
    'PLT': 'PLOTTIER',
}

# Sufijo del nombre del vendedor → sucursal
SUFIJO_SUCURSAL = {
    'SJ': 'DPM',
    'NQN': 'NQN',
    'CCO': 'CCO',
    'ESM': 'CIPO',
    'VR': 'REGINA',
    '151': 'PLT',
    'WEB': 'CENTRAL',
}

# ============================================================================
# TIPOS DE COMPROBANTE
# ============================================================================

FACTURAS = ['Factura de Venta']
NOTAS_CREDITO = ['Nota de Crédito Venta', 'Nota de Crédito Venta x Ajuste']
NOTAS_DEBITO = ['Nota de Débito Venta']
CANAL_WEB = 'Venta WEB'

# ============================================================================
# FAMILIAS DE FLETE
# ============================================================================

FAMILIA_FLETE = 'FLETES'

# ============================================================================
# SCORE ADN — PESOS Y UMBRALES
# ============================================================================

SCORE_WEIGHTS = {
    'facturacion': 0.25,
    'rentabilidad': 0.25,
    'cartera': 0.15,
    'diversidad': 0.13,
    'actividad': 0.12,
    'calidad': 0.10,
}

SCORE_METHOD = 'percentiles'

# Mínimo de transacciones para ser elegible al score
MIN_TXNS_SCORE = 10

# ============================================================================
# BADGES DE ESTADO
# ============================================================================

BADGES = {
    'estrella': {'min_score': 65, 'label': 'Estrella', 'color': 'green'},
    'estable': {'min_score': 45, 'label': 'Estable', 'color': 'blue'},
    'atencion': {'min_score': 25, 'label': 'Atención', 'color': 'yellow'},
    'riesgo': {'min_score': 0, 'label': 'En riesgo', 'color': 'red'},
    'inactivo': {'min_score': -1, 'label': 'Inactivo', 'color': 'gray'},
}

# ============================================================================
# UMBRALES PARA RISK FACTORS Y RECOMMENDATIONS
# ============================================================================

UMBRALES = {
    # Risk factors
    'margen_bajo_pct': 20,
    'margen_bajo_neto_min': 100_000_000,
    'concentracion_clientes_max': 5,
    'concentracion_neto_min': 50_000_000,
    'hhi_alto': 3000,
    'nc_frecuentes_min': 5,
    'flete_bajo_per_kg': 5,             # $/kg — por debajo es "no cobra flete"
    'flete_bajo_kgs_min': 50_000,       # Kgs mínimos para considerar (volumen significativo)
    # Recommendations
    'capacitar_flete_per_kg': 8,        # $/kg — por debajo sugerir capacitar
    'flete_alto_per_kg': 20,            # $/kg — por encima es "cobra bien el flete"
    'ampliar_mix_rubros': 4,
    'diversificar_clientes': 5,
    'revisar_descuentos_margen': 20,
    'reasignar_territorio_pct_own': 50,
    'referente_web_pct': 30,
}

# ============================================================================
# FIREBASE
# ============================================================================

FIREBASE_CREDENTIALS_PATH = 'serviceAccountKey.json'

# Estructura Firestore real — indicadores/{area}/{subcoleccion}/{periodo}
# indicadores/comercial/ventas_analytics/{periodo}
# indicadores/comercial/ventas_analytics/{periodo}/vendedores/{id}
# indicadores/comercial/clientes_analytics/{periodo}
# indicadores/comercial/articulos_analytics/{periodo}
# indicadores/comercial/config/adn-weights
ROOT_COL = 'indicadores'
AREA_DOC = 'comercial'

SUBCOLLECTIONS = {
    'ventas': 'ventas_analytics',       # /ventas_analytics/{periodo}
    'vendedores': 'vendedores',          # subcolección /ventas_analytics/{periodo}/vendedores/{id}
    'clientes': 'clientes_analytics',    # /clientes_analytics/{periodo}
    'articulos': 'articulos_analytics',  # /articulos_analytics/{periodo}
    'config': 'config',                  # /config/adn-weights
}
