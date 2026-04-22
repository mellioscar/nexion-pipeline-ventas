"""
pipeline_ventas_gmail.py
Bridge: Gmail → Excel → analizar_ventas.py → Firebase
Carlos Isla y Cía. — NET-LogistK ISLA

Flujo:
    1. Busca en Gmail el email con asunto configurable (no procesado aún)
    2. Descarga el adjunto .xlsx a un archivo temporal
    3. Renombra columnas del formato diario al esquema de analizar_ventas.py
    4. Llama a cargar_xlsx() → limpiar_datos() → pipeline ADN completo → Firebase
    5. Calcula métricas de flete y las sube a Firebase
    6. Marca el email con label NEXION_VTA_PROCESADO

Columnas del export diario de Nexion → esquema de analizar_ventas.py
Flete identificado por: Nombre familia == 'FLETES'
Neto flete = Neto facturas + Neto NC (las NC ya vienen en negativo)
"""

import os
import base64
import tempfile
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv

import pandas as pd
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from analizar_ventas import (
    cargar_xlsx, limpiar_datos, calcular_metricas_vendedor,
    calcular_score_adn, asignar_patterns_y_riesgos,
    calcular_sucursales, calcular_clientes, calcular_articulos,
    subir_a_firebase, imprimir_resumen,
)
from config import ROOT_COL, AREA_DOC, SUBCOLLECTIONS

load_dotenv()

GMAIL_SCOPES      = ["https://www.googleapis.com/auth/gmail.modify"]
GMAIL_CREDENTIALS = os.getenv("GMAIL_CREDENTIALS_PATH", "credentials.json")
GMAIL_TOKEN       = os.getenv("GMAIL_TOKEN_PATH", "token_ventas.json")
ASUNTO_EMAIL      = os.getenv("ASUNTO_VENTAS", "Estadisticas Ventas diarias")
LABEL_PROCESADO   = "NEXION_VTA_PROCESADO"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────
# MAPEO: columnas export diario Nexion → analizar_ventas.py
# Solo las que cambian de nombre. Ajustar si Nexion los modifica.
# ─────────────────────────────────────────────────────────
MAPEO_COLUMNAS = {
    "FecComp":      "Fec. Comp.",
    "NomSisTCOM":   "Nom Sis TCOM",
    "NroComp":      "Número",
    "CodSUC":       "Cod SUC",
    "CodCLI":       "Cliente",
    "RaSoc":        "Nombre Cliente",
    "CodVDR":       "Vendedor",
    "NomVDR":       "Nombre vendedor",
    "Cod VDR CLI":  "Vendedor Cliente",
    "Nom VDR CLI":  "Nombre Vendedor Cliente",
    "NomRBART":     "Nombre rubro",
    "NomGRART":     "Grupo Articulo",
    "NomFMART":     "Nombre familia",
    "CodART":       "Artículo",       # ← requerido por calcular_articulos()
    "DescART":      "Descripción",
    "CantVendida":  "Cantidad Vendida",
    "ImpPrecio":    "Precio",
    "NomDEP":       "Nom DEP",
    "ZonaReparto":  "Zona Reparto",
    "PorcComision": "Porc Comision",
    "NomMRART":     "Nombre marca",
    "NomGrupARTV":  "Nombre Grupo",
    "ClasComp":     "Clasificador Comprobante",
}

# Las que cargar_xlsx() valida con sys.exit(1) si faltan
COLS_REQUERIDAS = [
    "Fec. Comp.", "Nom Sis TCOM", "Número", "Cod SUC", "Cliente",
    "Nombre Cliente", "Vendedor", "Nombre vendedor", "Vendedor Cliente",
    "Nombre Vendedor Cliente", "Nombre rubro", "Grupo Articulo",
    "Nombre familia", "Cantidad Vendida", "Precio", "Neto", "Costo",
    "Kgs", "Dto/Rec", "Nom DEP", "Zona Reparto", "Porc Comision",
    "Nombre marca", "Nombre Grupo", "Clasificador Comprobante",
]

# Tipos de comprobante (deben coincidir con config.py)
FACTURAS      = ['Factura de Venta']
NOTAS_CREDITO = ['Nota de Crédito Venta', 'Nota de Crédito Venta x Ajuste']
NOTAS_DEBITO  = ['Nota de Débito Venta']


# ─────────────────────────────────────────────────────────
# 1. AUTENTICACIÓN GMAIL
# ─────────────────────────────────────────────────────────
def get_gmail_service():
    creds = None
    if os.path.exists(GMAIL_TOKEN):
        creds = Credentials.from_authorized_user_file(GMAIL_TOKEN, GMAIL_SCOPES)

    token_refrescado = False
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.info("Token expirado — refrescando...")
            creds.refresh(Request())
            token_refrescado = True
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                GMAIL_CREDENTIALS, GMAIL_SCOPES
            )
            creds = flow.run_local_server(port=0)
            token_refrescado = True

    with open(GMAIL_TOKEN, "w") as f:
        f.write(creds.to_json())

    if token_refrescado:
        with open("TOKEN_REFRESHED", "w") as f:
            f.write("1")
        log.info(f"Token actualizado → {GMAIL_TOKEN}")

    return build("gmail", "v1", credentials=creds)


# ─────────────────────────────────────────────────────────
# 2. BUSCAR Y DESCARGAR EMAIL
# ─────────────────────────────────────────────────────────
def buscar_email(service) -> str | None:
    query  = f'subject:"{ASUNTO_EMAIL}" -label:{LABEL_PROCESADO} has:attachment'
    result = service.users().messages().list(
        userId="me", q=query, maxResults=1
    ).execute()
    msgs = result.get("messages", [])
    if not msgs:
        log.info("Sin emails nuevos de ventas.")
        return None
    log.info(f"Email encontrado: {msgs[0]['id']}")
    return msgs[0]["id"]


def descargar_a_tempfile(service, msg_id: str) -> str | None:
    """Descarga el adjunto .xlsx a un archivo temporal. cargar_xlsx() requiere path."""
    mensaje = service.users().messages().get(userId="me", id=msg_id).execute()
    for parte in mensaje.get("payload", {}).get("parts", []):
        nombre = parte.get("filename", "")
        if nombre.lower().endswith((".xlsx", ".xls")):
            att_id = parte["body"].get("attachmentId")
            if not att_id:
                continue
            raw  = service.users().messages().attachments().get(
                userId="me", messageId=msg_id, id=att_id
            ).execute()
            data   = base64.urlsafe_b64decode(raw["data"])
            suffix = ".xlsx" if nombre.lower().endswith(".xlsx") else ".xls"
            tmp    = tempfile.NamedTemporaryFile(
                delete=False, suffix=suffix, prefix="nexion_ventas_"
            )
            tmp.write(data)
            tmp.close()
            log.info(f"Excel descargado: {nombre} ({len(data)/1024:.1f} KB)")
            return tmp.name

    log.warning("Sin adjunto Excel en el email.")
    return None


# ─────────────────────────────────────────────────────────
# 3. NORMALIZAR COLUMNAS
# ─────────────────────────────────────────────────────────
def normalizar_columnas(tmp_path: str) -> str:
    """
    Renombra columnas del export diario al esquema de analizar_ventas.py
    y sobreescribe el archivo temporal ANTES de llamar a cargar_xlsx().
    """
    df = pd.read_excel(tmp_path, header=0)
    log.info(f"Columnas en el export: {list(df.columns)}")

    renombrar = {k: v for k, v in MAPEO_COLUMNAS.items() if k in df.columns}
    df = df.rename(columns=renombrar)

    faltantes = [c for c in COLS_REQUERIDAS if c not in df.columns]
    if faltantes:
        raise ValueError(
            f"Columnas faltantes después de normalizar: {faltantes}\n"
            f"Revisar MAPEO_COLUMNAS contra el export real de Nexion."
        )

    log.info(f"✓ Columnas OK. Renombradas: {list(renombrar.keys())}")
    df.to_excel(tmp_path, index=False)
    return tmp_path


# ─────────────────────────────────────────────────────────
# 4. CÁLCULO DE FLETE
# ─────────────────────────────────────────────────────────
def calcular_flete(df: pd.DataFrame, periodo: str) -> dict:
    """
    Calcula métricas de flete del período.

    Identificación: Nombre familia == 'FLETES'
    Artículos:
        FLETETN1 → Gasto de envío x tonelada local
        FLETETN2 → Gasto de envío x tonelada larga distancia
        WEB      → Gasto de envío web

    Neto neto = Neto facturas + Neto NC
    (las NC ya vienen con Neto en negativo desde Nexion)
    """
    print("\n🚚 Calculando métricas de flete...")

    df_flete = df[df["Nombre familia"].str.upper() == "FLETES"].copy()

    if df_flete.empty:
        print("  ⚠ Sin líneas de flete en el período.")
        return {"periodo": periodo, "sin_datos": True}

    # Separar por tipo de comprobante
    mask_fact = df_flete["Nom Sis TCOM"].isin(FACTURAS)
    mask_nc   = df_flete["Nom Sis TCOM"].isin(NOTAS_CREDITO)
    mask_nd   = df_flete["Nom Sis TCOM"].isin(NOTAS_DEBITO)

    df_fact = df_flete[mask_fact]
    df_nc   = df_flete[mask_nc]
    df_nd   = df_flete[mask_nd]

    # Totales
    neto_facturas  = float(df_fact["Neto"].sum())
    neto_nc        = float(df_nc["Neto"].sum())    # ya negativo desde Nexion
    neto_nd        = float(df_nd["Neto"].sum())
    neto_neto      = neto_facturas + neto_nc + neto_nd  # NC negativa → resta
    costo_total    = float(df_fact["Costo"].sum())
    margen_total   = neto_neto - costo_total
    margen_pct     = round(margen_total / neto_neto * 100, 1) if neto_neto else 0.0

    cant_facturas  = float(df_fact["Cantidad Vendida"].sum())
    cant_nc        = float(df_nc["Cantidad Vendida"].sum())
    cant_neta      = cant_facturas + cant_nc    # NC negativa → resta

    lineas_fact    = int(len(df_fact))
    lineas_nc      = int(len(df_nc))
    comprob_fact   = int(df_fact["Número"].nunique()) if "Número" in df_fact.columns else 0

    print(f"  Facturas : {lineas_fact:,} líneas | "
          f"Cant: {cant_facturas:,.2f} | Neto: ${neto_facturas:,.0f}")
    print(f"  NC       : {lineas_nc:,} líneas | "
          f"Cant: {cant_nc:,.2f} | Neto: ${neto_nc:,.0f}")
    print(f"  Neto real: ${neto_neto:,.0f} | Margen: {margen_pct}%")

    # Por artículo
    por_articulo = {}
    if "Artículo" in df_flete.columns:
        for art, grp in df_flete.groupby("Artículo"):
            grp_fact = grp[grp["Nom Sis TCOM"].isin(FACTURAS)]
            grp_nc   = grp[grp["Nom Sis TCOM"].isin(NOTAS_CREDITO)]
            n_fact   = float(grp_fact["Neto"].sum())
            n_nc     = float(grp_nc["Neto"].sum())
            n_neto   = n_fact + n_nc
            desc     = grp["Descripción"].iloc[0] if "Descripción" in grp.columns else str(art)
            por_articulo[str(art)] = {
                "descripcion":     str(desc),
                "lineas_facturas": int(len(grp_fact)),
                "lineas_nc":       int(len(grp_nc)),
                "cantidad_fact":   round(float(grp_fact["Cantidad Vendida"].sum()), 3),
                "cantidad_nc":     round(float(grp_nc["Cantidad Vendida"].sum()), 3),
                "cantidad_neta":   round(float(grp_fact["Cantidad Vendida"].sum())
                                         + float(grp_nc["Cantidad Vendida"].sum()), 3),
                "neto_facturas":   round(n_fact, 2),
                "neto_nc":         round(n_nc, 2),
                "neto_neto":       round(n_neto, 2),
                "participacion_pct": round(n_neto / neto_neto * 100, 1) if neto_neto else 0.0,
            }

    # Por sucursal
    por_sucursal = {}
    if "sucursal" in df_flete.columns:
        for suc, grp in df_flete.groupby("sucursal"):
            grp_fact = grp[grp["Nom Sis TCOM"].isin(FACTURAS)]
            grp_nc   = grp[grp["Nom Sis TCOM"].isin(NOTAS_CREDITO)]
            n_fact   = float(grp_fact["Neto"].sum())
            n_nc     = float(grp_nc["Neto"].sum())
            n_neto   = n_fact + n_nc
            por_sucursal[str(suc)] = {
                "lineas_facturas": int(len(grp_fact)),
                "lineas_nc":       int(len(grp_nc)),
                "cantidad_fact":   round(float(grp_fact["Cantidad Vendida"].sum()), 3),
                "cantidad_neta":   round(float(grp_fact["Cantidad Vendida"].sum())
                                         + float(grp_nc["Cantidad Vendida"].sum()), 3),
                "neto_facturas":   round(n_fact, 2),
                "neto_nc":         round(n_nc, 2),
                "neto_neto":       round(n_neto, 2),
                "participacion_pct": round(n_neto / neto_neto * 100, 1) if neto_neto else 0.0,
            }

    # Por vendedor (top cobradores de flete)
    por_vendedor = {}
    if "Vendedor" in df_flete.columns and "Nombre vendedor" in df_flete.columns:
        for (cod, nom), grp in df_flete.groupby(["Vendedor", "Nombre vendedor"]):
            grp_fact = grp[grp["Nom Sis TCOM"].isin(FACTURAS)]
            grp_nc   = grp[grp["Nom Sis TCOM"].isin(NOTAS_CREDITO)]
            n_neto   = float(grp_fact["Neto"].sum()) + float(grp_nc["Neto"].sum())
            if abs(n_neto) < 1:
                continue
            por_vendedor[str(cod)] = {
                "nombre":         str(nom),
                "neto_neto":      round(n_neto, 2),
                "cantidad_neta":  round(float(grp_fact["Cantidad Vendida"].sum())
                                        + float(grp_nc["Cantidad Vendida"].sum()), 3),
                "lineas_facturas": int(len(grp_fact)),
                "lineas_nc":      int(len(grp_nc)),
                "neto_por_linea": round(n_neto / len(grp_fact), 2) if len(grp_fact) else 0,
            }
        # Ordenar por neto descendente
        por_vendedor = dict(
            sorted(por_vendedor.items(), key=lambda x: x[1]["neto_neto"], reverse=True)
        )

    resultado = {
        "periodo":           periodo,
        "timestamp":         datetime.now(timezone.utc).isoformat(),

        # Totales globales
        "lineas_facturas":   lineas_fact,
        "lineas_nc":         lineas_nc,
        "comprobantes":      comprob_fact,
        "cantidad_fact":     round(cant_facturas, 3),
        "cantidad_nc":       round(cant_nc, 3),
        "cantidad_neta":     round(cant_neta, 3),
        "neto_facturas":     round(neto_facturas, 2),
        "neto_nc":           round(neto_nc, 2),
        "neto_nd":           round(neto_nd, 2),
        "neto_neto":         round(neto_neto, 2),      # ← el dato real
        "costo_total":       round(costo_total, 2),
        "margen_total":      round(margen_total, 2),
        "margen_pct":        margen_pct,

        # Desgloses
        "por_articulo":  por_articulo,
        "por_sucursal":  por_sucursal,
        "por_vendedor":  por_vendedor,
    }

    print(f"  ✓ Flete calculado: {len(por_articulo)} artículos, "
          f"{len(por_sucursal)} sucursales, {len(por_vendedor)} vendedores")
    return resultado


def subir_flete(resultado_flete: dict, periodo: str, db):
    """
    Sube las métricas de flete a:
    indicadores/comercial/ventas_analytics/{periodo}/flete/resumen
    """
    ref = (
        db.collection(ROOT_COL)
        .document(AREA_DOC)
        .collection(SUBCOLLECTIONS["ventas"])
        .document(periodo)
        .collection("flete")
        .document("resumen")
    )
    ref.set(resultado_flete)
    print(f"  ✓ {ROOT_COL}/{AREA_DOC}/{SUBCOLLECTIONS['ventas']}"
          f"/{periodo}/flete/resumen → Firestore OK")


# ─────────────────────────────────────────────────────────
# 5. MARCAR EMAIL COMO PROCESADO
# ─────────────────────────────────────────────────────────
def marcar_procesado(service, msg_id: str):
    labels   = service.users().labels().list(userId="me").execute().get("labels", [])
    label_id = next(
        (l["id"] for l in labels if l["name"] == LABEL_PROCESADO), None
    )
    if not label_id:
        nuevo    = service.users().labels().create(
            userId="me",
            body={"name": LABEL_PROCESADO, "labelListVisibility": "labelShow"}
        ).execute()
        label_id = nuevo["id"]
        log.info(f"Label creado: {LABEL_PROCESADO}")

    service.users().messages().modify(
        userId="me", id=msg_id,
        body={"addLabelIds": [label_id]}
    ).execute()
    log.info(f"Email marcado como {LABEL_PROCESADO}")


# ─────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info("PIPELINE VENTAS: Gmail → ADN Vendedores + Flete → Firebase")
    log.info("=" * 60)

    tmp_path = None
    try:
        # 1. Gmail
        service = get_gmail_service()
        msg_id  = buscar_email(service)
        if not msg_id:
            return

        # 2. Descargar Excel a archivo temporal
        tmp_path = descargar_a_tempfile(service, msg_id)
        if not tmp_path:
            return

        # 3. Normalizar columnas → sobreescribe el temp
        tmp_path = normalizar_columnas(tmp_path)

        # 4. Pipeline ADN — analizar_ventas.py sin modificaciones
        df = cargar_xlsx(tmp_path)
        df = limpiar_datos(df)

        fecha_max = df["Fec. Comp."].max()
        periodo   = fecha_max.strftime("%Y-%m") if pd.notna(fecha_max) \
                    else datetime.now().strftime("%Y-%m")
        log.info(f"Período: {periodo}  (datos hasta {fecha_max.date()})")

        vendedores = calcular_metricas_vendedor(df)
        vendedores = calcular_score_adn(vendedores)
        vendedores = asignar_patterns_y_riesgos(vendedores)
        sucursales = calcular_sucursales(df, vendedores)
        clientes   = calcular_clientes(df)
        articulos  = calcular_articulos(df)

        imprimir_resumen(vendedores, sucursales, clientes, articulos, df)

        # Sube a indicadores/comercial/ventas_analytics/{periodo}
        subir_a_firebase(periodo, vendedores, sucursales, clientes, articulos, df)

        # 5. Flete — usa Firebase ya inicializado por subir_a_firebase()
        import firebase_admin
        from firebase_admin import firestore as fs
        db = fs.client()

        flete = calcular_flete(df, periodo)
        subir_flete(flete, periodo, db)

        # 6. Marcar email procesado
        marcar_procesado(service, msg_id)

        log.info(f"\n✅ Pipeline completo — {periodo}")
        log.info(f"   Vendedores    : {len(vendedores)}")
        log.info(f"   Neto flete    : ${flete.get('neto_neto', 0)/1e6:.2f}M")
        log.info(f"   Margen flete  : {flete.get('margen_pct', 0)}%")

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
            log.info("Archivo temporal eliminado.")


if __name__ == "__main__":
    main()
