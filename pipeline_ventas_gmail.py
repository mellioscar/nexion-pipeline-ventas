"""
pipeline_ventas_gmail.py
Bridge: Gmail → Excel → analizar_ventas.py → proyectado_ventas.py
Carlos Isla y Cía. — NET-LogistK ISLA

Flujo:
    1. Busca en Gmail el email con asunto configurable (no procesado aún)
    2. Descarga el adjunto .xlsx a un archivo temporal
    3. Renombra columnas del formato diario al esquema de analizar_ventas.py
    4. Llama a cargar_xlsx() → limpiar_datos() → pipeline ADN completo
    5. Llama a calcular_proyectado() usando feriados de Firestore
    6. Marca el email con label NEXION_VTA_PROCESADO

Notas:
    - cargar_xlsx() espera un PATH de archivo → usamos tempfile
    - subir_a_firebase() inicializa Firebase con FIREBASE_CREDENTIALS_PATH
    - proyectado sube a indicadores/comercial/ventas_proyectado/{periodo}
    - si el token OAuth se renueva, crea TOKEN_REFRESHED para que
      GitHub Actions lo persista de vuelta al Secret
"""

import os
import base64
import tempfile
import logging
from datetime import datetime
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
from proyectado_ventas import calcular_proyectado, subir_proyectado

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
# Solo las que cambian de nombre. Las que ya coinciden no se listan.
# Ajustar si Nexion cambia algún nombre de columna en el futuro.
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


def get_gmail_service():
    """
    OAuth2 con persistencia de token refresh.
    Crea TOKEN_REFRESHED si el token fue renovado → el workflow de
    GitHub Actions lo detecta y actualiza el Secret automáticamente.
    """
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
            # Primera ejecución local: abre el navegador una sola vez
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
    """
    Descarga el adjunto .xlsx a un archivo temporal y retorna su path.
    cargar_xlsx() requiere un path de archivo, no bytes en memoria.
    """
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


def normalizar_columnas(tmp_path: str) -> str:
    """
    Lee el Excel del export diario, renombra las columnas al esquema
    que espera cargar_xlsx(), y sobreescribe el mismo archivo temporal.

    Importante: hay que hacerlo ANTES de llamar a cargar_xlsx()
    porque esa función usa sys.exit(1) si faltan columnas.
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

    log.info(f"✓ Columnas normalizadas. Renombradas: {list(renombrar.keys())}")
    df.to_excel(tmp_path, index=False)
    return tmp_path


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


def main():
    log.info("=" * 60)
    log.info("PIPELINE VENTAS: Gmail → ADN Vendedores + Proyectado")
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
        df = cargar_xlsx(tmp_path)   # valida columnas, usa sys.exit(1) si faltan
        df = limpiar_datos(df)

        # Período desde los datos (no desde el nombre del archivo)
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

        # 5. Proyectado — usa Firebase ya inicializado por subir_a_firebase()
        import firebase_admin
        from firebase_admin import firestore as fs
        db         = fs.client()
        proyectado = calcular_proyectado(df, vendedores, periodo, db)
        subir_proyectado(proyectado, db)   # → indicadores/comercial/ventas_proyectado/{periodo}

        # 6. Marcar email procesado
        marcar_procesado(service, msg_id)

        log.info(f"\n✅ Pipeline completo — {periodo}")
        log.info(f"   Vendedores : {len(vendedores)}")
        log.info(f"   Proyectado : ${proyectado['neto_proyectado']/1e6:.2f}M al cierre")

    finally:
        # Siempre limpiar el temp aunque haya error
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)
            log.info("Archivo temporal eliminado.")


if __name__ == "__main__":
    main()
