"""
Scraper de Diarios de Sesiones → Parquet (v3)
===============================================

En vez de descargar PDFs, los lee en memoria, extrae el texto
y guarda un archivo Parquet por período.

Estructura del Parquet:
    id_periodo | id_reunion | fecha | descripcion | texto_pdf

Requisitos:
    pip install beautifulsoup4 requests pypdf pandas pyarrow
"""

import os
import re
import io
import time
import logging
import requests
import pandas as pd
from pypdf import PdfReader
from urllib.parse import parse_qs, urlparse
from bs4 import BeautifulSoup

# ─── Configuración ───────────────────────────────────────────────────────────

BASE_URL = "https://www.diputados.gob.ar/sesiones/"
OUTPUT_DIR = "parquets"
DELAY = 2

PDF_SERVERS = [
    "https://www3.hcdn.gob.ar",
    "https://www4.hcdn.gob.ar",
    "https://www1.hcdn.gob.ar",
]

# *** SOLO PERIODO 143 PARA TESTEAR ***
PERIODOS_FILTRO = None

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scraper.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger(__name__)


# ─── Paso 1: Obtener sesiones ───────────────────────────────────────────────

def obtener_sesiones():
    logger.info("Obteniendo lista de sesiones...")
    resp = requests.get(BASE_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    sesiones = []

    for link in soup.find_all("a", href=True):
        href = link["href"]
        if "sesion.html" not in href:
            continue

        texto = link.get_text(strip=True)
        if not texto:
            continue

        params = parse_qs(urlparse(href).query)
        sesion_id = params.get("id", [None])[0]
        periodo = params.get("periodo", [None])[0]
        reunion = params.get("reunion", [None])[0]

        if not sesion_id or not periodo:
            continue

        fecha_match = re.search(r"\((\d{2})/(\d{2})/(\d{4})\)", texto)
        if not fecha_match:
            continue

        dia, mes, anio = fecha_match.groups()

        url_sesion = f"https://www.diputados.gob.ar/sesiones/sesion.html?id={sesion_id}&numVid=0&reunion={reunion}&periodo={periodo}"

        sesion = {
            "id": sesion_id,
            "periodo": int(periodo),
            "reunion": int(reunion) if reunion and reunion != "null" else None,
            "dia": dia,
            "mes": mes,
            "anio": anio,
            "fecha": f"{anio}-{mes}-{dia}",
            "descripcion": texto,
            "url_sesion": url_sesion,
        }

        if PERIODOS_FILTRO and sesion["periodo"] not in PERIODOS_FILTRO:
            continue

        if not any(s["id"] == sesion_id for s in sesiones):
            sesiones.append(sesion)

    logger.info(f"Encontradas {len(sesiones)} sesiones.")
    return sesiones


# ─── Paso 2: Construir URLs del PDF ─────────────────────────────────────────

def construir_urls_pdf(sesion):
    periodo = sesion["periodo"]
    anio = sesion["anio"]
    mes = sesion["mes"]
    dia = sesion["dia"]
    reunion = sesion["reunion"]

    path_base = f"/dependencias/dtaquigrafos/diarios/periodo-{periodo}"
    urls = []

    if reunion is not None:
        reunion_padded = str(reunion).zfill(2)
        reunion_num = str(reunion)

        variantes = [
            f"diario_{anio}{mes}{dia}{reunion_padded}.pdf",
            f"diario_{anio}{mes}{dia}{reunion_num}.pdf",
        ]
        for server in PDF_SERVERS:
            for variante in variantes:
                urls.append(f"{server}{path_base}/{variante}")

    for server in PDF_SERVERS:
        urls.append(f"{server}{path_base}/diario_{anio}{mes}{dia}.pdf")

    return urls


# ─── Paso 3: Descargar PDF en memoria y extraer texto ───────────────────────

def extraer_texto_pdf(sesion):
    """Descarga el PDF en memoria (sin guardar a disco) y extrae el texto."""
    urls = construir_urls_pdf(sesion)

    for url in urls:
        try:
            resp = requests.get(url, timeout=60)

            if resp.status_code != 200:
                continue

            # Verificar que sea un PDF real
            if not resp.content[:5].startswith(b"%PDF"):
                continue

            # Leer el PDF directamente desde bytes en memoria
            pdf_reader = PdfReader(io.BytesIO(resp.content))

            texto_completo = ""
            for page in pdf_reader.pages:
                page_text = page.extract_text()
                if page_text:
                    texto_completo += page_text + "\n"

            texto_completo = texto_completo.strip()

            if len(texto_completo) < 50:  # Muy poco texto = probablemente falló
                continue

            size_mb = len(resp.content) / (1024 * 1024)
            logger.info(
                f"  ✓ Extraído: {len(texto_completo):,} chars "
                f"(PDF: {size_mb:.1f} MB) desde {url}"
            )
            return texto_completo

        except Exception as e:
            logger.debug(f"  Falló {url}: {e}")
            continue

    logger.warning(f"  ✗ No se pudo extraer texto: {sesion['descripcion'][:70]}")
    return None


# ─── Paso 4: Procesar un período y guardar Parquet ──────────────────────────

def procesar_periodo(periodo, sesiones):
    """Procesa todas las sesiones de un período y guarda un Parquet."""
    sesiones_periodo = [s for s in sesiones if s["periodo"] == periodo]
    logger.info(f"\n{'='*60}")
    logger.info(f"PERÍODO {periodo}: {len(sesiones_periodo)} sesiones")
    logger.info(f"{'='*60}")

    registros = []

    for i, sesion in enumerate(sesiones_periodo, 1):
        logger.info(f"\n[{i}/{len(sesiones_periodo)}] {sesion['descripcion'][:70]}")

        texto = extraer_texto_pdf(sesion)

        registros.append({
            "id_periodo": sesion["periodo"],
            "id_reunion": sesion["reunion"],
            "fecha": sesion["fecha"],
            "descripcion": sesion["descripcion"],
            "url_sesion": sesion["url_sesion"],
            "texto_pdf": texto,  # None si no se encontró
        })

        time.sleep(DELAY)

    # Crear DataFrame y guardar como Parquet
    df = pd.DataFrame(registros)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"periodo_{periodo}.parquet")
    df.to_parquet(filepath, index=False, engine="pyarrow")

    # Resumen
    con_texto = df["texto_pdf"].notna().sum()
    sin_texto = df["texto_pdf"].isna().sum()
    size_kb = os.path.getsize(filepath) / 1024

    logger.info(f"\n--- Resumen período {periodo} ---")
    logger.info(f"  Archivo:    {filepath}")
    logger.info(f"  Tamaño:     {size_kb:.0f} KB")
    logger.info(f"  Con texto:  {con_texto}/{len(registros)}")
    logger.info(f"  Sin texto:  {sin_texto}/{len(registros)}")

    return df


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    sesiones = obtener_sesiones()
    if not sesiones:
        logger.error("No se encontraron sesiones.")
        return

    # Obtener períodos únicos
    periodos = sorted(set(s["periodo"] for s in sesiones))
    logger.info(f"Períodos a procesar: {periodos}")

    for periodo in periodos:
        procesar_periodo(periodo, sesiones)

    logger.info("\n✓ ¡Listo! Parquets guardados en ./" + OUTPUT_DIR + "/")


if __name__ == "__main__":
    main()