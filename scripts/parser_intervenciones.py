"""
Parser de intervenciones taquigráficas - Cámara de Diputados Argentina
========================================================================
Toma un parquet con columnas: id_periodo, id_reunion, fecha, descripcion, url_sesion, texto_pdf
Produce un parquet/csv con: id_periodo, id_reunion, fecha, titulo, orador, n_intervencion, texto, largo_texto

Uso:
    python parser_intervenciones.py periodo_143.parquet
    python parser_intervenciones.py *.parquet              # procesar varios
"""

import pandas as pd
import re
import sys
from pathlib import Path

# ── Patrones ──────────────────────────────────────────────────────────────────

# Patrón principal: "Sr./Sra./Srta. NOMBRE. –"
SPEAKER_PATTERN = re.compile(
    r'(?:Sr|Sra|Srta)\.\s+'         # Prefijo de tratamiento
    r'([^.—–\-\n]+?)'               # Nombre del orador
    r'\s*\.\s*[—–\-]\s*'            # ". –" (variantes de dash y espaciado)
)

# Encabezados de página del PDF (para remover del texto)
PAGE_HEADER_A = re.compile(
    r'\n?\w+\s+\d{1,2}\s+de\s+\d{4}\s+CÁMARA DE DIPUTADOS DE LA NACIÓN\s+\d+\n?'
)
PAGE_HEADER_B = re.compile(
    r'\n?\d+\s+CÁMARA DE DIPUTADOS DE LA NACIÓN\s+Reunión\s+\d+[ªº]?\n?'
)


# ── Funciones ─────────────────────────────────────────────────────────────────

def clean_intervention_text(raw_text: str) -> str:
    """Limpia artefactos del PDF: headers de página, guiones de corte, espacios."""
    text = raw_text
    text = PAGE_HEADER_A.sub(' ', text)
    text = PAGE_HEADER_B.sub(' ', text)
    # Reunir palabras cortadas por guión al final de línea
    text = re.sub(r'(\w)\s*-\s*\n\s*(\w)', r'\1\2', text)
    # Colapsar saltos de línea y espacios múltiples
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'\s{2,}', ' ', text).strip()
    return text


def parse_session(row: pd.Series) -> list[dict]:
    """Parsea el texto_pdf de una sesión en intervenciones individuales."""
    text = row['texto_pdf']
    if pd.isna(text) or not text.strip():
        return []

    matches = list(SPEAKER_PATTERN.finditer(text))
    if not matches:
        return []

    results = []
    for i, m in enumerate(matches):
        raw_name = m.group(1)
        speaker = re.sub(r'\s+', ' ', raw_name).strip()

        prefix = m.group(0)
        if 'Srta.' in prefix:
            titulo = 'Srta.'
        elif 'Sra.' in prefix:
            titulo = 'Sra.'
        else:
            titulo = 'Sr.'

        text_start = m.end()
        text_end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        intervention_text = clean_intervention_text(text[text_start:text_end])

        results.append({
            'id_periodo': row['id_periodo'],
            'id_reunion': row['id_reunion'],
            'fecha': row['fecha'],
            'titulo': titulo,
            'orador': speaker,
            'n_intervencion': i + 1,
            'texto': intervention_text,
            'largo_texto': len(intervention_text),
        })

    return results


def process_parquet(input_path: str) -> pd.DataFrame:
    """Procesa un archivo parquet completo y retorna DataFrame de intervenciones."""
    df = pd.read_parquet(input_path)
    all_rows = []
    for _, row in df.iterrows():
        all_rows.extend(parse_session(row))
    return pd.DataFrame(all_rows)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    # ── Configuración ─────────────────────────────────────────────────────
    INPUT_DIR = Path(r"data\parquets")
    OUTPUT_DIR = Path("data")
    # ──────────────────────────────────────────────────────────────────────

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    archivos = sorted(INPUT_DIR.glob("periodo_*.parquet"))
    print(f"Parquets encontrados: {len(archivos)}\n")

    all_dfs = []
    for f in archivos:
        result = process_parquet(str(f))
        all_dfs.append(result)
        print(f"  {f.name}: {len(result)} intervenciones")

    combined = pd.concat(all_dfs, ignore_index=True)
    combined.to_parquet(OUTPUT_DIR / "intervenciones.parquet", index=False)
    combined.to_csv(OUTPUT_DIR / "intervenciones.csv", index=False)

    print(f"\n{'='*60}")
    print(f"Total: {len(combined)} intervenciones")
    print(f"Periodos: {combined['id_periodo'].nunique()}")
    print(f"Reuniones: {combined['id_reunion'].nunique()}")
    print(f"Oradores únicos: {combined['orador'].nunique()}")
    print(f"Guardado en {OUTPUT_DIR / 'intervenciones'}.parquet / .csv")