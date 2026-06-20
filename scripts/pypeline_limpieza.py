import pandas as pd
import spacy
import sys
import gc
from pathlib import Path

# ── Configuración ─────────────────────────────────────────────────────────────
INPUT_FILE = Path("data/intervenciones.parquet")
OUTPUT_FILE = Path("data/intervenciones_limpias.parquet")
TEMP_DIR = Path("data/temp_chunks")
CHUNK_SIZE = 5000
OVERLAP = 500
MAX_LARGO_TEXTO = 100_000  # Filas más largas que esto son errores de scraping
                            # (sesiones completas pegadas a la intervención del
                            # Presidente/Secretario, no una intervención real)

# ── Stopwords Customizadas ────────────────────────────────────────────────────
TERMINOS_PARLAMENTARIOS = {
    # ── BASE Y PROCEDIMENTAL ──
    "señor", "señora", "presidente", "presidenta", "diputado", "diputada", "honorable", "cámara", 
    "nación", "sesión", "artículo", "ley", "proyecto", "dictamen", "palabra", "bloque", "inciso", 
    "apartado", "comisión", "cuarto", "intermedio", "afirmativo", "negativo", "voto", "votación", 
    "mayoría", "minoría", "quiero", "decir", "hacer", "gracias", "muchas", "nacional", "votar", 
    "resultar", "afirmativa", "negativa", "nominal", "observación", "cuestión", "privilegio", 
    "asuntos", "presidencia", "asentimiento", "expediente", "resolución", "moción", "consideración", 
    "particular", "general", "interrupción", "preferencia", "tabla", "tratamiento", "reglamento", 
    "despacho", "rechazado", "venia", "conceder", "abstención", "tablero", "electrónico", "secretaría", 
    "lectura", "orador", "silencio", "resultado", "labor", "leer", "ausente", "instante", "formulario", 
    "formulado", "aclaración", "homenaje", "permitir", "exposición", "autorización", "práctica", 
    "aplauso", "aplausos", "diario", "redondee", "sesiones", "hora", "minuto", "pedir", "solicitar", 
    "quedar", "modificación", "término", "orden", "forma", "corresponder", "pasar", "sancionado", 
    "comunicar", "nacion", "senado", "informe", "vencido", "disidencia", "aprobar", "único", 
    "reglamentario", "destinado", "vencer", "plan", "continuar", "llamar", "guardir", "favor", 
    "practicar", "conforme", "presente", "nominalmente", "rmativo", "capítulo", "modifi", "sentido", 
    "indicar", "expresar", "haber", "manifestar", "considerar", "aprobado", "constancia", "acordado", 
    "ejecutivo", "propuesto", "aceptar", "aceptado", "parlamentaria", "informante", "miembro", "proponer",
    "inserción", "redondear", "constitucionales", "planteado", "girar", "posterior", "continuación", 
    "proceder", "indicado", "articulo", "conjunto", "dar", "pedido", "cerrar", "convocar", "especial", 
    "dictado", "entrada", "plantear", "tiempo", "hablar", "permiso", "aludir", "tablas", "asunto", 
    "mocion", "formular", "contenido", "incorporar", "requerir", "emitir", "juramento", "tercio", 
    "galería", "parte", "invitar", "distrito", "electo", "cuartos", "fecha", "art", "deber", "licencia", 
    "efectuar", "autorizar", "acuerdir", "curso", "consecuencia", "transcurso", "texto", "apéndice", 
    "sanción", "página", "apartamiento", "informar", "disponer", "solicito", "desear", "abstener", 
    "resulta", "considera", "tercero", "registrar", "referir", "computar", "acta", "dejar", "simplemente", 
    "asentado", "sentado", "querer", "posición", "terminar", "efecto", "comprender", "título", 
    "inclusive", "fórmula", "puesto", "prestar", "asistente", "banca", "propuesta", "aclarar", "tratar", 
    "acabar", "entender", "facultad", "recinto", "pie", "mástil", "izar", "bandera", "patria", "número", 
    "discurso", "insertar", "concluir", "lista", "pronunciar", "anotado", "cierre", "pensar", 
    "oportunamente", "declarar", "declaración", "agrado", "sala", "ver", "acompañar", "expuesto", 
    "solicitado", "nómina", "petición", "concedido", "entrados", "comunicación", "pertinente", 
    "respectivo", "cuyo", "correspondiente", "fundamento", "aprobación", "reunión", "agosto", 
    "septiembre", "diciembre", "camara", "presiden",

    # ── LOS INMORTALES Y NÚMEROS ROMANOS ──
    "él", "el", "de", "se", "con", "pro", "re", "sr", "ca", "tado", "mos", "pre", "aca", "en", "es",
    "iii", "vii", "viii", "iv", "vi", "ix",

    # ── OCR Y PALABRAS CORTADAS ──
    "ción", "cación", "modi", "rmativa", "apruer", "rrupción", "inte", "pala", "bra", "bue", "sefíor", 
    "seftor", "sefioro", "seílor", "señ", "tienir", "véasar", "vea", "cues", "tión", "artícu", "dipu", 
    "afinnativo", "afim1ativo", "aproba", "or", "sin", "in", "cia", "dad", "co", "acor", "hubierar", "don"
}

NOMBRES_Y_PROVINCIAS = {
    # ── APELLIDOS Y NOMBRES ──
    "juan", "carlos", "maria", "josé", "jose", "luis", "ana", "jorge", "miguel", "julio", "mario", 
    "fernando", "pablo", "roberto", "laura", "silvia", "claudio", "marcelo", "daniel", "diego", 
    "martin", "martín", "garcía", "gonzález", "gutiérrez", "díaz", "rodríguez", "pérez", "martínez",
    "camaño", "moreau", "lospennato", "ritondo", "heller", "giudici", "ferraro", "iglesias", "bregman", 
    "tetaz", "penacca", "lavagna", "plá", "caño", "herrera", "fernández", "rossi", "maría", "moreno", 
    "cigogna", "godoy", "lópez", "alonso", "bianchi", "recalde", "solanas", "carmona", "lozano", "perié",
    "conti", "otar", "leiva", "romero", "rosa", "méndez", "snopek", "fadel", "macaluse", "marino", 
    "salim", "acuña", "burgos", "brügge", "grana", "pugliese", "stubrin", "germán", "pierri", "cafiero", 
    "silva", "osorio", "fiol", "ferreyra", "bancalari", "pastori", "carrizo", "giubergia", "bertone", 
    "kunkel", "tullio", "bullrich", "carlotto", "roig", "pinedo", "comelli", "canela", "berraute", 
    "alvarez", "cantero", "gorbacz", "bianco", "iturrieta", "marconato", "carbajal", "buryaile", 
    "basterra", "parola",

    # ── PROVINCIAS ──
    "aires", "ciudad", "autónoma", "capital", "federal", "santa", "córdoba", "jujuy", "mendoza", 
    "chaco", "corrientes", "negro", "cruz", "catamarca", "rioja", "estero", "santiago", "san", 
    "salta", "pampa", "tucumán", "neuquén", "tierra", "fuego", "misiones", "río", "ríos", "formosa"
}

STOPWORDS_EXTENDIDAS = TERMINOS_PARLAMENTARIOS.union(NOMBRES_Y_PROVINCIAS)

# ── Funciones ─────────────────────────────────────────────────────────────────

def chunk_text(text: str, max_words=CHUNK_SIZE, overlap=OVERLAP) -> list[str]:
    if not isinstance(text, str):
        return []
    words = text.split()
    if len(words) <= max_words:
        return [text]
    chunks = []
    for i in range(0, len(words), max_words - overlap):
        chunk = words[i : i + max_words]
        chunks.append(" ".join(chunk))
        if i + max_words >= len(words):
            break
    return chunks

def apply_chunking(df: pd.DataFrame) -> pd.DataFrame:
    print(f"Aplicando chunking (Max: {CHUNK_SIZE} palabras, Overlap: {OVERLAP})...")
    df['texto_chunk'] = df['texto'].apply(chunk_text)
    df_exploded = df.explode('texto_chunk').reset_index(drop=True)
    df_exploded['n_chunk'] = df_exploded.groupby(['id_periodo', 'id_reunion', 'n_intervencion']).cumcount() + 1
    return df_exploded

def process_and_save_in_batches(df: pd.DataFrame, batch_size_df=20000) -> list[Path]:
    """
    Procesa el DataFrame en bloques y guarda cada bloque en disco para evitar OOM.
    """
    print("Cargando modelo de spaCy...")
    # max_length: por si algún texto/chunk es más largo de lo esperado tras el explode
    nlp = spacy.load("es_core_news_md", disable=["ner", "parser", "attribute_ruler"])
    nlp.max_length = 2_000_000
    nlp.Defaults.stop_words |= STOPWORDS_EXTENDIDAS

    total_rows = len(df)
    print(f"\nProcesando {total_rows} filas en bloques de a {batch_size_df}...")
    
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    archivos_temp = []

    # Iterar el dataframe en saltos
    for start_idx in range(0, total_rows, batch_size_df):
        end_idx = min(start_idx + batch_size_df, total_rows)
        print(f"  -> Procesando filas {start_idx} a {end_idx}...")
        
        # 1. Extraer solo un fragmento del DataFrame para la memoria
        df_batch = df.iloc[start_idx:end_idx].copy()
        
        # 2. Procesar EN UN SOLO PROCESO (n_process=1).
        #    Con modelos "md" (no transformer), el multiprocessing de spaCy
        #    suele ser más lento y más pesado en RAM que correrlo en un solo
        #    proceso, porque cada worker carga su propia copia del modelo
        #    y los textos se serializan (pickle) para mandarse a cada proceso.
        #    batch_size moderado para no acumular muchos Doc en memoria a la vez.
        docs = nlp.pipe(df_batch['texto_chunk'], n_process=8, batch_size=200)
        
        cleaned_texts = []
        for doc in docs:
            tokens = [
                token.lemma_.lower() for token in doc 
                if not token.is_punct and not token.is_space and not token.is_stop
                and len(token.lemma_) > 2
                and token.lemma_.lower() not in STOPWORDS_EXTENDIDAS
            ]
            cleaned_texts.append(" ".join(tokens))
        
        # 3. Limpiar y guardar
        df_batch['texto_limpio'] = cleaned_texts
        df_batch = df_batch[df_batch['texto_limpio'].str.strip() != '']
        df_batch = df_batch.drop(columns=['texto', 'texto_chunk'], errors='ignore')
        
        temp_file = TEMP_DIR / f"chunk_{start_idx}.parquet"
        df_batch.to_parquet(temp_file, index=False)
        archivos_temp.append(temp_file)
        
        # 4. LIBERAR RAM A LA FUERZA
        del df_batch
        del cleaned_texts
        del docs
        gc.collect() # Llama al Garbage Collector de Python
        
    return archivos_temp

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    if not INPUT_FILE.exists():
        print(f"Error: No se encontró {INPUT_FILE}")
        sys.exit(1)

    print("1. Cargando intervenciones originales...")
    df = pd.read_parquet(INPUT_FILE)
    df = df[df['largo_texto'] > 50].copy()
    df['texto'] = df['texto'].str.replace('ﬁ', 'fi')

    # Filtrar outliers: filas con texto anormalmente largo son errores de
    # scraping (sesiones completas pegadas a una sola intervención), no
    # intervenciones reales. Sin este filtro, unas pocas filas generan miles
    # de chunks extra y explotan la RAM/CPU en la lematización.
    n_antes = len(df)
    df = df[df['largo_texto'] <= MAX_LARGO_TEXTO].copy()
    n_descartadas = n_antes - len(df)
    print(f"   Descartadas {n_descartadas} filas con largo_texto > {MAX_LARGO_TEXTO} "
          f"(errores de scraping, sesiones completas pegadas).")
    print(f"   Quedan {len(df)} filas.")

    print("\n2. Preparando textos (Chunking)...")
    df_chunked = apply_chunking(df)
    
    # Borramos el df original para liberar RAM temprana
    del df
    gc.collect()

    print("\n3. Lematización por lotes (proceso único)...")
    archivos_generados = process_and_save_in_batches(df_chunked, batch_size_df=20000)

    print("\n4. Uniendo todos los bloques...")
    all_dfs = [pd.read_parquet(f) for f in archivos_generados]
    df_final = pd.concat(all_dfs, ignore_index=True)
    
    print("Guardando resultado final...")
    df_final.to_parquet(OUTPUT_FILE, index=False)
    
    print(f"\n{'='*60}")
    print("¡Preprocesamiento completado de forma segura!")
    print(f"Textos finales guardados: {len(df_final)}")
    print(f"Ruta: {OUTPUT_FILE}")