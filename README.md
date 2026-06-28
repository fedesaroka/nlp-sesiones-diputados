# NLP — Sesiones de la Cámara de Diputados de Argentina

Análisis de Topic Modelling sobre las sesiones parlamentarias argentinas (1983–2026) usando datos del [Diario de Sesiones](https://www.diputados.gob.ar/sesiones/).

---

## Estructura del proyecto

```
nlp-sesiones-diputados/
├── data/
│   ├── bertopic_diputados_final/   # Modelo BERTopic serializado (safetensors)
│   ├── topics_over_time.csv        # Salida de topics_over_time() cacheada
│   └── parquets/                   # Parquets crudos por período (no en Git)
├── notebooks/
│   ├── Fase1_Modelado.ipynb        # Experimento 1: entrenamiento de modelos
│   ├── Fase2_Evolucion_Temporal.ipynb  # Experimento 2: evolución temporal
│   └── figuras/                    # HTMLs interactivos exportados
├── scripts/
│   └── pypeline_limpieza.py        # Preprocesamiento y lematización
└── TP1 PLN.pdf                     # Consigna del trabajo práctico
```

> **Archivos no incluidos en Git por tamaño** (generarlos localmente):
> - `data/intervenciones.parquet` / `.csv` — datos crudos scrapeados
> - `data/intervenciones_limpias.parquet` — texto lematizado (generado por `pypeline_limpieza.py`)
> - `data/intervenciones_con_topico.parquet` — asignación de tópicos por intervención
> - `data/embeddings_intervenciones.npy` — embeddings 384-dim (265 MB, ~3 h de cómputo)
> - `data/parquets/` — parquets por período legislativo

---

## Experimento 1 — Modelado de Tópicos (`Fase1_Modelado.ipynb`)

### Objetivo
Entrenar y comparar tres modelos de topic modelling sobre las intervenciones parlamentarias.

### Modelos entrenados
| Modelo | Descripción | Tópicos |
|--------|-------------|---------|
| **LDA** | Latent Dirichlet Allocation (baseline clásico) | k = 15 |
| **NMF** | Non-negative Matrix Factorization (baseline clásico) | k = 15 |
| **BERTopic** | Embeddings + UMAP + HDBSCAN (modelo principal) | 190 (automático) |

### Pipeline de BERTopic
1. **Embeddings**: `paraphrase-multilingual-MiniLM-L12-v2` → vectores 384-dim por intervención
2. **Reducción**: UMAP 384 → 5 dimensiones
3. **Clustering**: HDBSCAN (`min_cluster_size=100`)
4. **Representación**: c-TF-IDF sobre los clusters

### Datos
- 172.417 documentos (intervenciones lematizadas, stopwords parlamentarias removidas)
- Rango: 1983–2026 (sin datos 1991–2000 por falta de digitalización en la HCDN)

### Resultado
El modelo BERTopic quedó guardado en `data/bertopic_diputados_final/` y es el que se usa en los experimentos siguientes.

---

## Experimento 2 — Evolución Temporal (`Fase2_Evolucion_Temporal.ipynb`)

### Objetivo
Analizar cómo varía la prevalencia de los tópicos a lo largo del tiempo y en relación a eventos históricos argentinos.

### Tópicos seleccionados para el análisis
De los 190 tópicos del modelo se seleccionaron 10 temáticamente coherentes:

| ID | Etiqueta |
|----|----------|
| 1 | Presupuesto / Finanzas públicas |
| 5 | Energía / Gas / Combustibles |
| 6 | Derecho penal |
| 8 | Impuestos / Fiscal |
| 11 | Trabajo / Laboral |
| 12 | Salud / Discapacidad |
| 18 | Jubilaciones / Previsional |
| 22 | Defensa / Fuerzas militares |
| 30 | Agropecuario / Ganadería |
| 129 | Derechos humanos / Terrorismo |

### Visualizaciones generadas

1. **Topics over Time** (BERTopic nativo) — evolución de frecuencia por año de los 5 tópicos más frecuentes
2. **Heatmap por año** — prevalencia de los 10 tópicos × cada año disponible
3. **Heatmap por período presidencial** — prevalencia × gobierno (Alfonsín → Milei)
4. **Heatmap por evento histórico** — prevalencia en ventana de 2 años alrededor de 9 hitos:
   Juicio a las Juntas (1985), Hiperinflación (1989), Crisis 2001, Res. 125 (2008), Matrimonio igualitario (2010), YPF (2012), IVE (2018), COVID-19 (2020), Ley Bases (2024)

Todas las figuras se exportan como HTML interactivo en `notebooks/figuras/`.

---

## Experimento 3 — Diferencias temáticas por tipo de sesión (`Fase3_Tipo_Sesion.ipynb`)

### Objetivo
Determinar si existen diferencias estadísticamente significativas en la distribución de tópicos según el tipo de sesión parlamentaria.

### Tipos de sesión analizados
De los 9 tipos existentes se seleccionaron los 4 con suficiente masa de datos:

| Tipo | Sesiones | Intervenciones |
|---|---|---|
| Ordinaria | 368 | ~89.000 |
| Especial | 219 | ~93.000 |
| Extraordinaria Especial | 86 | ~22.000 |
| Extraordinaria | 78 | ~18.000 |

Los tipos restantes (Expresión en Minoría, Asamblea Legislativa, Preparatoria, Informativa) fueron descartados por tener pocas intervenciones por sesión o ser actos formales sin contenido temático relevante.

### Metodología
1. **EDA** — distribución de sesiones e intervenciones por tipo
2. **Heatmap** — prevalencia de los 10 tópicos × tipo de sesión
3. **Bar chart comparativo** — proporción de cada tópico por tipo de sesión
4. **Kruskal-Wallis** por tópico (α = 0.05) — detecta si hay diferencias globales entre tipos
5. **Dunn's test** (corrección Bonferroni) — identifica qué pares de tipos difieren para los tópicos significativos
6. **Bar chart de tópicos discriminantes** — visualiza la dirección de las diferencias significativas

### Resultados principales
5 de los 10 tópicos mostraron diferencias significativas entre tipos de sesión:
- **Agropecuario / Ganadería** — más presente en Ordinarias; menos en Especiales y Extraordinarias Especiales
- **Jubilaciones / Previsional** — menor presencia en sesiones Especiales vs. Ordinarias
- **Salud / Discapacidad**, **Trabajo / Laboral**, **Defensa / Fuerzas militares** — diferencias marginales

### Visualizaciones generadas
- `eda_tipos_sesion.html` — intervenciones por tipo de sesión
- `heatmap_topicos_por_tipo_sesion.html` — heatmap completo
- `barras_topicos_por_tipo_sesion.html` — comparación de los 10 tópicos
- `barras_topicos_significativos_tipo_sesion.html` — solo tópicos con p < 0.05

---

## Cómo reproducir

```bash
# 1. Instalar dependencias
pip install -r requirements.txt

# 2. Preprocesar textos (genera intervenciones_limpias.parquet, tarda ~1 hora)
python scripts/pypeline_limpieza.py

# 3. Abrir notebooks en orden
#    Fase1_Modelado.ipynb       → entrena y guarda el modelo BERTopic
#    Fase2_Evolucion_Temporal.ipynb → análisis temporal (los embeddings tardan ~3 h la primera vez, luego se cachean)
```
