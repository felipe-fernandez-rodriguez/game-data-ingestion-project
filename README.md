# Plataforma de Big Data Simulada – Inteligencia de Mercado de Videojuegos Free-to-Play

Pipeline de ingeniería de datos, reproducible y automatizado, que construye un **dataset analítico
de videojuegos free-to-play** integrando tres APIs públicas. El proyecto simula, en un entorno local
y con herramientas de código abierto, las capas de una plataforma de Big Data en la nube:

```text
FreeToGame API ──▶ Ingesta ──▶ SQLite (games.db)
                                   │
                                   ▼
                     ELT con PySpark (local[*]) ──▶ games_cleaned.csv
                                                        │
              GamerPower API (JSON) ──┐                 ▼
              MMOBomb API (JSON) ─────┴──▶ Enriquecimiento por título ──▶ games_enriched.csv
                                                        │
                                   Evidencias (CSV + TXT) · pytest · GitHub Actions
```

Cada capa deja **evidencias auditables** con cifras calculadas en la ejecución (muestras en CSV y
reportes en texto plano), cuenta con **pruebas automatizadas** y se ejecuta de forma **programada**
mediante GitHub Actions.

> **Contexto del caso.** La plataforma se diseñó para el escenario de *LudoMetrics Analytics*, una
> firma **ficticia** de inteligencia de mercado del segmento free-to-play que necesita un dataset con
> granularidad *1 juego = 1 registro* que combine el catálogo base con la actividad promocional
> (giveaways) y con un catálogo de contraste. La organización es un caso de estudio académico; las
> APIs, el código y los artefactos son reales.

---

## Tabla de contenido

1. [Fuentes de datos](#fuentes-de-datos)
2. [Arquitectura](#arquitectura)
3. [Estructura del repositorio](#estructura-del-repositorio)
4. [Requisitos e instalación](#requisitos-e-instalación)
5. [Ejecución del pipeline](#ejecución-del-pipeline)
6. [Capa 1 – Ingesta](#capa-1--ingesta)
7. [Capa 2 – Preprocesamiento y limpieza (ELT)](#capa-2--preprocesamiento-y-limpieza-elt)
8. [Capa 3 – Enriquecimiento](#capa-3--enriquecimiento)
9. [Modelo de datos](#modelo-de-datos)
10. [Pruebas](#pruebas)
11. [Automatización con GitHub Actions](#automatización-con-github-actions)
12. [Evidencias generadas](#evidencias-generadas)
13. [Documentación de arquitectura y modelo de datos](#documentación-de-arquitectura-y-modelo-de-datos)
14. [Trazabilidad end-to-end](#trazabilidad-end-to-end)
15. [Correspondencia con las actividades académicas](#correspondencia-con-las-actividades-académicas)
16. [Solución de problemas](#solución-de-problemas)
17. [Limitaciones](#limitaciones)
18. [Atribuciones](#atribuciones)

---

## Fuentes de datos

| Fuente | Endpoint | Formato | Autenticación | Rol en la plataforma | Archivo RAW |
|---|---|---|---|---|---|
| **FreeToGame** | `https://www.freetogame.com/api/games` | JSON | No requiere | Fuente principal: catálogo de juegos | `data/raw/games.json` |
| **GamerPower** | `https://www.gamerpower.com/api/giveaways` | JSON | No requiere | Enriquecimiento: giveaways activos (valor, tipo, estado, fechas) | `data/raw/gamerpower/giveaways.json` |
| **MMOBomb** | `https://www.mmobomb.com/api1/games` | JSON | No requiere | Enriquecimiento y contraste: catálogo alternativo | `data/raw/mmobomb/games.json` |

Documentación oficial: [FreeToGame](https://www.freetogame.com/api-doc) ·
[GamerPower](https://www.gamerpower.com/api-read) · [MMOBomb](https://www.mmobomb.com/api).

Los archivos RAW se guardan **exactamente como los entrega cada API** (mismos campos, mismos valores,
misma estructura). Toda transformación ocurre después, en memoria, sobre copias de proceso.

---

## Arquitectura

La plataforma sigue una arquitectura por capas con contratos de datos explícitos entre ellas:

| Capa | Entrada | Proceso | Salida (contrato) |
|---|---|---|---|
| **Ingesta** | FreeToGame API | Extracción → validación → persistencia idempotente en SQLite | `data/database/games.db` |
| **Preprocesamiento (ELT)** | `games.db` | Extract → Load en Spark → perfilamiento → limpieza → validación | `data/processed/games_cleaned.csv` |
| **Enriquecimiento** | `games_cleaned.csv` + GamerPower + MMOBomb | Homologación por título → LEFT JOIN → control de cardinalidad → validación | `data/enriched/games_enriched.csv` |

### Simulación del entorno cloud

No se utilizan servicios de nube reales. Cada componente local representa una capa que en la nube
cubriría un servicio administrado:

| Capa conceptual | Componente local (implementado) | Equivalente en nube (referencia) |
|---|---|---|
| Zona RAW / data lake | `data/raw/*.json` | S3 · Azure Blob · GCS |
| Base analítica | SQLite (`games.db`) | RDS/PostgreSQL · Cloud SQL |
| Motor de procesamiento | PySpark `local[*]` + Apache Arrow | Databricks · EMR · Dataproc |
| Zona procesada / enriquecida | CSV en `data/processed/`, `data/enriched/` | Parquet/Delta en almacén de objetos |
| Orquestación / CI-CD | GitHub Actions (`push`, manual, `cron`) | Airflow · Data Factory + CI/CD |

`local[*]` ejecuta Spark en una sola máquina usando tantos hilos como núcleos disponibles, con el
mismo motor (Catalyst), la misma API de DataFrames y la misma evaluación perezosa que un clúster
real. La simulación es fiel a la lógica distribuida, no a su escala.

### Principios de diseño

- **Configuración centralizada** en `src/config.py` (rutas con `pathlib`, URLs, constantes,
  clasificación de campos, linaje).
- **Reutilización entre capas**: el acceso a SQLite vive solo en `src/database.py`; la generación de
  muestras comparte `write_csv_sample`; la sesión de Spark y el puente Pandas→Spark viven en
  `src/spark_session.py`.
- **Ninguna cifra inventada**: todos los reportes se construyen con métricas calculadas durante la
  ejecución.
- **Fallo explícito**: cada orquestador devuelve `0` (éxito), `1` (error) o `2` (ejecutado con
  observaciones), y GitHub Actions verifica la existencia de cada evidencia.

---

## Estructura del repositorio

```text
game-data-ingestion-project/
│
├── .github/workflows/
│   ├── ingestion.yml            # Capa 1: ingesta (pytest + main.py + artifacts)
│   └── bigdata.yml              # Capas 2 y 3: ELT + enriquecimiento (pytest + preprocessing.py + enrichment.py)
│
├── data/
│   ├── raw/
│   │   ├── games.json                  # RAW FreeToGame
│   │   ├── gamerpower/giveaways.json   # RAW GamerPower
│   │   └── mmobomb/games.json          # RAW MMOBomb
│   ├── database/games.db               # SQLite (tabla games)
│   ├── processed/games_cleaned.csv     # Dataset limpio (contrato de la capa 2)
│   ├── mappings/title_mapping.csv      # Equivalencias manuales de título (vacío por defecto)
│   └── enriched/games_enriched.csv     # Dataset analítico final
│
├── docs/
│   ├── Arquitectura_Modelo_Datos.pdf   # Documentación de arquitectura y modelo de datos
│   └── diagrams/                       # Diagramas editables (.drawio) y su versión PNG
│       ├── arquitectura_general.drawio / .png
│       ├── flujo_ingesta.drawio / .png
│       ├── flujo_elt.drawio / .png
│       ├── flujo_enriquecimiento.drawio / .png
│       └── modelo_datos.drawio / .png
│
├── output/
│   ├── games_sample.csv         # Muestra de la ingesta
│   ├── audit_report.txt         # Auditoría API vs SQLite
│   ├── cleaned_games_sample.csv # Muestra del dataset limpio
│   ├── cleaning_report.txt      # Auditoría de limpieza
│   ├── enriched_games_sample.csv# Muestra del dataset enriquecido
│   └── enrichment_report.txt    # Auditoría de enriquecimiento
│
├── src/
│   ├── __init__.py
│   ├── config.py                # Rutas, URLs, constantes y linaje (compartido por las tres capas)
│   ├── extract.py               # Capa 1: petición HTTP, validación y guardado del RAW de FreeToGame
│   ├── database.py              # Capa 1: esquema SQLite e inserción idempotente (reutilizado por la capa 2)
│   ├── audit.py                 # Capa 1: comparación API vs SQLite y reporte
│   ├── generate_sample.py       # Muestras CSV reproducibles (compartido)
│   ├── main.py                  # Orquestador de la capa 1
│   ├── spark_session.py         # SparkSession local[*] y puente Pandas→Spark (compartido)
│   ├── profiling.py             # Capa 2: perfilamiento con PySpark
│   ├── cleaning.py              # Capa 2: deduplicación, texto, nulos, tipos, release_year, outliers
│   ├── validation.py            # Capa 2: validaciones de origen, esquema, resultado y Quality Score
│   ├── preprocessing.py         # Orquestador de la capa 2 (ELT)
│   ├── gamerpower_client.py     # Capa 3: cliente GamerPower
│   ├── mmobomb_client.py        # Capa 3: cliente MMOBomb
│   ├── title_matching.py        # Capa 3: title_match, mapping manual, deduplicación por título
│   ├── enrichment_audit.py      # Capa 3: métricas de cobertura/cardinalidad y reporte
│   └── enrichment.py            # Orquestador de la capa 3
│
├── tests/
│   ├── test_ingestion.py        # 12 pruebas (mocks HTTP)
│   ├── test_preprocessing.py    # 15 pruebas (Spark local)
│   └── test_enrichment.py       # 13 pruebas (mocks HTTP + Spark local)
│
├── conftest.py                  # Agrega la raíz del proyecto a sys.path para pytest
├── requirements.txt
├── README.md
└── .gitignore
```

---

## Requisitos e instalación

- Python 3.10 o superior (en CI se fija **3.11**).
- Git.
- **Java 11 o 17 (JDK)** para las capas 2 y 3 (PySpark corre sobre la JVM). No es necesario para la
  capa 1.

```bash
git clone https://github.com/felipe-fernandez-rodriguez/game-data-ingestion-project.git
cd game-data-ingestion-project

# Windows
python -m venv venv
venv\Scripts\activate

# Linux / macOS
python -m venv venv
source venv/bin/activate

pip install -r requirements.txt
```

Dependencias (`requirements.txt`): `requests`, `pandas`, `pytest`, `pyspark>=3.5,<4.0`, `pyarrow`,
`setuptools`. `sqlite3` y `json` son parte de la biblioteca estándar.

---

## Ejecución del pipeline

Desde la raíz del proyecto, con el entorno virtual activado y **en este orden** (cada capa consume la
salida de la anterior):

```bash
python src/main.py            # Capa 1: FreeToGame → games.db
python src/preprocessing.py   # Capa 2: games.db → games_cleaned.csv
python src/enrichment.py      # Capa 3: games_cleaned.csv + GamerPower + MMOBomb → games_enriched.csv
```

Códigos de salida (en las tres capas): `0` = ejecución y validación exitosas · `1` = error que
detiene el pipeline · `2` = pipeline ejecutado pero con observaciones de calidad (ver el reporte
correspondiente en `output/`).

---

## Capa 1 – Ingesta

**Módulos:** `src/main.py`, `src/extract.py`, `src/database.py`, `src/generate_sample.py`, `src/audit.py`.

1. Se consulta `https://www.freetogame.com/api/games` con `requests.get()` y un timeout de 15 s.
2. Se valida en cadena: código HTTP 200 → JSON válido → lista no vacía → todos los elementos son
   objetos. Cualquier incumplimiento lanza `ExtractionError` y termina con código `1`, sin escribir
   nada. Además se comparan las claves del primer registro con los 11 campos esperados
   (`config.EXPECTED_FIELDS`) y se registran advertencias si la API agregó o retiró campos.
3. La respuesta se guarda sin transformar en `data/raw/games.json`.
4. Se crea/abre `data/database/games.db`, se ejecuta `CREATE TABLE IF NOT EXISTS games` y se
   insertan los registros en una única transacción con **`INSERT OR REPLACE`**: la operación es
   idempotente (repetirla no duplica filas) y refleja siempre el estado más reciente de la API.
5. Con Pandas (`read_sql_query`) se genera `output/games_sample.csv`: hasta 20 filas,
   `random_state=42`, ordenadas por `id`.
6. `output/audit_report.txt` compara API vs SQLite por `id`: conteos, IDs faltantes/adicionales y
   diferencias campo a campo (normalizando `None` y espacios). Resultado: `VALIDACIÓN EXITOSA` o
   `VALIDACIÓN CON ERRORES`.

---

## Capa 2 – Preprocesamiento y limpieza (ELT)

**Módulos:** `src/preprocessing.py`, `src/spark_session.py`, `src/profiling.py`, `src/cleaning.py`, `src/validation.py`.

Esta capa **no vuelve a consultar la API**: su única fuente es `games.db`. Es **ELT** porque los datos
se extraen y se cargan tal cual en Spark, y solo después se transforman dentro del motor.

| Etapa | Qué ocurre |
|---|---|
| Pre-flight | Se verifica que la base exista, que la tabla `games` exista y tenga registros (si no, código `1`). |
| **Extract** | `extract_from_sqlite()` reutiliza `database.get_connection` y `database.fetch_all_games` (11 columnas de datos; excluye `ingested_at`). |
| **Load** | Lista de registros → Pandas → Spark vía `pandas_to_spark()` (fuerza `dtype=object` para preservar `None` como `NULL`; conversión con Apache Arrow). Se valida el esquema contra los campos esperados. |
| Perfilamiento inicial | Registros, columnas, nulos y vacíos por columna, duplicados completos y por `id`, rango de `id`, validez de `release_date`, valores distintos en `genre`, `platform`, `publisher`, `developer`. |
| **Transform** | 1) Deduplicación completa y por `id`. 2) Normalización de texto (trim, colapso de espacios, vacíos → `NULL`) en todas las columnas de texto, incluidas las categóricas. 3) Nulos: filas sin `id`/`title` se eliminan; `genre`/`platform`/`publisher`/`developer` → `"Unknown"`; `thumbnail`/`short_description`/`game_url`/`freetogame_profile_url` → `"Not Available"`. 4) Tipos: `id` → `IntegerType`, texto → `StringType`, `release_date` → `DateType` (`yyyy-MM-dd`; inválidos → `NULL`, contabilizados). 5) Columna derivada `release_year`. 6) Outliers por IQR sobre numéricas continuas: `id` es un identificador secuencial, por lo que se documenta explícitamente que no aplica. |
| Validación final | Sin `id` duplicados, sin nulos críticos, tipos correctos; **Data Quality Score** (0-100 %) = promedio de `sin_ids_duplicados`, `campos_criticos_completos`, `tipos_correctos`, `reduccion_de_problemas`. |
| Salidas | `data/processed/games_cleaned.csv` (12 columnas: las 11 originales + `release_year`), `output/cleaned_games_sample.csv`, `output/cleaning_report.txt`. Spark se cierra siempre en `finally`. |

Configuración de Spark (`src/spark_session.py`): `local[*]`, `spark.sql.shuffle.partitions=4`,
Arrow habilitado con `fallback` deshabilitado, nivel de log `WARN`, e importación de `setuptools`
antes de Spark (workaround oficial de Apache Spark para Python 3.12+, ver
[Solución de problemas](#solución-de-problemas)).

---

## Capa 3 – Enriquecimiento

**Módulos:** `src/enrichment.py`, `src/gamerpower_client.py`, `src/mmobomb_client.py`, `src/title_matching.py`, `src/enrichment_audit.py`.

### Descarga y conservación del RAW

Ambos clientes siguen el patrón de la capa 1: petición con timeout, validación de HTTP y JSON, e
interpretación tolerante de la estructura (lista plana u objeto con clave contenedora `giveaways` /
`games`, que se verifica en tiempo de ejecución). El payload se guarda **tal cual** en
`data/raw/gamerpower/giveaways.json` y `data/raw/mmobomb/games.json`: sin renombrar, sin deduplicar,
sin normalizar.

### Por qué no se cruzan los `id`

FreeToGame, GamerPower y MMOBomb tienen sistemas de identificación independientes. El `id` de
FreeToGame se conserva como identificador principal; `gamerpower_id` y `mmobomb_id` se conservan
solo como columnas de procedencia. La clave lógica de integración es el **título**, normalizado en la
columna auxiliar `title_match`.

### Homologación (`title_match`)

`normalize_title()` (Pandas, sin UDFs de Spark) aplica reglas determinísticas y **nunca modifica
`title`**:

1. Elimina el sufijo final `"Giveaway"`.
2. Elimina anotaciones de distribuidor/plataforma entre paréntesis pertenecientes a una **lista
   cerrada** (`(Steam)`, `(Epic Games)`, `(GOG)`, `(Indiegala)`, `(Origin)`, `(Ubisoft Connect)`,
   `(Humble Bundle)`, `(DRM-free)`, `(itch.io)`, `(Battle.net)`, `(PlayStation)`, `(Xbox)`,
   `(Switch)`, entre otras).
3. Convierte a minúsculas; recorta y colapsa espacios.

```text
title:        "Sausage Hunter (Indiegala) Giveaway"
title_match:  "sausage hunter"
```

Niveles de coincidencia:

1. **Exacta** sobre `title_match`.
2. **Mapping manual** en `data/mappings/title_mapping.csv`
   (`freetogame_title,gamerpower_title,mmobomb_title,match_method`). Se entrega **vacío**; solo se
   agregan equivalencias verificadas a mano, que `apply_title_mapping()` usa como anulación de
   `title_match`.
3. **Fuzzy matching: deliberadamente no implementado.** Lo que no coincide exacto ni está mapeado
   queda como no coincidencia (columnas de esa fuente en `NULL`), nunca como coincidencia forzada.

### Deduplicación previa y LEFT JOIN

- `resolve_duplicate_titles()` deja cada fuente con un registro por `title_match`: ordena por la
  columna de prioridad descendente (`published_date` en GamerPower, `release_date` en MMOBomb) y
  desempata por `id` ascendente. Los títulos vacíos tras normalizar no participan del cruce.
- Las columnas seleccionadas se renombran con prefijos `gamerpower_` y `mmobomb_`, para que nunca
  sobrescriban columnas del dataset base.
- En Spark, `left_join_on_title()` hace **LEFT JOIN** base ⟕ GamerPower ⟕ MMOBomb. Como el lado
  derecho tiene `title_match` único, la unión **no puede multiplicar filas**: la granularidad
  *1 juego = 1 registro* está garantizada por construcción y, además, se verifica. La cobertura se
  mide renombrando temporalmente la clave de la fuente y contando filas base con clave no nula.
- Se agregan `genre_match`, `platform_match`, `publisher_match`, `developer_match` comparando
  FreeToGame vs MMOBomb (sin distinguir mayúsculas/espacios). Sin coincidencia MMOBomb quedan en
  **`NULL`, no `False`**: "no se pudo comparar" ≠ "se comparó y difiere".

### Control de cardinalidad y validación final

Se registra el número de registros en cada etapa (base → tras GamerPower → tras MMOBomb → final) y
se valida que: el dataset enriquecido tenga las mismas filas que `games_cleaned.csv`; no existan
`id` duplicados; y las columnas originales no hayan cambiado de valor (`exceptAll` contra la base).

**Salidas:** `data/enriched/games_enriched.csv` (33 columnas), `output/enriched_games_sample.csv`,
`output/enrichment_report.txt` (fuentes, cobertura por fuente, homologación, duplicados,
cardinalidad, validación, resultado y linaje de las columnas nuevas).

---

## Modelo de datos

### Modelo físico (SQLite)

La base `data/database/games.db` contiene **una única tabla**, `games`, definida en
`src/database.py`. No hay otras tablas, claves foráneas ni índices adicionales.

| Campo | Tipo | PK | Nulo | Descripción |
|---|---|---|---|---|
| `id` | INTEGER | Sí | No | Identificador único asignado por FreeToGame |
| `title` | TEXT | No | No (`NOT NULL`) | Título del juego |
| `thumbnail` | TEXT | No | Sí | URL de la miniatura |
| `short_description` | TEXT | No | Sí | Descripción breve |
| `game_url` | TEXT | No | Sí | URL del juego |
| `genre` | TEXT | No | Sí | Género |
| `platform` | TEXT | No | Sí | Plataforma |
| `publisher` | TEXT | No | Sí | Distribuidor |
| `developer` | TEXT | No | Sí | Desarrollador |
| `release_date` | TEXT | No | Sí | Fecha de lanzamiento, ISO `YYYY-MM-DD` |
| `freetogame_profile_url` | TEXT | No | Sí | URL del perfil en FreeToGame |
| `ingested_at` | TEXT | No | No (`DEFAULT datetime('now')`) | Marca interna de inserción (no proviene de la API) |

### Conjuntos derivados (CSV) y modelo lógico

Los archivos CSV **no son tablas de base de datos**: son materializaciones del procesamiento.

| Conjunto | Columnas | Clave | Relación con el anterior |
|---|---|---|---|
| `games_cleaned.csv` | 12: las 11 columnas de datos de `games` + `release_year` | `id` | 1:1 por `id` con `games` (salvo filas eliminadas por duplicidad o nulos críticos) |
| `games_enriched.csv` | 33: 12 base + `title_match` + 9 `gamerpower_*` + 7 `mmobomb_*` + 4 `*_match` | `id` (principal), `title_match` (auxiliar) | LEFT JOIN 1 : 0..1 con cada fuente por `title_match` |

En CSV los tipos se serializan como texto; al consumir el dataset conviene volver a tipificar
`id`, `release_date`, `release_year` y las columnas `*_match`.

### Linaje de columnas del dataset enriquecido

| Columna(s) | Origen |
|---|---|
| `id`, `title`, `thumbnail`, `short_description`, `game_url`, `genre`, `platform`, `publisher`, `developer`, `release_date`, `freetogame_profile_url` | FreeToGame API (ingesta), normalizados/imputados en la capa ELT |
| `release_year` | Derivada de `release_date` (capa ELT) |
| `title_match` | Derivada de `title` (capa de enriquecimiento) |
| `gamerpower_id`, `gamerpower_worth`, `gamerpower_type`, `gamerpower_platforms`, `gamerpower_status`, `gamerpower_users`, `gamerpower_published_date`, `gamerpower_end_date`, `gamerpower_url` | GamerPower API |
| `mmobomb_id`, `mmobomb_genre`, `mmobomb_platform`, `mmobomb_publisher`, `mmobomb_developer`, `mmobomb_release_date`, `mmobomb_profile_url` | MMOBomb API |
| `genre_match`, `platform_match`, `publisher_match`, `developer_match` | Comparación FreeToGame vs MMOBomb |

Esta misma tabla la genera el pipeline al final de `output/enrichment_report.txt` a partir de
`config.DATA_LINEAGE_EA3`. El diagrama del modelo (físico + lógico) está en
`docs/diagrams/modelo_datos.drawio` y en el documento de arquitectura.

---

## Pruebas

```bash
pytest -v
```

| Módulo | Pruebas | Cobertura |
|---|---|---|
| `tests/test_ingestion.py` | 12 | Estructura válida/inválida de la API, HTTP incorrecto, guardado del RAW, creación de base y tabla, inserción e idempotencia, muestra CSV, auditoría, detección de faltantes y diferencias |
| `tests/test_preprocessing.py` | 15 | Extracción desde SQLite y sus errores, creación del DataFrame, duplicados, nulos, tipos, `release_year`, outliers, esquema, pipeline completo, muestra y reporte |
| `tests/test_enrichment.py` | 13 | Descarga y RAW de ambas APIs, estructuras inválidas, carga del dataset base, `title_match`, deduplicación por recencia, LEFT JOIN con cobertura, unión de dos fuentes sin duplicados y con `*_match`, CSV, muestra y reporte |

Todas las llamadas HTTP se simulan con `unittest.mock` (la suite no depende de la red). Las pruebas
de Spark usan DataFrames pequeños creados para cada caso y comparten una `SparkSession` por módulo.
`conftest.py` agrega la raíz del proyecto a `sys.path`.

---

## Automatización con GitHub Actions

Dos flujos independientes, ambos disparados en cada `push`, manualmente (`workflow_dispatch`) y con
ejecución programada diaria.

### `.github/workflows/ingestion.yml` – Capa 1

`cron: "0 6 * * *"` (06:00 UTC). Pasos: checkout → Python 3.11 (caché pip) → `pip install -r
requirements.txt` → `pytest -v` → `python src/main.py` → verificación (`test -f`) de `games.db`,
`games_sample.csv`, `audit_report.txt` → impresión de las evidencias en el log → artifact
**`evidencias-ingestion-ea1`** (`games.json`, `games.db`, `games_sample.csv`, `audit_report.txt`;
retención 30 días).

### `.github/workflows/bigdata.yml` – Capas 2 y 3

`cron: "30 6 * * *"` (06:30 UTC, después de la ingesta). Depende de que `data/database/games.db`
exista en el repositorio; si no existe, falla explícitamente. Pasos: checkout → Python 3.11 → Java 17
(Temurin) → dependencias → verificación de `games.db` → `pytest -v` → `python src/preprocessing.py`
→ verificación e impresión de evidencias ELT → artifact **`evidencias-preprocesamiento-ea2`** →
`python src/enrichment.py` → verificación de los RAW y de las evidencias de enriquecimiento →
artifacts **`gamerpower_raw`**, **`mmobomb_raw`**, **`enriched_dataset`**, **`enrichment_evidence`**.

Los pasos de Spark fijan `PYSPARK_PYTHON=python` (mismo intérprete en driver y executors) y
`SPARK_LOCAL_IP=127.0.0.1` (evita resolver el hostname del runner por red).

### Archivos versionados vs artifacts

- **Archivos versionados** (`data/`, `output/`): evidencia "viva" del último estado confirmado en el
  repositorio. A propósito, `.gitignore` **no** los excluye.
- **Artifacts**: evidencia oficial de *cada ejecución* del workflow, descargable desde la pestaña
  *Actions*, útil para comparar corridas (p. ej., cambios en el catálogo o en la cobertura de
  giveaways a lo largo del tiempo).

---

## Evidencias generadas

| Archivo | Capa | Contenido |
|---|---|---|
| `data/raw/games.json` | Ingesta | Respuesta literal de FreeToGame |
| `data/database/games.db` | Ingesta | SQLite con la tabla `games` poblada |
| `output/games_sample.csv` | Ingesta | Muestra reproducible (≤ 20 filas, `random_state=42`) |
| `output/audit_report.txt` | Ingesta | Comparación API vs SQLite por `id` |
| `data/processed/games_cleaned.csv` | ELT | Dataset limpio completo (12 columnas) |
| `output/cleaned_games_sample.csv` | ELT | Muestra reproducible del dataset limpio |
| `output/cleaning_report.txt` | ELT | Perfilamiento inicial/final, operaciones, antes/después, validación, Quality Score |
| `data/raw/gamerpower/giveaways.json` | Enriquecimiento | Respuesta literal de GamerPower |
| `data/raw/mmobomb/games.json` | Enriquecimiento | Respuesta literal de MMOBomb |
| `data/enriched/games_enriched.csv` | Enriquecimiento | Dataset analítico final (33 columnas) |
| `output/enriched_games_sample.csv` | Enriquecimiento | Muestra reproducible del dataset enriquecido |
| `output/enrichment_report.txt` | Enriquecimiento | Cobertura, homologación, duplicados, cardinalidad, validación, linaje |

### Cómo verificar una ejecución

1. `audit_report.txt` debe terminar en `VALIDACIÓN EXITOSA` sin IDs faltantes/adicionales ni
   diferencias.
2. `cleaning_report.txt` debe indicar `VALIDACIÓN EXITOSA`, 0 IDs duplicados y 0 nulos críticos.
3. `enrichment_report.txt` debe indicar `ENRIQUECIMIENTO EXITOSO`, registros base = registros
   finales, 0 IDs duplicados y 0 coincidencias ambiguas.
4. En la pestaña *Actions*, ambos workflows en verde (✔) y sus artifacts descargables.

---

## Documentación de arquitectura y modelo de datos

**Objetivo.** Consolidar en un único documento técnico la arquitectura completa de la plataforma
(ingesta → ELT → enriquecimiento), el flujo de datos por capa, la automatización, el modelo de datos
físico y lógico, el linaje, la justificación de herramientas, la calidad, las limitaciones y la ruta
de evolución hacia una infraestructura en la nube.

**Documento:** [`docs/Arquitectura_Modelo_Datos.pdf`](docs/Arquitectura_Modelo_Datos.pdf)

**Contenido del documento:**

| Sección | Qué documenta |
|---|---|
| Contexto del proyecto | Caso de estudio simulado (*LudoMetrics Analytics*), problema, objetivo analítico, alcance y evolución del pipeline |
| Arquitectura | Visión global (Figura 1), componentes, simulación del entorno cloud |
| Flujo de datos | Ingesta (Figura 2), ELT (Figura 3), enriquecimiento (Figura 4), flujo end-to-end |
| Automatización | GitHub, `ingestion.yml`, `bigdata.yml`, pruebas, evidencias y artifacts |
| Modelo de datos | Esquema real de `games`, diferencia modelo físico / dataset procesado / dataset enriquecido, diagrama (Figura 5), modelo lógico, claves, relaciones, linaje, justificación |
| Herramientas | Justificación de Python, Requests, SQLite, Pandas, PySpark, Arrow, pytest, GitHub, GitHub Actions |
| Calidad, seguridad, evidencias | Controles por capa, configuración, artefactos |
| Limitaciones y recomendaciones | Estado actual vs arquitectura futura propuesta (no implementada) |

**Diagramas editables** (`docs/diagrams/*.drawio`, abrir con [diagrams.net](https://app.diagrams.net/)):
`arquitectura_general`, `flujo_ingesta`, `flujo_elt`, `flujo_enriquecimiento`, `modelo_datos`.

**Relación con el código y la automatización.** El documento describe únicamente lo implementado en
`src/`, `tests/` y `.github/workflows/`; los nombres de tablas, columnas, archivos y pasos de los
workflows coinciden con el repositorio. Las tecnologías de nube se presentan solo como referencia de
migración, no como parte de la implementación.

---

## Trazabilidad end-to-end

| Etapa | Entrada | Proceso | Salida |
|---|---|---|---|
| Ingesta | FreeToGame API | `src/extract.py` (extracción y validación) | `data/raw/games.json` |
| Ingesta | JSON validado | `src/database.py` (persistencia idempotente) | `data/database/games.db` |
| Ingesta | SQLite | `src/generate_sample.py` | `output/games_sample.csv` |
| Ingesta | API + SQLite | `src/audit.py` | `output/audit_report.txt` |
| ELT | `games.db` | `src/preprocessing.py` (Extract/Load) | Spark DataFrame |
| ELT | Spark DataFrame | `src/profiling.py` | Métricas de calidad |
| ELT | Spark DataFrame | `src/cleaning.py` + `src/validation.py` | `data/processed/games_cleaned.csv` |
| ELT | Dataset limpio | `src/preprocessing.py` (reporte) | `output/cleaning_report.txt` |
| Enriquecimiento | GamerPower + MMOBomb APIs | `src/gamerpower_client.py`, `src/mmobomb_client.py` | `giveaways.json`, `games.json` |
| Enriquecimiento | `games_cleaned.csv` + RAW | `src/title_matching.py` | `title_match` por fuente |
| Enriquecimiento | Base + fuentes homologadas | `src/enrichment.py` (LEFT JOIN, validación) | `data/enriched/games_enriched.csv` |
| Enriquecimiento | Dataset enriquecido | `src/enrichment_audit.py` | `output/enrichment_report.txt` |
| Documentación | Repositorio completo | `docs/` | Documento de arquitectura y diagramas |

`data/enriched/games_enriched.csv` es la **salida final** de la plataforma y el punto de entrada
para cualquier análisis o visualización posterior sobre estos datos.

---

## Correspondencia con las actividades académicas

El proyecto se desarrolló de forma incremental en el marco del Proyecto Integrador de Big Data. Para
facilitar la evaluación, esta tabla relaciona cada capa con la actividad en la que se construyó; los
nombres de algunos artifacts de CI conservan esa nomenclatura.

| Actividad | Capa del proyecto | Orquestador | Workflow | Evidencias |
|---|---|---|---|---|
| EA1 – Ingestión de Datos desde un API | Ingesta | `src/main.py` | `ingestion.yml` | `games.json`, `games.db`, `games_sample.csv`, `audit_report.txt` |
| EA2 – Preprocesamiento y Limpieza (Big Data en la nube, simulado) | ELT | `src/preprocessing.py` | `bigdata.yml` | `games_cleaned.csv`, `cleaned_games_sample.csv`, `cleaning_report.txt` |
| EA3 – Enriquecimiento de Datos | Enriquecimiento | `src/enrichment.py` | `bigdata.yml` | RAW de GamerPower y MMOBomb, `games_enriched.csv`, `enriched_games_sample.csv`, `enrichment_report.txt` |
| EA4 – Documentación de la Arquitectura y Modelo de Datos | Documentación | — | — | `docs/Arquitectura_Modelo_Datos.pdf`, `docs/diagrams/*.drawio` |

---

## Solución de problemas

### `ModuleNotFoundError: No module named 'distutils'`

Aparece típicamente en Windows con Python 3.12+: Python 3.12 eliminó `distutils` de la librería
estándar y PySpark 3.5.x aún lo usa internamente (p. ej., al llamar `spark.createDataFrame()`). El
proyecto lo resuelve en dos niveles: `requirements.txt` incluye `setuptools` (provee una copia de
`distutils`) y `src/spark_session.py` importa `setuptools` antes de Spark, workaround oficial
documentado en [SPARK-47613](https://issues.apache.org/jira/browse/SPARK-47613). Si el entorno
virtual es anterior a este cambio:

```bash
pip install --upgrade -r requirements.txt
```

La solución definitiva sería `pyspark>=4.0`, donde Spark eliminó `distutils`
([SPARK-44120](https://issues.apache.org/jira/browse/SPARK-44120)); por eso CI fija Python 3.11.

### `PicklingError: ... RecursionError: Stack overflow` en `createDataFrame`

Sin Apache Arrow, la conversión Pandas→Spark usa una ruta basada en RDD + `cloudpickle` que puede
agotar la pila en Windows con Python 3.12+. `src/spark_session.py` habilita
`spark.sql.execution.arrow.pyspark.enabled=true` (con `fallback` deshabilitado para que cualquier
problema sea explícito), lo que requiere `pyarrow` (ya incluido). Reinstalar dependencias si el
entorno es anterior.

### `bigdata.yml` falla con "games.db no existe"

La capa ELT depende de que la ingesta haya confirmado `data/database/games.db` en el repositorio.
Ejecutar primero `python src/main.py` (o el workflow `ingestion.yml`) y confirmar el archivo.

---

## Limitaciones

- SQLite es una base local de un solo archivo; no equivale a una base analítica administrada.
- PySpark se ejecuta en modo local en una sola máquina: no se evalúa escalabilidad ni tolerancia a
  fallos.
- Los datasets se materializan en CSV, formato sin esquema ni compresión que pierde los tipos.
- Las APIs externas pueden cambiar; el pipeline advierte y falla de forma controlada, pero no se
  adapta automáticamente.
- La homologación por título exacto es conservadora: puede dejar no coincidencias que solo se
  resuelven con equivalencias manuales.
- Las columnas `gamerpower_*` son una fotografía de las promociones vigentes al momento de la
  consulta, no un histórico.
- Las evidencias de datos se versionan en Git, adecuado para el volumen actual pero no escalable.
- GitHub Actions orquesta por horarios escalonados; no reemplaza un orquestador de datos con
  dependencias explícitas, reintentos y monitoreo.

Las recomendaciones de evolución (Parquet, almacén de objetos, Spark administrado, orquestador,
catálogo, modelo dimensional, gestión de secretos) se detallan en el documento de arquitectura y se
presentan como propuestas, **no** como parte de la implementación actual.

---

## Atribuciones

- Datos del catálogo principal: [FreeToGame.com](https://www.freetogame.com/), según su
  [documentación de uso](https://www.freetogame.com/api-doc).
- Datos de giveaways: [GamerPower.com](https://www.gamerpower.com/) (atribución obligatoria según su
  política de uso; API gratuita sin key).
- Datos del catálogo de contraste: [MMOBomb.com](https://www.mmobomb.com/).
