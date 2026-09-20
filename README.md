# Proyecto Integrador de Big Data - FreeToGame

Este repositorio contiene el **Proyecto Integrador de Big Data**, desarrollado de forma
incremental a través de actividades sucesivas que comparten los mismos datos, la misma
estructura de proyecto y el mismo repositorio:

- **Actividad 1 (EA1):** Ingestión de datos desde un API REST hacia SQLite.
- **Actividad 2 (EA2):** Preprocesamiento, limpieza y transformación con PySpark (ELT).
- **Actividad 3 (EA3):** Enriquecimiento de datos integrando las APIs de GamerPower y MMOBomb (JSON).

Cada actividad se documenta en su propia sección de este README, sin reemplazar lo ya construido
en las actividades anteriores. Ver también la sección [Trazabilidad del Proyecto](#trazabilidad-del-proyecto).

---

# Actividad 1 - Ingestión de Datos desde un API

## Introducción

Esta primera etapa del proyecto integrador implementa la ingestión de datos desde una API REST
pública, su almacenamiento estructurado en una base de datos SQLite, la generación de evidencias
reproducibles (muestra en CSV y reporte de auditoría) y la automatización completa del proceso
mediante GitHub Actions.

El objetivo académico es demostrar el dominio de un flujo ETL básico pero completo:

```
API REST → extracción JSON → validación → transformación → SQLite → CSV de muestra → auditoría TXT → GitHub Actions
```

## Fuente de datos

Se utiliza la **FreeToGame API**, una API pública, gratuita y sin autenticación que expone un
catálogo de videojuegos *free-to-play*.

- **Endpoint utilizado:** `https://www.freetogame.com/api/games`
- **Documentación oficial:** `https://www.freetogame.com/api-doc`
- **Formato:** JSON
- **Autenticación:** no requerida

**Atribución:** los datos utilizados en este proyecto provienen de [FreeToGame.com](https://www.freetogame.com/),
tal como lo solicita su documentación oficial de uso.

Cada registro de la API contiene, entre otros, los siguientes campos: `id`, `title`, `thumbnail`,
`short_description`, `game_url`, `genre`, `platform`, `publisher`, `developer`, `release_date` y
`freetogame_profile_url`. El proyecto **no asume campos adicionales** ni inventa información que
la API no entregue.

## Arquitectura

```text
FreeToGame API
      ↓
Python / Requests
      ↓
JSON
      ↓
Pandas / Transformación
      ↓
SQLite
      ↓
 ┌────┴─────────────┐
 ↓                  ↓
CSV              Auditoría TXT
      ↓
GitHub Actions
```

## Estructura del proyecto

```text
game-data-ingestion-project-main/
│
├── .github/
│   └── workflows/
│       ├── ingestion.yml        # Automatización de EA1 con GitHub Actions
│       └── bigdata.yml          # (EA2+EA3) Automatización de EA2 y EA3 con GitHub Actions
│
├── data/
│   ├── raw/
│   │   ├── games.json           # Respuesta cruda de la API de FreeToGame (evidencia de EA1)
│   │   ├── gamerpower/
│   │   │   └── giveaways.json   # (EA3) Respuesta RAW de GamerPower, sin transformar
│   │   └── mmobomb/
│   │       └── games.json       # (EA3) Respuesta RAW de MMOBomb, sin transformar
│   ├── database/
│   │   └── games.db             # Base de datos SQLite (generada por EA1, consumida por EA2)
│   ├── processed/
│   │   └── games_cleaned.csv    # (EA2) Dataset limpio, entrada base para EA3
│   ├── mappings/
│   │   └── title_mapping.csv    # (EA3) Mapping manual de títulos (vacío por defecto)
│   └── enriched/
│       └── games_enriched.csv   # (EA3) Dataset enriquecido, entrada base para EA4
│
├── output/
│   ├── games_sample.csv         # Muestra de datos generada con Pandas (EA1)
│   ├── audit_report.txt         # Reporte de auditoría API vs SQLite (EA1)
│   ├── cleaned_games_sample.csv # (EA2) Muestra del dataset limpio
│   ├── cleaning_report.txt      # (EA2) Auditoría de preprocesamiento y limpieza
│   ├── enriched_games_sample.csv # (EA3) Muestra del dataset enriquecido
│   └── enrichment_report.txt    # (EA3) Auditoría de enriquecimiento
│
├── src/
│   ├── __init__.py              # Marca src/ como paquete Python
│   ├── config.py                # Rutas y configuración centralizada, compartida por EA1, EA2 y EA3
│   ├── extract.py               # Extracción y validación de datos desde la API de FreeToGame (EA1)
│   ├── database.py              # Esquema SQLite e inserción idempotente (EA1, reutilizado por EA2)
│   ├── generate_sample.py       # Generación de CSV de muestra con Pandas (EA1, reutilizado por EA2/EA3)
│   ├── audit.py                 # Comparación API vs SQLite y reporte de auditoría (EA1)
│   ├── main.py                  # Orquestador del pipeline de EA1
│   ├── spark_session.py         # (EA2) Ciclo de vida de la SparkSession y puente Pandas↔Spark (reutilizado por EA3)
│   ├── profiling.py             # (EA2) Perfilamiento (Data Profiling) con PySpark
│   ├── cleaning.py              # (EA2) Deduplicación, nulos, tipos, texto, outliers
│   ├── validation.py            # (EA2) Validaciones de origen, esquema, resultado final y quality score
│   ├── preprocessing.py         # (EA2) Orquestador del pipeline ELT completo
│   ├── gamerpower_client.py     # (EA3) Cliente de la API de GamerPower
│   ├── mmobomb_client.py        # (EA3) Cliente de la API de MMOBomb
│   ├── title_matching.py        # (EA3) Normalización de títulos (title_match) y resolución de duplicados
│   ├── enrichment_audit.py      # (EA3) Métricas de cobertura/cardinalidad y reporte de auditoría
│   └── enrichment.py            # (EA3) Orquestador del pipeline de enriquecimiento
│
├── tests/
│   ├── test_ingestion.py        # Pruebas de EA1 (usa mocks para la API)
│   ├── test_preprocessing.py    # (EA2) Pruebas del pipeline ELT con PySpark
│   └── test_enrichment.py       # (EA3) Pruebas de descarga, matching, joins y auditoría (con mocks HTTP)
│
├── conftest.py                  # Configuración de sys.path para pytest (EA1, EA2 y EA3)
├── requirements.txt             # Dependencias del proyecto (EA1, EA2 y EA3)
├── README.md                    # Este documento
└── .gitignore
```

### Responsabilidad de cada archivo

| Archivo | Responsabilidad |
|---|---|
| `src/config.py` | Centraliza rutas (con `pathlib`), URLs de las tres APIs, configuración de Spark, clasificación de campos (EA2) y columnas/lineage/prefijos de EA3. |
| `src/extract.py` | Realiza la petición HTTP GET a FreeToGame, valida código de estado, JSON y estructura, y guarda el JSON crudo (EA1). |
| `src/database.py` | Define el esquema `CREATE TABLE`, abre/crea la base SQLite e inserta registros de forma idempotente. Sus funciones de conexión y lectura se reutilizan en EA2. |
| `src/generate_sample.py` | Genera CSV de muestra reproducible; `generate_csv_sample` lee de SQLite (EA1) y `write_csv_sample` recibe un DataFrame ya construido (EA2 y EA3), compartiendo la misma lógica de muestreo. |
| `src/audit.py` | Compara los datos de la API contra los de SQLite (por `id`) y genera `audit_report.txt` (EA1). |
| `src/main.py` | Orquesta el pipeline de ingestión de EA1 de punta a punta. |
| `src/spark_session.py` | Crea y cierra la `SparkSession` local (`local[*]`); `pandas_to_spark` es el puente Pandas→Spark compartido por EA2 y EA3. |
| `src/profiling.py` | Calcula métricas de perfilamiento (registros, nulos, duplicados, distintos por columna, validez de fechas) sobre un DataFrame de Spark; se usa antes y después de la limpieza. |
| `src/cleaning.py` | Implementa la deduplicación, normalización de texto, tratamiento de nulos, corrección de tipos, columna derivada `release_year` y análisis de outliers (EA2). |
| `src/validation.py` | Valida que el origen (SQLite) exista y tenga datos, que el esquema cargado sea el esperado, que el dataset limpio cumpla las reglas de calidad, y calcula el Data Quality Score (EA2). |
| `src/preprocessing.py` | Orquesta el pipeline ELT completo de EA2 (Extract → Load → Transform) y genera el reporte de limpieza. |
| `src/gamerpower_client.py` | Descarga giveaways de GamerPower, guarda el RAW tal cual y extrae los registros para procesamiento. |
| `src/mmobomb_client.py` | Descarga el catálogo de MMOBomb, guarda el RAW tal cual y extrae los registros para procesamiento. |
| `src/title_matching.py` | Normaliza títulos (`title_match`), aplica el mapping manual y consolida duplicados por título con una regla de recencia documentada — todo en Pandas, sin UDFs de Spark. |
| `src/enrichment_audit.py` | Dataclasses de resultado de cada integración/checkpoint de cardinalidad y la función que renderiza `enrichment_report.txt`. |
| `src/enrichment.py` | Orquesta el pipeline de EA3: descarga ambas APIs, homologa por título, integra con LEFT JOIN, controla cardinalidad, valida consistencia y genera las evidencias. |
| `tests/test_ingestion.py` | Pruebas unitarias de EA1 con `pytest`, usando mocks para no depender de la red. |
| `tests/test_preprocessing.py` | Pruebas unitarias de EA2 con DataFrames de Spark pequeños, creados específicamente para cada prueba. |
| `tests/test_enrichment.py` | Pruebas unitarias de EA3: descarga (con mocks HTTP), `title_match`, deduplicación, joins y generación de evidencias. |
| `conftest.py` | Agrega la raíz del proyecto a `sys.path` para que `pytest` resuelva los imports `src.*` en las tres actividades. |

## Requisitos

- Python 3.10 o superior
- Git
- **Java 11 o 17 (JDK)**, únicamente para ejecutar la Actividad 2 (PySpark corre sobre la JVM).
  No es necesario para ejecutar solamente la Actividad 1.

## Instalación

### Windows

```bash
git clone https://github.com/felipe-fernandez-rodriguez/game-data-ingestion-project.git
cd ea1-api-ingestion
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Linux / macOS

```bash
git clone https://github.com/felipe-fernandez-rodriguez/game-data-ingestion-project.git
cd ea1-api-ingestion
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Ejecución

Desde la raíz del proyecto, con el entorno virtual activado:

```bash
python src/main.py
```

El script:

1. Crea los directorios necesarios (`data/raw`, `data/database`, `output`) si no existen.
2. Consulta la API de FreeToGame.
3. Guarda la respuesta cruda en `data/raw/games.json`.
4. Crea/actualiza `data/database/games.db` e inserta los registros de forma idempotente.
5. Genera `output/games_sample.csv` con Pandas.
6. Genera `output/audit_report.txt` comparando API vs SQLite.
7. Imprime un resumen final en consola mediante `logging`.

**Códigos de salida:** `0` = ejecución y auditoría exitosas · `1` = error en el pipeline ·
`2` = pipeline ejecutado pero la auditoría detectó diferencias.

## Pruebas

```bash
pytest -v
```

Las pruebas cubren, como mínimo:

- Que la API devuelve una estructura válida (y que se detectan estructuras inválidas).
- Que la base de datos y la tabla `games` se crean correctamente.
- Que el número de registros insertados es mayor que cero y que la inserción es idempotente.
- Que el archivo CSV de muestra se genera correctamente.
- Que el archivo TXT de auditoría se genera correctamente.
- Que la auditoría detecta correctamente registros faltantes y diferencias de contenido.

Las pruebas que involucran la API utilizan `unittest.mock` para simular la respuesta HTTP y así
evitar depender de la red (importante para que el workflow de GitHub Actions sea estable).

## Archivos generados

| Archivo | Descripción |
|---|---|
| `data/raw/games.json` | Copia exacta de la respuesta JSON entregada por la API en la última ejecución. |
| `data/database/games.db` | Base de datos SQLite con la tabla `games` poblada. |
| `output/games_sample.csv` | Muestra reproducible (hasta 20 filas, `random_state=42`) de los datos almacenados. |
| `output/audit_report.txt` | Comparación detallada entre lo extraído de la API y lo almacenado en SQLite. |

## GitHub Actions

El workflow `.github/workflows/ingestion.yml` se ejecuta automáticamente en cada `push`, puede
lanzarse manualmente desde la pestaña **Actions** (`workflow_dispatch`) y además cuenta con una
ejecución programada diaria (`schedule`).

En cada ejecución el workflow:

1. Descarga el repositorio.
2. Configura Python 3.11.
3. Instala las dependencias desde `requirements.txt`.
4. Ejecuta la suite de pruebas con `pytest`.
5. Ejecuta el pipeline completo (`python src/main.py`).
6. Verifica que los tres archivos de evidencia existan.
7. Muestra en el log información de los archivos generados.
8. Publica `games.json`, `games.db`, `games_sample.csv` y `audit_report.txt` como
   **artifacts** del workflow, descargables desde la página de la ejecución en la pestaña
   *Actions* del repositorio (retención de 30 días).

### Archivos versionados vs. artifacts de GitHub Actions

- **Archivos versionados en el repositorio:** quedan en el historial de Git, cualquiera que
  clone el repositorio los ve tal como quedaron en el último commit, pero **no reflejan
  automáticamente el resultado de cada ejecución del workflow** a menos que se agregue un paso
  adicional que haga commit de los cambios.
- **Artifacts de GitHub Actions:** son la evidencia oficial de *cada ejecución individual* del
  workflow. No requieren modificar el repositorio, se generan y almacenan automáticamente, y
  permiten comparar resultados entre distintas ejecuciones (por ejemplo, para detectar cambios
  en el catálogo de juegos con el paso del tiempo).

Este proyecto usa ambos mecanismos de forma complementaria: los archivos quedan en el
repositorio como evidencia "viva" del último estado, y además cada ejecución del workflow deja
su propio artifact descargable de forma independiente.

## Validación

Para comprobar que la ingesta fue exitosa:

1. **Cantidad de registros:** revisar la línea `Registros insertados/actualizados: XXX` en el
   log de ejecución, o el conteo en `output/audit_report.txt`.
2. **Existencia de la base de datos:** verificar que `data/database/games.db` exista y que la
   tabla `games` contenga filas (`SELECT COUNT(*) FROM games;`).
3. **Existencia del CSV:** verificar que `output/games_sample.csv` exista y contenga filas con
   encabezados coherentes con las columnas de `games`.
4. **Contenido del reporte de auditoría:** abrir `output/audit_report.txt` y confirmar que el
   resultado final indique `VALIDACIÓN EXITOSA`, sin IDs faltantes, adicionales ni registros con
   diferencias.
5. **Ejecución exitosa de GitHub Actions:** revisar en la pestaña *Actions* del repositorio que
   el workflow `EA1 - Ingestión de Datos desde API` haya finalizado en verde (✔) y descargar el
   artifact `evidencias-ingestion-ea1` para inspeccionar los archivos generados en esa ejecución.

---

# Actividad 2 - Preprocesamiento, Limpieza y ELT

## Objetivo

La Actividad 1 resuelve la ingesta de datos "crudos" hacia SQLite, pero no garantiza que esos
datos estén libres de duplicados, valores nulos, tipos inconsistentes o formatos de texto poco
uniformes. La Actividad 2 resuelve exactamente ese problema: toma los datos ya almacenados en
SQLite, los perfila, los limpia y los transforma con PySpark, dejando como resultado un dataset
confiable (`data/processed/games_cleaned.csv`) listo para ser consumido por las siguientes
actividades del proyecto integrador (enriquecimiento, modelado, análisis).

## Relación con la Actividad 1

La Actividad 2 **no vuelve a consultar la API de FreeToGame**. Reutiliza directamente lo que la
Actividad 1 ya dejó almacenado:

```text
Actividad 1:  FreeToGame API → SQLite (games.db)
Actividad 2:  SQLite (games.db) → PySpark → Limpieza → games_cleaned.csv
```

Para leer SQLite, la Actividad 2 reutiliza las mismas funciones de `src/database.py` que usa la
Actividad 1 (`get_connection`, `fetch_all_games`), en vez de reimplementar una segunda lógica de
conexión. De la misma forma, `src/generate_sample.py` se extendió (no se duplicó) para que tanto
EA1 como EA2 generen sus respectivos CSV de muestra con la misma función de muestreo reproducible.

## Arquitectura ELT

```text
               ACTIVIDAD 1
             FreeToGame API
                    ↓
              Python Requests
                    ↓
                 JSON
                    ↓
                 SQLite
                    │
                    │
                    ▼
               ACTIVIDAD 2
                    │
                EXTRACT
                    ↓
              SQLite → Spark
                    │
                  LOAD
                    ↓
             Spark DataFrame
                    │
               PROFILE
                    ↓
              DATA QUALITY
                    │
               TRANSFORM
                    ↓
              CLEAN DATASET
                    ↓
             CSV / Evidencias
```

### ¿Por qué esto es ELT y no ETL?

- **ETL** (Extract → Transform → Load): los datos se transforman *antes* de cargarlos en su
  destino final de almacenamiento/análisis.
- **ELT** (Extract → Load → Transform): los datos se cargan primero, "tal cual", en el entorno de
  procesamiento, y **después** se transforman dentro de ese entorno.

En este proyecto:

- **Extract:** `src/preprocessing.py` lee los registros crudos desde `data/database/games.db`
  (la etapa `EXTRACT` del pipeline, implementada en `extract_from_sqlite`).
- **Load:** esos registros se cargan tal cual (sin limpiar) en un `DataFrame` de PySpark
  (`load_into_spark`), que actúa como el entorno de procesamiento.
- **Transform:** una vez los datos ya están "dentro" de Spark, se perfila su calidad y se aplican
  todas las transformaciones (deduplicación, nulos, tipos, texto, fechas) usando las
  transformaciones distribuidas de Spark (`src/cleaning.py`).

Esto es una **simulación de arquitectura cloud**: SQLite representa el almacenamiento previo
(equivalente a un data lake o una base de origen), y PySpark en modo `local[*]` representa el
motor de procesamiento distribuido (equivalente a un clúster Spark administrado en la nube, como
Databricks o EMR), ejecutado localmente para fines académicos.

## Tecnologías

- **Python:** lenguaje base del proyecto (compartido con EA1).
- **PySpark:** motor de procesamiento principal de EA2; ejecuta perfilamiento y limpieza como
  transformaciones sobre un `DataFrame` distribuido, en modo local (`local[*]`).
- **Pandas:** se mantiene para la generación de evidencias (CSV de muestra) a partir del dataset
  ya limpio (`spark_df.toPandas()`), igual que en EA1.
- **SQLite:** fuente de datos de EA2 (no destino); es la misma base de datos que EA1 generó.
- **GitHub Actions:** automatiza la ejecución de EA2 en un workflow independiente
  (`bigdata.yml`), que depende de que la base de EA1 ya exista en el repositorio.

## Perfilamiento y limpieza aplicados

1. **Perfilamiento inicial:** registros, columnas, duplicados (completos y por `id`), nulos,
   vacíos, IDs únicos, rango de `id`, validez de `release_date` y valores distintos en columnas
   categóricas (`genre`, `platform`, `publisher`, `developer`).
2. **Eliminación de duplicados:** primero duplicados completos, luego duplicados por `id` (clave
   lógica del juego), conservando una única versión de cada registro.
3. **Normalización de texto:** recorte de espacios y colapso de espacios internos en todas las
   columnas de texto (incluye las variables categóricas), y conversión de cadenas vacías a `NULL`
   antes del tratamiento de nulos.
4. **Tratamiento de nulos**, según la importancia del campo:
   - Campos críticos (`id`, `title`): la fila se elimina si faltan.
   - Campos importantes (`genre`, `platform`, `publisher`, `developer`): se imputan como `"Unknown"`.
   - Campos descriptivos (`thumbnail`, `short_description`, `game_url`, `freetogame_profile_url`):
     se imputan como `"Not Available"`.
5. **Corrección de tipos:** `id` → `IntegerType`, campos de texto → `StringType`, `release_date` →
   `DateType` (formato `yyyy-MM-dd`); los valores no convertibles quedan como `NULL` y se cuentan.
6. **Columna derivada:** `release_year`, extraída de `release_date` ya validada.
7. **Análisis de outliers:** se evalúan las columnas numéricas continuas disponibles. En este
   dataset, `id` es el único campo numérico y es un identificador secuencial, no una métrica
   continua, por lo que el proyecto documenta explícitamente que no aplica un análisis de
   outliers sobre `id` (evitando falsos positivos), dejando el método IQR ya implementado para
   futuras columnas numéricas que puedan incorporarse.
8. **Validación final y Data Quality Score:** se confirma que no queden IDs duplicados ni nulos
   en campos críticos, que los tipos sean correctos, y se calcula un score de calidad (0-100%)
   como promedio de cuatro componentes documentados en `src/validation.py`.

## Ejecución local

```bash
python src/preprocessing.py
```

El script:

1. Verifica que `data/database/games.db` exista y tenga registros (si no, se detiene con error).
2. Inicializa una `SparkSession` local (`local[*]`).
3. Extrae los registros desde SQLite y los carga en un DataFrame de Spark (vía Pandas).
4. Ejecuta el perfilamiento inicial.
5. Aplica la limpieza y transformación completas.
6. Ejecuta el perfilamiento final y la validación de calidad.
7. Cierra Spark correctamente.
8. Genera `data/processed/games_cleaned.csv`, `output/cleaned_games_sample.csv` y
   `output/cleaning_report.txt` con Pandas.
9. Imprime un resumen final en consola.

**Códigos de salida:** `0` = ejecución y validación exitosas · `1` = error en el pipeline (por
ejemplo, si `games.db` no existe) · `2` = pipeline ejecutado pero con observaciones de calidad.

### Solución de problemas: `ModuleNotFoundError: No module named 'distutils'`

Si al ejecutar `python src/preprocessing.py` aparece este error (típicamente en Windows, con
Python 3.12 o superior), la causa es conocida: Python 3.12 eliminó el módulo `distutils` de su
librería estándar, y PySpark 3.5.x todavía lo usa internamente para comparar versiones de Pandas
(se dispara, por ejemplo, al llamar `spark.createDataFrame()`). Además, los entornos virtuales
creados con Python 3.12+ ya no incluyen `setuptools` por defecto, que antes cubría ese hueco.

El proyecto ya incluye la solución en dos niveles:

1. `requirements.txt` agrega `setuptools`, que provee una copia de `distutils` compatible.
2. `src/spark_session.py` importa `setuptools` explícitamente antes de usar Spark (el workaround
   oficial documentado por Apache Spark en
   [SPARK-47613](https://issues.apache.org/jira/browse/SPARK-47613)).

Si el error persiste (por ejemplo, porque el entorno virtual ya estaba creado antes de este
cambio), basta con reinstalar dependencias dentro del `venv`:

```bash
pip install --upgrade -r requirements.txt
```

La solución definitiva (sin depender de este workaround) es actualizar a `pyspark>=4.0`, versión
en la que Apache Spark eliminó por completo el uso de `distutils`
([SPARK-44120](https://issues.apache.org/jira/browse/SPARK-44120)); por eso el workflow de
GitHub Actions (`bigdata.yml`) fija Python 3.11, donde este problema no existe.

### Solución de problemas: `PicklingError: ... RecursionError: Stack overflow`

Si al ejecutar `python src/preprocessing.py` aparece este error justo en la línea
`spark.createDataFrame(pdf)` (típicamente en Windows), la causa es otra limitación conocida de
PySpark 3.5.x: sin Apache Arrow habilitado, la conversión de un DataFrame de Pandas a uno de
Spark usa una ruta antigua basada en RDDs, en la que cada fila se envuelve en una función Python
que se serializa con `cloudpickle` para enviarla a la JVM. En Windows, con Python 3.12+ y ciertas
combinaciones de versiones, esa serialización puede entrar en una recursión que agota la pila.

El proyecto ya incluye la solución: `src/spark_session.py` habilita la conversión Pandas↔Spark
vía **Apache Arrow** (`spark.sql.execution.arrow.pyspark.enabled=true`), que reemplaza por
completo esa ruta basada en `cloudpickle` por un formato binario columnar, evitando el problema y
siendo además más rápida. Esto requiere el paquete `pyarrow`, ya agregado a `requirements.txt`.

Si ya tenías el entorno virtual creado antes de este cambio, solo necesitas reinstalar
dependencias:

```bash
pip install --upgrade -r requirements.txt
```

## Pruebas

```bash
pytest -v
```

`pytest` ejecuta automáticamente tanto `tests/test_ingestion.py` (EA1) como
`tests/test_preprocessing.py` (EA2). Las pruebas de EA2 usan DataFrames de Spark pequeños,
creados específicamente para cada caso (incluyendo un `id` duplicado y un registro con campos
críticos nulos a propósito), y no dependen de que la Actividad 1 se haya ejecutado previamente
(salvo la prueba de extracción, que construye su propia base SQLite temporal).

## GitHub Actions

El workflow `.github/workflows/bigdata.yml` ejecuta **únicamente** la Actividad 2, y es
independiente del workflow de ingestión (`ingestion.yml`). En cada ejecución:

1. Descarga el repositorio y configura Python 3.11.
2. Configura Java 17 (Temurin), requerido para que PySpark funcione sobre la JVM del runner.
3. Instala las dependencias desde `requirements.txt` (incluye `pyspark`).
4. Verifica que `data/database/games.db` exista en el repositorio (si no, falla explícitamente
   indicando que se debe ejecutar primero la Actividad 1).
5. Ejecuta la suite de pruebas (`pytest`).
6. Ejecuta el pipeline completo (`python src/preprocessing.py`), que internamente crea la
   `SparkSession`, extrae, carga, perfila, limpia y valida los datos.
7. Verifica que los tres archivos de evidencia de EA2 existan.
8. Muestra en el log información de los archivos generados (incluyendo el reporte completo de
   limpieza).
9. Publica `games_cleaned.csv`, `cleaned_games_sample.csv` y `cleaning_report.txt` como
   **artifacts** del workflow (`evidencias-preprocesamiento-ea2`, retención de 30 días).

**Configuración de PySpark en GitHub Actions:** además de instalar Java, el workflow fija las
variables de entorno `PYSPARK_PYTHON=python` (para que el driver y los executors usen el mismo
intérprete) y `SPARK_LOCAL_IP=127.0.0.1` (evita que Spark intente resolver el hostname del
runner por red, lo cual puede fallar o ser lento en el entorno de CI). No se requiere
configuración de memoria adicional dado el tamaño reducido del dataset.

## Evidencias

| Archivo | Descripción |
|---|---|
| `data/processed/games_cleaned.csv` | Dataset completo ya limpio y transformado; es la entrada esperada para las siguientes actividades del proyecto integrador. |
| `output/cleaned_games_sample.csv` | Muestra reproducible (hasta 20 filas, `random_state=42`, adaptada dinámicamente si hay menos registros) del dataset limpio. |
| `output/cleaning_report.txt` | Auditoría completa: perfilamiento inicial, operaciones de limpieza aplicadas, comparación antes/después, validación final y Data Quality Score. |

---

# Actividad 3 - Enriquecimiento de Datos

## Objetivo

La Actividad 2 deja un dataset limpio, pero limitado a los campos que la API de FreeToGame
expone directamente (título, género, plataforma, fechas, etc.). La Actividad 3 amplía ese
dataset integrando dos APIs REST reales adicionales —**GamerPower** (giveaways) y **MMOBomb**
(catálogo de juegos)—, homologando sus títulos y combinándolos mediante `LEFT JOIN`, para producir
un dataset **enriquecido** con más atributos por juego, preparado para que la Actividad 4
(modelado) tenga variables adicionales con las que trabajar.

A diferencia de una versión anterior de esta actividad (que usaba fuentes auxiliares simuladas en
seis formatos de archivo), esta versión consulta **APIs externas reales** y conserva sus
respuestas **exclusivamente en JSON**, tal como las entrega cada proveedor.

## Fuentes utilizadas

### GamerPower

- **Documentación:** `https://www.gamerpower.com/api-read`
- **Endpoint utilizado:** `https://www.gamerpower.com/api/giveaways`
- **Formato:** JSON
- **Archivo RAW:** `data/raw/gamerpower/giveaways.json`
- **Propósito:** obtener información complementaria sobre giveaways activos (valor estimado,
  estado, usuarios interesados, fechas de publicación/expiración) relacionados con juegos.

`data/raw/gamerpower/giveaways.json` es la respuesta descargada **directamente** desde
`https://www.gamerpower.com/api/giveaways`, sin ninguna transformación: mismos nombres de campo,
mismos valores, misma estructura. No es un archivo generado artificialmente.

**Atribución:** los datos de giveaways provienen de [GamerPower.com](https://www.gamerpower.com/),
tal como exige su política de uso (atribución obligatoria; API gratuita, sin necesidad de key).

### MMOBomb

- **Documentación:** `https://www.mmobomb.com/api`
- **Endpoint utilizado:** `https://www.mmobomb.com/api1/games`
- **Formato:** JSON
- **Archivo RAW:** `data/raw/mmobomb/games.json`
- **Propósito:** obtener información complementaria del catálogo de juegos (género, plataforma,
  publisher, developer, fecha de lanzamiento) para contrastarla con la de FreeToGame.

`data/raw/mmobomb/games.json` es igualmente la respuesta descargada **directamente** desde
`https://www.mmobomb.com/api1/games`, conservada tal cual.

**Atribución:** los datos de catálogo provienen de [MMOBomb.com](https://www.mmobomb.com/).

### Resumen

| Fuente | Formato | Propósito | Clave | Columnas nuevas |
|---|---|---|---|---|
| GamerPower | JSON | Giveaways activos relacionados con juegos | `title_match` (derivada de `title`) | `gamerpower_id`, `gamerpower_worth`, `gamerpower_type`, `gamerpower_platforms`, `gamerpower_status`, `gamerpower_users`, `gamerpower_published_date`, `gamerpower_end_date`, `gamerpower_url` |
| MMOBomb | JSON | Catálogo complementario de juegos | `title_match` (derivada de `title`) | `mmobomb_id`, `mmobomb_genre`, `mmobomb_platform`, `mmobomb_publisher`, `mmobomb_developer`, `mmobomb_release_date`, `mmobomb_profile_url`, y `genre_match`/`platform_match`/`publisher_match`/`developer_match` |

## Por qué NO se cruzan los `id` entre proveedores

FreeToGame, GamerPower y MMOBomb son **tres sistemas independientes**, cada uno con su propio
esquema de identificadores internos. Nada garantiza que `FreeToGame.id == MMOBomb.id`; de hecho,
en general no lo son. Por eso:

- El `id` de FreeToGame se conserva como identificador principal del juego en el dataset final.
- `gamerpower_id` y `mmobomb_id` se conservan como columnas separadas (procedencia), nunca como
  reemplazo de `id`.
- La integración usa como clave lógica el **título del juego**, normalizado en la columna
  auxiliar `title_match`.

## Normalización de `title_match`

`src/title_matching.py` implementa `normalize_title`, aplicada únicamente para producir
`title_match` (nunca se modifica `title`):

1. Elimina el sufijo final `"Giveaway"` (frecuente en GamerPower).
2. Elimina anotaciones de distribuidor/plataforma conocidas entre paréntesis — `(Steam)`,
   `(Epic Games)`, `(GOG)`, `(Indiegala)`, etc. (lista cerrada y explícita, no un `regex` genérico
   sobre cualquier paréntesis).
3. Convierte a minúsculas.
4. Recorta espacios y colapsa espacios múltiples.

Ejemplo real usado como caso de prueba:

```text
title:        "Sausage Hunter (Indiegala) Giveaway"
title_match:  "sausage hunter"
```

### Estrategia de matching (tres niveles)

1. **Coincidencia exacta** sobre `title_match` — la normalización anterior ya resuelve la mayoría
   de las variaciones triviales (espacios, mayúsculas, ruido promocional de GamerPower), por lo
   que los niveles 1 y 2 del enunciado quedan unificados en una sola función determinística.
2. **Mapping manual explícito** (`data/mappings/title_mapping.csv`, columnas
   `freetogame_title,gamerpower_title,mmobomb_title,match_method`): para casos que la
   normalización automática no resuelve. **El archivo se entrega vacío** (solo encabezado): el
   proyecto no inventa equivalencias; se agregan filas manualmente solo cuando se verifica un caso
   real, y `apply_title_mapping` las usa como anulación explícita de `title_match`.
3. **Fuzzy matching: deliberadamente NO implementado para aceptar coincidencias automáticas.** Una
   coincidencia que no sea exacta (tras normalizar) ni esté en el mapping manual queda como
   **no coincidencia** (columnas de esa fuente en `NULL` para ese juego), nunca como una
   coincidencia forzada.

## Resolución de duplicados (antes del join, nunca sobre el RAW)

Los archivos RAW (`giveaways.json`, `games.json`) se guardan **exactamente como se reciben**: sin
eliminar duplicados, sin renombrar campos, sin normalizar valores. Toda limpieza ocurre solo sobre
la estructura ya cargada en memoria para el pipeline.

Si una fuente tiene varios registros para el mismo `title_match` (p. ej. dos giveaways activos del
mismo juego), `title_matching.resolve_duplicate_titles` consolida a un único registro
representativo con una regla determinística y documentada:

1. Se ordena por una columna de prioridad (`published_date` en GamerPower, `release_date` en
   MMOBomb) de forma descendente y se conserva el más reciente.
2. Como desempate final, se ordena por `id` de la fuente ascendente.

Esta lógica corre en **Pandas**, antes de crear cualquier DataFrame de Spark: expresarla como una
UDF de PySpark obligaría a serializar funciones Python hacia la JVM con `cloudpickle`, la misma
ruta que causó los errores de `RecursionError` en Windows ya documentados en `spark_session.py`.

## Arquitectura

```text
FreeToGame API
     ↓
EA1 → SQLite
     ↓
EA2 → PySpark / ELT → games_cleaned.csv
     ↓
EA3
     ↓
┌───────────────────────┐
│ GamerPower API (JSON) │
│ MMOBomb API (JSON)    │
└───────────────────────┘
     ↓
Homologación (title_match, Pandas)
     ↓
LEFT JOIN (Spark, por title_match)
     ↓
Validación (cardinalidad, duplicados, consistencia)
     ↓
games_enriched.csv
     ↓
EA4
```

## Estrategia de integración

Por defecto se usa **LEFT JOIN**, con el dataset base (salida de EA2) como referencia principal:
así ningún registro de `games_cleaned.csv` se pierde solo porque GamerPower o MMOBomb no tengan
información para ese juego. Como cada fuente se deja con `title_match` único (deduplicado) ANTES
del join, un `LEFT JOIN` nunca puede multiplicar filas del lado base: la granularidad **1 juego =
1 registro** queda garantizada por construcción, no solo verificada después.

Para medir cobertura de forma robusta (independiente de que las columnas nuevas puedan ser
legítimamente nulas), la clave de cada fuente se renombra temporalmente antes del join; que esa
columna quede nula tras el join indica, sin ambigüedad, que no hubo coincidencia para esa fila
(`src/enrichment.py::left_join_on_title`).

### Comparación FreeToGame vs. MMOBomb

Donde hubo coincidencia, se agregan columnas booleanas `genre_match`, `platform_match`,
`publisher_match` y `developer_match`, comparando (sin distinguir mayúsculas/espacios) el campo
original de FreeToGame contra el equivalente de MMOBomb. Si no hubo coincidencia con MMOBomb, esas
columnas quedan en `NULL` (no en `False`): `NULL` significa "no se pudo comparar", `False`
significaría "se comparó y difiere" — una distinción deliberada para no sugerir una discrepancia
inexistente.

## Control de cardinalidad

En cada etapa se registra la cantidad de registros antes y después
(`src/enrichment_audit.py::CardinalityCheckpoint`): dataset base → después de GamerPower →
después de MMOBomb → dataset final. Al cierre se valida explícitamente que:

- `games_enriched.csv` tenga la misma cantidad de filas que `games_cleaned.csv`.
- No existan `id` duplicados (`accumulated_df.count() == accumulated_df.select("id").distinct().count()`).
- Las columnas originales de EA2 no hayan cambiado de valor (comparación fila a fila contra el
  dataset base).

Todo esto queda documentado con cifras reales (nunca inventadas) en `output/enrichment_report.txt`.

## Data lineage

| Columna | Origen |
|---|---|
| `id`, `title`, `genre`, `platform`, `publisher`, `developer`, `release_date` | Actividad 1 / FreeToGame API |
| `release_year` | Actividad 2 / derivada de `release_date` |
| `title_match` | Actividad 3 / derivada de `title` (normalización para matching) |
| `gamerpower_id`, `gamerpower_worth`, `gamerpower_type`, `gamerpower_platforms`, `gamerpower_status`, `gamerpower_users`, `gamerpower_published_date`, `gamerpower_end_date`, `gamerpower_url` | Actividad 3 / GamerPower API |
| `mmobomb_id`, `mmobomb_genre`, `mmobomb_platform`, `mmobomb_publisher`, `mmobomb_developer`, `mmobomb_release_date`, `mmobomb_profile_url` | Actividad 3 / MMOBomb API |
| `genre_match`, `platform_match`, `publisher_match`, `developer_match` | Actividad 3 / comparación FreeToGame vs. MMOBomb |

Esta misma tabla se genera automáticamente al final de `output/enrichment_report.txt`.

## Ejecución local

```bash
python src/enrichment.py
```

El script:

1. Verifica que `games_cleaned.csv` (EA2) exista.
2. Carga el dataset base y calcula `title_match`.
3. Descarga GamerPower y MMOBomb, guardando el RAW tal cual en `data/raw/`.
4. Aplica el mapping manual de títulos (si existe alguna equivalencia agregada).
5. Consolida duplicados de `title_match` por fuente (regla de recencia documentada).
6. Inicializa Spark, hace `LEFT JOIN` con GamerPower y luego con MMOBomb, midiendo cobertura.
7. Agrega las columnas de comparación (`*_match`) contra MMOBomb.
8. Valida el resultado final y genera `data/enriched/games_enriched.csv`,
   `output/enriched_games_sample.csv` y `output/enrichment_report.txt`.

**Códigos de salida:** `0` = enriquecimiento y validación exitosos · `1` = error (falta el dataset
base, falla una API, JSON corrupto, etc.) · `2` = pipeline ejecutado con observaciones.

### Agregar una equivalencia manual de título

Si detectas un caso real que la normalización automática no resuelve, agrega una fila a
`data/mappings/title_mapping.csv`:

```text
freetogame_title,gamerpower_title,mmobomb_title,match_method
World of Tanks,WoT Steam Bundle Giveaway,,manual
```

Solo se deben agregar equivalencias verificadas manualmente; el proyecto nunca genera estas filas
automáticamente.

## Pruebas

```bash
pytest -v
```

`pytest` ejecuta automáticamente `test_ingestion.py` (EA1), `test_preprocessing.py` (EA2) y
`test_enrichment.py` (EA3). Las llamadas a GamerPower y MMOBomb se simulan con mocks (igual que la
API de FreeToGame en EA1), por lo que las pruebas no dependen de la red ni de la disponibilidad de
las APIs externas.

## GitHub Actions

El workflow `.github/workflows/bigdata.yml` ejecuta EA2 y EA3 en el mismo job: primero preprocesa
y limpia (EA2), luego ejecuta el enriquecimiento (EA3), que internamente descarga ambas APIs y
guarda sus RAW. Publica artifacts separados: `gamerpower_raw`, `mmobomb_raw`, `enriched_dataset` y
`enrichment_evidence` (muestra + reporte de auditoría).

## Evidencias

| Archivo | Descripción |
|---|---|
| `data/raw/gamerpower/giveaways.json` | Respuesta RAW de GamerPower, sin transformar. |
| `data/raw/mmobomb/games.json` | Respuesta RAW de MMOBomb, sin transformar. |
| `data/enriched/games_enriched.csv` | Dataset completo enriquecido. |
| `output/enriched_games_sample.csv` | Muestra reproducible del dataset enriquecido (columnas originales y nuevas). |
| `output/enrichment_report.txt` | Auditoría completa: cobertura por fuente, homologación, duplicados, cardinalidad, validación final y data lineage. |

---

# Trazabilidad del Proyecto

| Etapa | Entrada | Proceso | Salida |
|---|---|---|---|
| EA1 | FreeToGame API | Ingestión (`src/main.py`) | SQLite (`games.db`) |
| EA1 | API | Serialización (`src/extract.py`) | `games.json` |
| EA1 | SQLite | Evidencia (`src/generate_sample.py`) | `games_sample.csv` |
| EA1 | API + SQLite | Auditoría (`src/audit.py`) | `audit_report.txt` |
| EA2 | `games.db` | Carga (`src/preprocessing.py` → Extract/Load) | Spark DataFrame |
| EA2 | Spark DataFrame | Perfilamiento (`src/profiling.py`) | Métricas de calidad |
| EA2 | Spark DataFrame | Limpieza (`src/cleaning.py`) | `games_cleaned.csv` |
| EA2 | Dataset limpio | Validación y auditoría (`src/validation.py`) | `cleaning_report.txt` |
| EA3 | GamerPower + MMOBomb APIs | Descarga (`src/gamerpower_client.py`, `src/mmobomb_client.py`) | `giveaways.json`, `games.json` |
| EA3 | `games_cleaned.csv` + JSON crudos | Homologación y matching (`src/title_matching.py`) | `title_match` por fuente |
| EA3 | Dataset limpio + fuentes homologadas | Enriquecimiento (`src/enrichment.py`) | `games_enriched.csv` |
| EA3 | Dataset enriquecido | Auditoría (`src/enrichment_audit.py`) | `enrichment_report.txt` |
| EA4 | `games_enriched.csv` | Modelado | Modelo final |

La salida de la Actividad 3 (`data/enriched/games_enriched.csv`) queda identificada como el punto
de entrada confiable para las siguientes fases del proyecto integrador:

```text
EA1 - Ingestión
        ↓
EA2 - Limpieza / Preprocesamiento
        ↓
EA3 - Enriquecimiento
        ↓
EA4 - Modelado
        ↓
EA5 - Análisis / Visualización
```
