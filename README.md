# EA1. Ingestión de Datos desde un API

## Introducción

Este proyecto implementa la **primera etapa (EA1) de un proyecto integrador de Big Data**: la
ingestión de datos desde una API REST pública, su almacenamiento estructurado en una base de
datos SQLite, la generación de evidencias reproducibles (muestra en CSV y reporte de auditoría)
y la automatización completa del proceso mediante GitHub Actions.

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
ea1-api-ingestion/
│
├── .github/
│   └── workflows/
│       └── ingestion.yml        # Automatización con GitHub Actions
│
├── data/
│   ├── raw/
│   │   └── games.json           # Respuesta cruda de la API (evidencia)
│   └── database/
│       └── games.db             # Base de datos SQLite
│
├── output/
│   ├── games_sample.csv         # Muestra de datos generada con Pandas
│   └── audit_report.txt         # Reporte de auditoría API vs SQLite
│
├── src/
│   ├── __init__.py              # Marca src/ como paquete Python
│   ├── config.py                # Rutas y configuración centralizada del proyecto
│   ├── extract.py               # Extracción y validación de datos desde la API
│   ├── database.py              # Esquema SQLite e inserción idempotente
│   ├── generate_sample.py       # Generación del CSV de muestra con Pandas
│   ├── audit.py                 # Comparación API vs SQLite y reporte de auditoría
│   └── main.py                  # Orquestador del pipeline completo
│
├── tests/
│   └── test_ingestion.py        # Pruebas automatizadas con pytest (usa mocks para la API)
│
├── conftest.py                  # Configuración de sys.path para pytest
├── requirements.txt             # Dependencias del proyecto
├── README.md                    # Este documento
└── .gitignore
```

### Responsabilidad de cada archivo

| Archivo | Responsabilidad |
|---|---|
| `src/config.py` | Centraliza rutas (con `pathlib`), URL de la API, tamaño de muestra y demás constantes. |
| `src/extract.py` | Realiza la petición HTTP GET, valida código de estado, JSON y estructura, y guarda el JSON crudo. |
| `src/database.py` | Define el esquema `CREATE TABLE`, abre/crea la base SQLite e inserta registros de forma idempotente. |
| `src/generate_sample.py` | Lee SQLite con `pandas.read_sql_query` y genera `games_sample.csv`. |
| `src/audit.py` | Compara los datos de la API contra los de SQLite (por `id`) y genera `audit_report.txt`. |
| `src/main.py` | Orquesta todo el pipeline de punta a punta y define el código de salida del proceso. |
| `tests/test_ingestion.py` | Pruebas unitarias con `pytest`, usando mocks para no depender de la red. |
| `conftest.py` | Agrega la raíz del proyecto a `sys.path` para que `pytest` resuelva los imports `src.*`. |

## Requisitos

- Python 3.10 o superior
- Git

## Instalación

### Windows

```bash
git clone https://github.com/<tu-usuario>/ea1-api-ingestion.git
cd ea1-api-ingestion
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

### Linux / macOS

```bash
git clone https://github.com/<tu-usuario>/ea1-api-ingestion.git
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
