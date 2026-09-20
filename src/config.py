"""
config.py
=========

Configuración centralizada del proyecto de Big Data (EA1 + EA2 + EA3).

Mantener todas las rutas, constantes y parámetros de configuración en un único
módulo evita "números mágicos" y rutas duplicadas a lo largo del código,
y facilita adaptar el proyecto a otros entornos (local, CI/CD, etc.).

Este módulo es compartido por las tres actividades:

- EA1 (ingestión): usa las rutas de ``RAW_DIR``, ``DB_DIR`` y los archivos
  ``games_sample.csv`` / ``audit_report.txt``.
- EA2 (preprocesamiento / ELT con PySpark): agrega ``PROCESSED_DIR`` y los
  archivos ``games_cleaned.csv`` / ``cleaned_games_sample.csv`` /
  ``cleaning_report.txt``, además de la clasificación de campos usada por
  la limpieza y el perfilamiento.
- EA3 (enriquecimiento con APIs de GamerPower y MMOBomb): agrega las rutas
  RAW de ambas APIs, el mapping manual de títulos y los archivos de salida
  del enriquecimiento.
"""

from __future__ import annotations

import logging
from pathlib import Path

# --------------------------------------------------------------------------
# Rutas base del proyecto (multiplataforma, usando pathlib)
# --------------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).resolve().parent.parent

DATA_DIR: Path = BASE_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
DB_DIR: Path = DATA_DIR / "database"

PROCESSED_DIR: Path = DATA_DIR / "processed"
MAPPINGS_DIR: Path = DATA_DIR / "mappings"
ENRICHED_DIR: Path = DATA_DIR / "enriched"

GAMERPOWER_RAW_DIR: Path = RAW_DIR / "gamerpower"
MMOBOMB_RAW_DIR: Path = RAW_DIR / "mmobomb"

OUTPUT_DIR: Path = BASE_DIR / "output"

# --------------------------------------------------------------------------
# Archivos generados por el pipeline de EA1 (ingestión)
# --------------------------------------------------------------------------
RAW_JSON_PATH: Path = RAW_DIR / "games.json"
DB_PATH: Path = DB_DIR / "games.db"
CSV_SAMPLE_PATH: Path = OUTPUT_DIR / "games_sample.csv"
AUDIT_REPORT_PATH: Path = OUTPUT_DIR / "audit_report.txt"

# --------------------------------------------------------------------------
# Archivos generados por el pipeline de EA2 (preprocesamiento / ELT)
# --------------------------------------------------------------------------
CLEANED_CSV_PATH: Path = PROCESSED_DIR / "games_cleaned.csv"
CLEANED_SAMPLE_PATH: Path = OUTPUT_DIR / "cleaned_games_sample.csv"
CLEANING_REPORT_PATH: Path = OUTPUT_DIR / "cleaning_report.txt"

# --------------------------------------------------------------------------
# Fuentes y archivos generados por el pipeline de EA3 (enriquecimiento)
# --------------------------------------------------------------------------
GAMERPOWER_JSON_PATH: Path = GAMERPOWER_RAW_DIR / "giveaways.json"
MMOBOMB_JSON_PATH: Path = MMOBOMB_RAW_DIR / "games.json"
TITLE_MAPPING_PATH: Path = MAPPINGS_DIR / "title_mapping.csv"

ENRICHED_CSV_PATH: Path = ENRICHED_DIR / "games_enriched.csv"
ENRICHED_SAMPLE_PATH: Path = OUTPUT_DIR / "enriched_games_sample.csv"
ENRICHMENT_REPORT_PATH: Path = OUTPUT_DIR / "enrichment_report.txt"

# --------------------------------------------------------------------------
# Configuración de las APIs
# --------------------------------------------------------------------------
API_URL: str = "https://www.freetogame.com/api/games"
API_DOC_URL: str = "https://www.freetogame.com/api-doc"
REQUEST_TIMEOUT_SECONDS: int = 15

GAMERPOWER_DOC_URL: str = "https://www.gamerpower.com/api-read"
GAMERPOWER_API_URL: str = "https://www.gamerpower.com/api/giveaways"

MMOBOMB_DOC_URL: str = "https://www.mmobomb.com/api"
MMOBOMB_API_URL: str = "https://www.mmobomb.com/api1/games"

# Campos esperados en cada registro devuelto por la API de FreeToGame.
# Se usan para validar la estructura de la respuesta sin asumir que
# nunca cambiará (ver punto 18 del enunciado de EA1: "la API puede cambiar").
EXPECTED_FIELDS: tuple[str, ...] = (
    "id",
    "title",
    "thumbnail",
    "short_description",
    "game_url",
    "genre",
    "platform",
    "publisher",
    "developer",
    "release_date",
    "freetogame_profile_url",
)

# Campos documentados de la respuesta de GamerPower (ver
# https://www.gamerpower.com/api-read). No se asume que todos estén
# siempre presentes; se valida en tiempo de ejecución.
GAMERPOWER_EXPECTED_FIELDS: tuple[str, ...] = (
    "id",
    "title",
    "worth",
    "thumbnail",
    "image",
    "description",
    "instructions",
    "open_giveaway_url",
    "published_date",
    "type",
    "platforms",
    "end_date",
    "users",
    "status",
    "gamerpower_url",
)

# Campos documentados de la respuesta de MMOBomb (ver
# https://www.mmobomb.com/api), con el mismo esquema que FreeToGame.
MMOBOMB_EXPECTED_FIELDS: tuple[str, ...] = (
    "id",
    "title",
    "thumbnail",
    "short_description",
    "game_url",
    "genre",
    "platform",
    "publisher",
    "developer",
    "release_date",
    "profile_url",
)

# --------------------------------------------------------------------------
# Configuración de la muestra CSV (usada por EA1, EA2 y EA3)
# --------------------------------------------------------------------------
SAMPLE_SIZE: int = 20
SAMPLE_RANDOM_STATE: int = 42

# --------------------------------------------------------------------------
# Directorios que deben existir antes de ejecutar cualquiera de los pipelines
# --------------------------------------------------------------------------
REQUIRED_DIRS: tuple[Path, ...] = (
    RAW_DIR,
    DB_DIR,
    PROCESSED_DIR,
    GAMERPOWER_RAW_DIR,
    MMOBOMB_RAW_DIR,
    MAPPINGS_DIR,
    ENRICHED_DIR,
    OUTPUT_DIR,
)


def ensure_directories() -> None:
    """Crea todos los directorios requeridos por el proyecto si no existen."""
    for directory in REQUIRED_DIRS:
        directory.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# Configuración de Spark (EA2 y EA3)
# --------------------------------------------------------------------------
SPARK_APP_NAME: str = "EA2-Preprocesamiento-FreeToGame"
SPARK_MASTER: str = "local[*]"

# --------------------------------------------------------------------------
# Clasificación de campos para la limpieza de datos (EA2)
# --------------------------------------------------------------------------
# `id` y `title` son indispensables para identificar y describir un
# registro: sin ellos, la fila no aporta valor y se descarta.
CRITICAL_FIELDS: tuple[str, ...] = ("id", "title")

# Campos relevantes para el análisis, pero cuya ausencia no invalida el
# registro: se imputan con un valor explícito ("Unknown") en vez de
# eliminarse o inventarse.
IMPORTANT_FIELDS: tuple[str, ...] = ("genre", "platform", "publisher", "developer")

# Campos descriptivos/de enlace: su ausencia no afecta el análisis
# estructurado, se marcan explícitamente como "Not Available".
DESCRIPTIVE_FIELDS: tuple[str, ...] = (
    "thumbnail",
    "short_description",
    "game_url",
    "freetogame_profile_url",
)

# Columnas categóricas sobre las que se aplica normalización de texto
# y perfilamiento de valores distintos.
CATEGORICAL_FIELDS: tuple[str, ...] = ("genre", "platform", "publisher", "developer")

UNKNOWN_PLACEHOLDER: str = "Unknown"
NOT_AVAILABLE_PLACEHOLDER: str = "Not Available"

# Formato de fecha esperado desde la API / SQLite (ISO 8601).
RELEASE_DATE_FORMAT: str = "yyyy-MM-dd"


def configure_logging() -> None:
    """
    Configura el logging estándar del proyecto.

    Centralizado aquí para que ``src/main.py`` (EA1), ``src/preprocessing.py``
    (EA2) y ``src/enrichment.py`` (EA3) compartan el mismo formato de
    mensajes, evitando duplicar la configuración de ``logging.basicConfig``
    en cada script de entrada.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# --------------------------------------------------------------------------
# Configuración del enriquecimiento (EA3)
# --------------------------------------------------------------------------
# Prefijos usados para las columnas nuevas de cada fuente, de modo que su
# origen sea evidente en el dataset final y nunca se sobrescriban columnas
# del dataset base (punto 21/22 del enunciado).
GAMERPOWER_PREFIX: str = "gamerpower_"
MMOBOMB_PREFIX: str = "mmobomb_"

# Columnas de GamerPower que se incorporan al dataset enriquecido (con el
# prefijo ya aplicado). Se seleccionan solo los campos que aportan valor
# nuevo (se descartan campos puramente de marketing como `description`,
# `instructions` o `image`).
GAMERPOWER_SELECTED_COLUMNS: dict[str, str] = {
    "id": "gamerpower_id",
    "worth": "gamerpower_worth",
    "type": "gamerpower_type",
    "platforms": "gamerpower_platforms",
    "status": "gamerpower_status",
    "users": "gamerpower_users",
    "published_date": "gamerpower_published_date",
    "end_date": "gamerpower_end_date",
    "gamerpower_url": "gamerpower_url",
}

# Columnas de MMOBomb que se incorporan al dataset enriquecido (con el
# prefijo ya aplicado).
MMOBOMB_SELECTED_COLUMNS: dict[str, str] = {
    "id": "mmobomb_id",
    "genre": "mmobomb_genre",
    "platform": "mmobomb_platform",
    "publisher": "mmobomb_publisher",
    "developer": "mmobomb_developer",
    "release_date": "mmobomb_release_date",
    "profile_url": "mmobomb_profile_url",
}

# Campos de MMOBomb comparables contra los equivalentes de FreeToGame
# cuando existe coincidencia (punto 24 del enunciado): genera columnas
# booleanas `<campo>_match`.
MMOBOMB_COMPARISON_FIELDS: tuple[str, ...] = ("genre", "platform", "publisher", "developer")

# Data lineage: de dónde proviene cada columna del dataset final.
# `games_cleaned.csv` ya documenta su propio linaje (EA1 -> EA2); aquí solo
# se agrega el de las columnas nuevas introducidas por EA3.
DATA_LINEAGE_EA3: dict[str, str] = {
    "title_match": "Actividad 3 / derivada de 'title' (normalización para matching)",
    "gamerpower_id": "Actividad 3 / GamerPower API",
    "gamerpower_worth": "Actividad 3 / GamerPower API",
    "gamerpower_type": "Actividad 3 / GamerPower API",
    "gamerpower_platforms": "Actividad 3 / GamerPower API",
    "gamerpower_status": "Actividad 3 / GamerPower API",
    "gamerpower_users": "Actividad 3 / GamerPower API",
    "gamerpower_published_date": "Actividad 3 / GamerPower API",
    "gamerpower_end_date": "Actividad 3 / GamerPower API",
    "gamerpower_url": "Actividad 3 / GamerPower API",
    "mmobomb_id": "Actividad 3 / MMOBomb API",
    "mmobomb_genre": "Actividad 3 / MMOBomb API",
    "mmobomb_platform": "Actividad 3 / MMOBomb API",
    "mmobomb_publisher": "Actividad 3 / MMOBomb API",
    "mmobomb_developer": "Actividad 3 / MMOBomb API",
    "mmobomb_release_date": "Actividad 3 / MMOBomb API",
    "mmobomb_profile_url": "Actividad 3 / MMOBomb API",
    "genre_match": "Actividad 3 / comparación FreeToGame vs MMOBomb",
    "platform_match": "Actividad 3 / comparación FreeToGame vs MMOBomb",
    "publisher_match": "Actividad 3 / comparación FreeToGame vs MMOBomb",
    "developer_match": "Actividad 3 / comparación FreeToGame vs MMOBomb",
}
