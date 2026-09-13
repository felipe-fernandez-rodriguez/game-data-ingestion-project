"""
config.py
=========

Configuración centralizada del proyecto de Big Data (EA1 + EA2).

Mantener todas las rutas, constantes y parámetros de configuración en un único
módulo evita "números mágicos" y rutas duplicadas a lo largo del código,
y facilita adaptar el proyecto a otros entornos (local, CI/CD, etc.).

Este módulo es compartido por ambas actividades:

- EA1 (ingestión): usa las rutas de ``RAW_DIR``, ``DB_DIR`` y los archivos
  ``games_sample.csv`` / ``audit_report.txt``.
- EA2 (preprocesamiento / ELT con PySpark): agrega ``PROCESSED_DIR`` y los
  archivos ``games_cleaned.csv`` / ``cleaned_games_sample.csv`` /
  ``cleaning_report.txt``, además de la clasificación de campos usada por
  la limpieza y el perfilamiento.
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
# Configuración de la API
# --------------------------------------------------------------------------
API_URL: str = "https://www.freetogame.com/api/games"
API_DOC_URL: str = "https://www.freetogame.com/api-doc"
REQUEST_TIMEOUT_SECONDS: int = 15

# Campos esperados en cada registro devuelto por la API.
# Se usan para validar la estructura de la respuesta sin asumir que
# nunca cambiará (ver punto 18 del enunciado: "la API puede cambiar").
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

# --------------------------------------------------------------------------
# Configuración de la muestra CSV (usada por EA1 y EA2)
# --------------------------------------------------------------------------
SAMPLE_SIZE: int = 20
SAMPLE_RANDOM_STATE: int = 42

# --------------------------------------------------------------------------
# Directorios que deben existir antes de ejecutar cualquiera de los pipelines
# --------------------------------------------------------------------------
REQUIRED_DIRS: tuple[Path, ...] = (RAW_DIR, DB_DIR, PROCESSED_DIR, OUTPUT_DIR)


def ensure_directories() -> None:
    """Crea todos los directorios requeridos por el proyecto si no existen."""
    for directory in REQUIRED_DIRS:
        directory.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------
# Configuración de Spark (EA2)
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

    Centralizado aquí para que tanto ``src/main.py`` (EA1) como
    ``src/preprocessing.py`` (EA2) compartan el mismo formato de mensajes,
    evitando duplicar la configuración de ``logging.basicConfig`` en cada
    script de entrada.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
