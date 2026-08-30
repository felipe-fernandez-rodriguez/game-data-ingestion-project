"""
config.py
=========

Configuración centralizada del proyecto EA1 - Ingestión de Datos desde un API.

Mantener todas las rutas, constantes y parámetros de configuración en un único
módulo evita "números mágicos" y rutas duplicadas a lo largo del código,
y facilita adaptar el proyecto a otros entornos (local, CI/CD, etc.).
"""

from __future__ import annotations

from pathlib import Path

# --------------------------------------------------------------------------
# Rutas base del proyecto (multiplataforma, usando pathlib)
# --------------------------------------------------------------------------
BASE_DIR: Path = Path(__file__).resolve().parent.parent

DATA_DIR: Path = BASE_DIR / "data"
RAW_DIR: Path = DATA_DIR / "raw"
DB_DIR: Path = DATA_DIR / "database"

OUTPUT_DIR: Path = BASE_DIR / "output"

# --------------------------------------------------------------------------
# Archivos generados por el pipeline
# --------------------------------------------------------------------------
RAW_JSON_PATH: Path = RAW_DIR / "games.json"
DB_PATH: Path = DB_DIR / "games.db"
CSV_SAMPLE_PATH: Path = OUTPUT_DIR / "games_sample.csv"
AUDIT_REPORT_PATH: Path = OUTPUT_DIR / "audit_report.txt"

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
# Configuración de la muestra CSV
# --------------------------------------------------------------------------
SAMPLE_SIZE: int = 20
SAMPLE_RANDOM_STATE: int = 42

# --------------------------------------------------------------------------
# Directorios que deben existir antes de ejecutar el pipeline
# --------------------------------------------------------------------------
REQUIRED_DIRS: tuple[Path, ...] = (RAW_DIR, DB_DIR, OUTPUT_DIR)


def ensure_directories() -> None:
    """Crea todos los directorios requeridos por el proyecto si no existen."""
    for directory in REQUIRED_DIRS:
        directory.mkdir(parents=True, exist_ok=True)
