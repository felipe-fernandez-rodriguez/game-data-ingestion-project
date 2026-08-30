"""
main.py
=======

Punto de entrada del pipeline EA1 - Ingestión de Datos desde un API.

Coordina, en orden, las siguientes etapas:

    1. Creación de directorios necesarios.
    2. Consulta a la API FreeToGame.
    3. Persistencia del JSON original (evidencia cruda).
    4. Creación/actualización de la base de datos SQLite.
    5. Inserción idempotente de los registros.
    6. Generación del CSV de muestra con Pandas.
    7. Generación del reporte de auditoría.
    8. Impresión de un resumen final.

Uso:
    python src/main.py

Código de salida:
    0 si el pipeline se ejecutó y la auditoría fue exitosa.
    1 si ocurrió un error durante el pipeline.
    2 si el pipeline se ejecutó pero la auditoría detectó diferencias.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Permite ejecutar este archivo tanto como `python src/main.py` (script)
# como `python -m src.main` (módulo), asegurando que la raíz del proyecto
# esté en sys.path para que los imports `from src....` funcionen en ambos
# casos.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src import config  # noqa: E402
from src.audit import write_audit_report  # noqa: E402
from src.database import (  # noqa: E402
    DatabaseError,
    count_games,
    create_schema,
    fetch_all_games,
    get_connection,
    insert_games,
)
from src.extract import ExtractionError, fetch_games, save_raw_response  # noqa: E402
from src.generate_sample import SampleGenerationError, generate_csv_sample  # noqa: E402

logger = logging.getLogger(__name__)


def configure_logging() -> None:
    """Configura el logging estándar del proyecto."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def run_pipeline() -> int:
    """
    Ejecuta el pipeline completo de ingestión.

    Returns:
        Código de salida del proceso (0 = éxito total, 1 = error,
        2 = ejecución completa pero con diferencias detectadas en auditoría).
    """
    logger.info("Iniciando proceso de ingestión")

    try:
        config.ensure_directories()

        # 1. Extracción
        api_games = fetch_games(config.API_URL)
        save_raw_response(api_games, config.RAW_JSON_PATH)

        # 2. Carga en SQLite
        logger.info("Creando base de datos SQLite")
        connection = get_connection(config.DB_PATH)
        try:
            create_schema(connection)
            insert_games(connection, api_games)

            total_in_db = count_games(connection)
            db_games = fetch_all_games(connection)

            # 3. Muestra CSV
            sample_result = generate_csv_sample(connection, config.CSV_SAMPLE_PATH)
        finally:
            connection.close()

        # 4. Auditoría
        audit_result = write_audit_report(api_games, db_games, config.AUDIT_REPORT_PATH)

        _print_summary(
            api_count=len(api_games),
            db_count=total_in_db,
            sample_size=sample_result.sample_size,
            audit_ok=audit_result.is_successful,
        )

        return 0 if audit_result.is_successful else 2

    except ExtractionError as exc:
        logger.error("Fallo en la etapa de extracción: %s", exc)
        return 1
    except DatabaseError as exc:
        logger.error("Fallo en la etapa de base de datos: %s", exc)
        return 1
    except SampleGenerationError as exc:
        logger.error("Fallo generando el CSV de muestra: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - último recurso, se registra y se sale con error
        logger.exception("Error inesperado durante el pipeline: %s", exc)
        return 1


def _print_summary(api_count: int, db_count: int, sample_size: int, audit_ok: bool) -> None:
    """Imprime un resumen final legible del resultado del pipeline."""
    status = "EXITOSA" if audit_ok else "CON DIFERENCIAS (ver audit_report.txt)"
    logger.info(
        "Resumen final -> API: %d | SQLite: %d | Muestra CSV: %d filas | Auditoría: %s",
        api_count,
        db_count,
        sample_size,
        status,
    )


def main() -> None:
    configure_logging()
    exit_code = run_pipeline()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
