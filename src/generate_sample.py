"""
generate_sample.py
===================

Genera un archivo CSV de muestra a partir de los datos almacenados en
SQLite, utilizando Pandas.

Se prefiere CSV sobre Excel para evitar dependencias adicionales
(por ejemplo, openpyxl) que no aportan valor a esta actividad.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from src.config import SAMPLE_RANDOM_STATE, SAMPLE_SIZE
from src.database import GAME_COLUMNS

logger = logging.getLogger(__name__)


class SampleGenerationError(Exception):
    """Error genérico durante la generación del CSV de muestra."""


@dataclass(frozen=True)
class SampleResult:
    """Resumen del proceso de generación de la muestra CSV."""

    total_records: int
    sample_size: int
    columns: list[str]
    output_path: Path


def generate_csv_sample(
    connection: sqlite3.Connection,
    output_path: Path,
    sample_size: int = SAMPLE_SIZE,
    random_state: int = SAMPLE_RANDOM_STATE,
) -> SampleResult:
    """
    Lee todos los registros de la tabla `games` con `pandas.read_sql_query`
    y genera un CSV con una muestra reproducible de los datos.

    Args:
        connection: Conexión SQLite activa.
        output_path: Ruta destino del CSV.
        sample_size: Cantidad máxima de filas a incluir en la muestra.
        random_state: Semilla para que la muestra sea reproducible.

    Returns:
        Un `SampleResult` con estadísticas del proceso.

    Raises:
        SampleGenerationError: si la lectura de SQLite o la escritura del
            CSV fallan, o si el archivo no queda efectivamente creado.
    """
    logger.info("Generando muestra CSV con Pandas")

    try:
        df = pd.read_sql_query(f"SELECT {', '.join(GAME_COLUMNS)} FROM games", connection)
    except (pd.errors.DatabaseError, sqlite3.Error) as exc:
        raise SampleGenerationError(f"Error leyendo datos desde SQLite con Pandas: {exc}") from exc

    total_records = len(df)
    if total_records == 0:
        raise SampleGenerationError("No hay registros en 'games' para generar la muestra.")

    # Muestra reproducible: si hay menos filas que sample_size, se toman todas.
    effective_size = min(sample_size, total_records)
    sample_df = df.sample(n=effective_size, random_state=random_state).sort_values("id")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        sample_df.to_csv(output_path, index=False, encoding="utf-8")
    except OSError as exc:
        raise SampleGenerationError(f"No fue posible escribir el CSV en '{output_path}': {exc}") from exc

    if not output_path.exists():
        raise SampleGenerationError(
            f"El archivo CSV no fue encontrado tras intentar generarlo: {output_path}"
        )

    logger.info(
        "Muestra CSV generada: %d de %d registros -> %s",
        effective_size,
        total_records,
        output_path,
    )

    return SampleResult(
        total_records=total_records,
        sample_size=effective_size,
        columns=list(df.columns),
        output_path=output_path,
    )
