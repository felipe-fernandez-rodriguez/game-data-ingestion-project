"""
profiling.py
============

Perfilamiento (Data Profiling) de un DataFrame de PySpark.

Este módulo se ejecuta dos veces en la Actividad 2: una vez sobre los
datos "crudos" recién cargados desde SQLite (perfilamiento inicial) y otra
vez sobre el dataset ya limpio (perfilamiento posterior), de modo que el
mismo código produzca ambos lados de la comparación "antes / después"
exigida por la actividad.

Todas las métricas se calculan realmente sobre el DataFrame recibido; no
se generan valores de ejemplo ni estimaciones.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType

from src.config import CATEGORICAL_FIELDS

logger = logging.getLogger(__name__)

ID_COLUMN = "id"
RELEASE_DATE_COLUMN = "release_date"


@dataclass
class ProfileResult:
    """Resultado estructurado de un perfilamiento de datos."""

    label: str
    total_records: int
    total_columns: int
    columns: list[str]
    dtypes: dict[str, str]
    null_counts: dict[str, int] = field(default_factory=dict)
    empty_counts: dict[str, int] = field(default_factory=dict)
    duplicate_rows: int = 0
    unique_ids: Optional[int] = None
    duplicate_id_rows: Optional[int] = None
    id_min: Optional[int] = None
    id_max: Optional[int] = None
    release_date_valid: Optional[int] = None
    release_date_invalid: Optional[int] = None
    categorical_distinct: dict[str, int] = field(default_factory=dict)

    @property
    def total_nulls(self) -> int:
        return sum(self.null_counts.values())

    @property
    def total_empty(self) -> int:
        return sum(self.empty_counts.values())


def _compute_null_counts(df: DataFrame) -> dict[str, int]:
    """Cuenta valores nulos por columna en una sola pasada sobre los datos."""
    exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) for c in df.columns]
    row = df.select(*exprs).collect()[0]
    return {c: int(row[c] or 0) for c in df.columns}


def _compute_empty_string_counts(df: DataFrame, string_columns: list[str]) -> dict[str, int]:
    """Cuenta cadenas vacías (tras recortar espacios) por columna de texto."""
    if not string_columns:
        return {}
    exprs = [
        F.sum(
            F.when(F.col(c).isNotNull() & (F.trim(F.col(c)) == ""), 1).otherwise(0)
        ).alias(c)
        for c in string_columns
    ]
    row = df.select(*exprs).collect()[0]
    return {c: int(row[c] or 0) for c in string_columns}


def _compute_duplicate_rows(df: DataFrame, total_records: int) -> int:
    """
    Cantidad de filas duplicadas por completo (todas las columnas iguales).

    Se calcula como total de filas menos filas distintas.
    """
    distinct_records = df.distinct().count()
    return total_records - distinct_records


def _compute_id_metrics(
    df: DataFrame, total_records: int
) -> tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """
    Calcula, cuando existe la columna `id`:
        unique_ids, duplicate_id_rows, id_min, id_max

    `id` se usa como clave lógica del juego: dos filas con el mismo `id`
    representan el mismo videojuego, sin importar si el resto de las
    columnas difiere.
    """
    if ID_COLUMN not in df.columns:
        return None, None, None, None

    unique_ids = df.select(ID_COLUMN).distinct().count()
    duplicate_id_rows = total_records - unique_ids

    minmax = df.select(
        F.min(ID_COLUMN).alias("id_min"),
        F.max(ID_COLUMN).alias("id_max"),
    ).collect()[0]

    return unique_ids, duplicate_id_rows, minmax["id_min"], minmax["id_max"]


def _compute_release_date_validity(df: DataFrame) -> tuple[Optional[int], Optional[int]]:
    """
    Calcula cuántos valores no nulos de `release_date` son fechas válidas
    en formato ISO (`yyyy-MM-dd`) y cuántos no lo son.

    Funciona tanto si la columna todavía es de tipo `string` (perfilamiento
    inicial) como si ya fue convertida a `DateType` (perfilamiento
    posterior a la limpieza).
    """
    if RELEASE_DATE_COLUMN not in df.columns:
        return None, None

    date_type = df.schema[RELEASE_DATE_COLUMN].dataType

    if isinstance(date_type, DateType):
        non_null = df.filter(F.col(RELEASE_DATE_COLUMN).isNotNull()).count()
        # Si ya es DateType, todo valor no nulo es, por definición, una
        # fecha válida (la conversión inválida ya se resolvió a null).
        return non_null, 0

    non_null_df = df.filter(F.col(RELEASE_DATE_COLUMN).isNotNull())
    non_null_count = non_null_df.count()
    valid_count = non_null_df.filter(
        F.to_date(F.col(RELEASE_DATE_COLUMN), "yyyy-MM-dd").isNotNull()
    ).count()
    invalid_count = non_null_count - valid_count

    return valid_count, invalid_count


def _compute_categorical_distinct(df: DataFrame) -> dict[str, int]:
    """Cuenta valores distintos para cada columna categórica presente en el DataFrame."""
    present = [c for c in CATEGORICAL_FIELDS if c in df.columns]
    return {c: df.select(c).distinct().count() for c in present}


def profile_dataframe(df: DataFrame, label: str = "PERFILAMIENTO") -> ProfileResult:
    """
    Ejecuta el perfilamiento completo de un DataFrame de PySpark.

    Args:
        df: DataFrame a perfilar (crudo o ya limpio).
        label: Etiqueta descriptiva (por ejemplo "INICIAL" o "FINAL"),
            usada únicamente para logging y reportes.

    Returns:
        Un `ProfileResult` con todas las métricas calculadas.
    """
    logger.info("Iniciando perfilamiento: %s", label)

    total_records = df.count()
    columns = list(df.columns)
    dtypes = dict(df.dtypes)
    string_columns = [c for c, t in df.dtypes if t == "string"]

    null_counts = _compute_null_counts(df)
    empty_counts = _compute_empty_string_counts(df, string_columns)
    duplicate_rows = _compute_duplicate_rows(df, total_records)
    unique_ids, duplicate_id_rows, id_min, id_max = _compute_id_metrics(df, total_records)
    release_valid, release_invalid = _compute_release_date_validity(df)
    categorical_distinct = _compute_categorical_distinct(df)

    result = ProfileResult(
        label=label,
        total_records=total_records,
        total_columns=len(columns),
        columns=columns,
        dtypes=dtypes,
        null_counts=null_counts,
        empty_counts=empty_counts,
        duplicate_rows=duplicate_rows,
        unique_ids=unique_ids,
        duplicate_id_rows=duplicate_id_rows,
        id_min=id_min,
        id_max=id_max,
        release_date_valid=release_valid,
        release_date_invalid=release_invalid,
        categorical_distinct=categorical_distinct,
    )

    logger.info(
        "Perfilamiento '%s' -> registros=%d, duplicados=%d, ids_duplicados=%s, nulos_totales=%d",
        label,
        total_records,
        duplicate_rows,
        duplicate_id_rows,
        result.total_nulls,
    )

    return result


def render_profile_report(result: ProfileResult) -> str:
    """Genera el bloque de texto del perfilamiento en el formato solicitado por la actividad."""
    lines: list[str] = [
        f"PERFILAMIENTO {result.label}",
        "=" * (14 + len(result.label)),
        "",
        f"Registros: {result.total_records}",
        f"Columnas: {result.total_columns} ({', '.join(result.columns)})",
        "",
        f"Duplicados (filas completas): {result.duplicate_rows}",
        f"Valores nulos (total): {result.total_nulls}",
        f"Valores vacíos (total, texto): {result.total_empty}",
        "",
    ]

    if result.unique_ids is not None:
        lines.extend(
            [
                "ID:",
                f"IDs únicos: {result.unique_ids}",
                f"IDs duplicados (filas adicionales): {result.duplicate_id_rows}",
                f"Rango de id: {result.id_min} - {result.id_max}",
                "",
            ]
        )

    if result.release_date_valid is not None:
        lines.extend(
            [
                "release_date:",
                f"Valores válidos: {result.release_date_valid}",
                f"Valores inválidos: {result.release_date_invalid}",
                "",
            ]
        )

    for col_name, distinct_count in result.categorical_distinct.items():
        lines.extend([f"{col_name}:", f"Valores distintos: {distinct_count}", ""])

    lines.append("Nulos por columna:")
    for col_name, count in result.null_counts.items():
        if count > 0:
            lines.append(f"  - {col_name}: {count}")
    if not any(result.null_counts.values()):
        lines.append("  Ninguna columna presenta valores nulos.")

    return "\n".join(lines)
