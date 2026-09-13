"""
cleaning.py
===========

Operaciones de limpieza y transformación (`Transform`, dentro del
paradigma ELT) sobre un DataFrame de PySpark cargado desde SQLite.

Cada función es independiente, documenta su justificación técnica y
devuelve tanto el DataFrame resultante como un resumen de lo realizado
(cuando aplica), para que `preprocessing.py` pueda construir el reporte
de auditoría con cifras reales, nunca inventadas.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType, StringType

from src.config import (
    CRITICAL_FIELDS,
    DESCRIPTIVE_FIELDS,
    IMPORTANT_FIELDS,
    NOT_AVAILABLE_PLACEHOLDER,
    RELEASE_DATE_FORMAT,
    UNKNOWN_PLACEHOLDER,
)

logger = logging.getLogger(__name__)

ID_COLUMN = "id"
RELEASE_DATE_COLUMN = "release_date"


@dataclass
class DeduplicationResult:
    """Resumen de la etapa de eliminación de duplicados."""

    rows_before: int
    full_duplicate_rows_removed: int
    rows_after_full_dedup: int
    id_duplicate_rows_removed: int
    rows_after: int


@dataclass
class NullHandlingResult:
    """Resumen de la etapa de tratamiento de valores nulos."""

    rows_before: int
    rows_dropped_critical_nulls: int
    rows_after: int
    important_fields_imputed: dict[str, int] = field(default_factory=dict)
    descriptive_fields_imputed: dict[str, int] = field(default_factory=dict)


@dataclass
class TypeCorrectionResult:
    """Resumen de la etapa de corrección de tipos de datos."""

    release_date_values_before: int
    release_date_values_parsed: int
    release_date_values_invalidated: int


# --------------------------------------------------------------------------
# 1. Eliminación de duplicados
# --------------------------------------------------------------------------
def deduplicate(df: DataFrame, id_column: str = ID_COLUMN) -> tuple[DataFrame, DeduplicationResult]:
    """
    Elimina duplicados en dos niveles:

    1. Duplicados completos (todas las columnas idénticas): es el caso más
       obvio de un registro repetido sin ambigüedad.
    2. Duplicados por `id`: se usa `id` como clave lógica del juego, ya que
       es el identificador estable que la propia API asigna a cada
       videojuego. Si dos filas comparten `id` pero difieren en otro
       campo, se conserva una única versión (Spark selecciona una fila de
       forma determinística por grupo mediante `dropDuplicates`).

    Args:
        df: DataFrame de entrada (recién cargado desde SQLite).
        id_column: Nombre de la columna que actúa como clave lógica.

    Returns:
        Tupla (DataFrame deduplicado, resumen de la operación).
    """
    rows_before = df.count()

    full_dedup_df = df.dropDuplicates()
    rows_after_full_dedup = full_dedup_df.count()
    full_duplicate_rows_removed = rows_before - rows_after_full_dedup

    if id_column in df.columns:
        id_dedup_df = full_dedup_df.dropDuplicates(subset=[id_column])
    else:
        id_dedup_df = full_dedup_df

    rows_after = id_dedup_df.count()
    id_duplicate_rows_removed = rows_after_full_dedup - rows_after

    result = DeduplicationResult(
        rows_before=rows_before,
        full_duplicate_rows_removed=full_duplicate_rows_removed,
        rows_after_full_dedup=rows_after_full_dedup,
        id_duplicate_rows_removed=id_duplicate_rows_removed,
        rows_after=rows_after,
    )

    logger.info(
        "Duplicados eliminados: %d completos, %d por id (quedan %d registros)",
        full_duplicate_rows_removed,
        id_duplicate_rows_removed,
        rows_after,
    )

    return id_dedup_df, result


# --------------------------------------------------------------------------
# 2. Normalización de texto (incluye variables categóricas)
# --------------------------------------------------------------------------
def normalize_text_columns(df: DataFrame) -> DataFrame:
    """
    Normaliza todas las columnas de texto del DataFrame:

    - Recorta espacios al inicio y al final (`trim`).
    - Colapsa espacios internos múltiples a uno solo (evita
      inconsistencias como `"PC   (Windows)"`).
    - Convierte cadenas vacías (tras el recorte) en `NULL`, para que el
      tratamiento de nulos posterior las maneje de forma unificada, en
      vez de tener "nulos ocultos" como cadenas vacías.

    Esta misma normalización resuelve tanto la limpieza general de texto
    (títulos, descripciones, URLs) como la normalización de las variables
    categóricas (`genre`, `platform`, `publisher`, `developer`), ya que el
    problema típico en ambos casos es el mismo: espacios sobrantes.
    No se altera la semántica original (no se cambia mayúsculas/minúsculas
    ni se reescriben valores), solo se estandariza el formato.

    Args:
        df: DataFrame de entrada.

    Returns:
        Nuevo DataFrame con las columnas de texto normalizadas.
    """
    string_columns = [c for c, t in df.dtypes if t == "string"]

    result_df = df
    for column in string_columns:
        cleaned = F.trim(F.regexp_replace(F.col(column), r"\s+", " "))
        result_df = result_df.withColumn(
            column,
            F.when(cleaned == "", None).otherwise(cleaned),
        )

    logger.info("Normalización de texto aplicada a %d columnas: %s", len(string_columns), string_columns)
    return result_df


# --------------------------------------------------------------------------
# 3. Tratamiento de valores nulos
# --------------------------------------------------------------------------
def handle_nulls(df: DataFrame) -> tuple[DataFrame, NullHandlingResult]:
    """
    Aplica una estrategia de tratamiento de nulos diferenciada según la
    importancia de cada campo (debe ejecutarse DESPUÉS de
    `normalize_text_columns`, para que las cadenas vacías ya se hayan
    convertido en `NULL`):

    - Campos críticos (`id`, `title`): sin ellos el registro no es
      utilizable ni identificable, por lo que la fila se elimina.
    - Campos importantes (`genre`, `platform`, `publisher`, `developer`):
      se imputan con el valor explícito `"Unknown"`. Se prefiere esto a
      eliminar la fila completa (se perdería información válida de otras
      columnas) y a inventar un valor plausible (violaría la integridad
      de los datos).
    - Campos descriptivos (`thumbnail`, `short_description`, `game_url`,
      `freetogame_profile_url`): se imputan con `"Not Available"`, ya que
      son campos de apoyo (no estructurales) cuya ausencia no impide
      analizar el resto del registro, pero conviene dejar explícita la
      ausencia en vez de un valor nulo silencioso en el CSV final.

    Args:
        df: DataFrame ya normalizado (ver `normalize_text_columns`).

    Returns:
        Tupla (DataFrame sin nulos problemáticos, resumen de la operación).
    """
    rows_before = df.count()

    critical_present = [c for c in CRITICAL_FIELDS if c in df.columns]
    if critical_present:
        condition = None
        for c in critical_present:
            col_is_null = F.col(c).isNull()
            condition = col_is_null if condition is None else (condition | col_is_null)
        rows_with_critical_nulls = df.filter(condition).count()
        df = df.filter(~condition)
    else:
        rows_with_critical_nulls = 0

    rows_after_critical = df.count()

    important_imputed: dict[str, int] = {}
    for column in IMPORTANT_FIELDS:
        if column not in df.columns:
            continue
        null_count = df.filter(F.col(column).isNull()).count()
        if null_count > 0:
            df = df.withColumn(
                column, F.when(F.col(column).isNull(), UNKNOWN_PLACEHOLDER).otherwise(F.col(column))
            )
        important_imputed[column] = null_count

    descriptive_imputed: dict[str, int] = {}
    for column in DESCRIPTIVE_FIELDS:
        if column not in df.columns:
            continue
        null_count = df.filter(F.col(column).isNull()).count()
        if null_count > 0:
            df = df.withColumn(
                column,
                F.when(F.col(column).isNull(), NOT_AVAILABLE_PLACEHOLDER).otherwise(F.col(column)),
            )
        descriptive_imputed[column] = null_count

    result = NullHandlingResult(
        rows_before=rows_before,
        rows_dropped_critical_nulls=rows_with_critical_nulls,
        rows_after=rows_after_critical,
        important_fields_imputed=important_imputed,
        descriptive_fields_imputed=descriptive_imputed,
    )

    logger.info(
        "Tratamiento de nulos: %d filas eliminadas por campos críticos, imputaciones importantes=%s, descriptivas=%s",
        rows_with_critical_nulls,
        important_imputed,
        descriptive_imputed,
    )

    return df, result


# --------------------------------------------------------------------------
# 4. Corrección de tipos de datos
# --------------------------------------------------------------------------
def correct_types(df: DataFrame) -> tuple[DataFrame, TypeCorrectionResult]:
    """
    Aplica conversiones explícitas de tipo:

    - `id` -> `IntegerType`
    - Campos de texto -> `StringType` (ya lo son en la mayoría de los
      casos al provenir de SQLite/Pandas, pero se fuerza el tipo de forma
      explícita para que el esquema quede documentado y sea robusto ante
      cambios menores en el origen de datos).
    - `release_date` -> `DateType`, usando el formato ISO (`yyyy-MM-dd`)
      en el que la API y SQLite entregan la fecha. Los valores que no
      logren convertirse (formato inesperado) quedan como `NULL` en vez
      de provocar un error, y la cantidad de conversiones fallidas queda
      registrada para la auditoría.

    Args:
        df: DataFrame de entrada (después de limpieza de nulos).

    Returns:
        Tupla (DataFrame con tipos corregidos, resumen de la conversión
        de fechas).
    """
    result_df = df

    if ID_COLUMN in result_df.columns:
        result_df = result_df.withColumn(ID_COLUMN, F.col(ID_COLUMN).cast(IntegerType()))

    string_target_columns = [
        c for c in result_df.columns if c not in (ID_COLUMN, RELEASE_DATE_COLUMN)
    ]
    for column in string_target_columns:
        result_df = result_df.withColumn(column, F.col(column).cast(StringType()))

    release_date_before = 0
    release_date_parsed = 0
    release_date_invalidated = 0

    if RELEASE_DATE_COLUMN in result_df.columns:
        release_date_before = result_df.filter(F.col(RELEASE_DATE_COLUMN).isNotNull()).count()

        result_df = result_df.withColumn(
            RELEASE_DATE_COLUMN,
            F.to_date(F.col(RELEASE_DATE_COLUMN).cast(StringType()), RELEASE_DATE_FORMAT),
        )

        release_date_parsed = result_df.filter(F.col(RELEASE_DATE_COLUMN).isNotNull()).count()
        release_date_invalidated = release_date_before - release_date_parsed

    result = TypeCorrectionResult(
        release_date_values_before=release_date_before,
        release_date_values_parsed=release_date_parsed,
        release_date_values_invalidated=release_date_invalidated,
    )

    logger.info(
        "Corrección de tipos aplicada. release_date: %d válidas, %d invalidadas por formato",
        release_date_parsed,
        release_date_invalidated,
    )

    return result_df, result


# --------------------------------------------------------------------------
# 5. Columna derivada: release_year
# --------------------------------------------------------------------------
def add_release_year(df: DataFrame) -> DataFrame:
    """
    Agrega la columna derivada `release_year`, extraída de `release_date`
    (ya convertida a `DateType` por `correct_types`).

    Justificación: contar con el año de lanzamiento como columna propia
    facilita análisis agregados en actividades futuras (por ejemplo,
    "juegos publicados por año") sin tener que volver a parsear la fecha
    completa cada vez. Se agrega únicamente si `release_date` existe y ya
    es de tipo fecha.

    Args:
        df: DataFrame con `release_date` de tipo `DateType`.

    Returns:
        DataFrame con la columna `release_year` agregada (o el mismo
        DataFrame si `release_date` no está disponible).
    """
    if RELEASE_DATE_COLUMN not in df.columns:
        return df

    if not isinstance(df.schema[RELEASE_DATE_COLUMN].dataType, DateType):
        logger.warning(
            "No se agrega 'release_year': '%s' no es de tipo DateType todavía.",
            RELEASE_DATE_COLUMN,
        )
        return df

    return df.withColumn("release_year", F.year(F.col(RELEASE_DATE_COLUMN)))


# --------------------------------------------------------------------------
# 6. Análisis de outliers
# --------------------------------------------------------------------------
def analyze_outliers(df: DataFrame) -> str:
    """
    Analiza variables numéricas continuas en busca de outliers mediante
    el método IQR (rango intercuartílico).

    En el dataset de FreeToGame, la única columna numérica es `id`, que es
    un identificador secuencial asignado por la API, no una métrica
    continua (no representa una magnitud como precio, duración o
    calificación). Por lo tanto, no tiene sentido estadístico buscar
    "outliers" en `id`: un id numéricamente alto no es un dato anómalo,
    simplemente fue asignado más tarde. Este criterio se aplica de forma
    explícita en vez de reportar falsos outliers.

    Si en el futuro el dataset incorpora variables numéricas continuas
    (por ejemplo, precio o rating), esta función ya aplicaría el método
    IQR automáticamente sobre ellas.

    Args:
        df: DataFrame ya limpio y con tipos corregidos.

    Returns:
        Texto explicativo del resultado del análisis de outliers.
    """
    numeric_types = {"int", "bigint", "double", "float", "smallint"}
    candidate_columns = [
        c for c, t in df.dtypes if t in numeric_types and c not in (ID_COLUMN,)
    ]

    if not candidate_columns:
        return (
            "No se identifican variables numéricas continuas apropiadas para aplicar\n"
            "un análisis clásico de outliers mediante IQR o Z-Score. El único campo\n"
            "numérico disponible ('id') es un identificador secuencial asignado por la\n"
            "API, no una métrica continua, por lo que no se le aplica este análisis."
        )

    lines = ["Análisis de outliers (método IQR) sobre variables numéricas continuas:"]
    for column in candidate_columns:
        quantiles = df.approxQuantile(column, [0.25, 0.75], 0.01)
        if len(quantiles) != 2:
            continue
        q1, q3 = quantiles
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        outlier_count = df.filter(
            (F.col(column) < lower_bound) | (F.col(column) > upper_bound)
        ).count()
        lines.append(
            f"  - {column}: Q1={q1:.2f}, Q3={q3:.2f}, IQR={iqr:.2f}, "
            f"límites=[{lower_bound:.2f}, {upper_bound:.2f}], outliers detectados={outlier_count}"
        )

    return "\n".join(lines)
