"""
validation.py
=============

Validaciones utilizadas en distintos puntos del pipeline de EA2:

1. Validaciones de origen (pre-flight): confirman que la base SQLite de
   la Actividad 1 existe, que la tabla `games` existe y que contiene
   registros, ANTES de iniciar Spark. Esto evita levantar una sesión de
   Spark para luego fallar por falta de datos.
2. Validación de esquema: confirma que el DataFrame cargado contiene (al
   menos) las columnas esperadas según la Actividad 1.
3. Validación final del dataset limpio: confirma que la limpieza logró su
   objetivo (sin ids duplicados, sin nulos en campos críticos, tipos
   correctos).
4. Cálculo de un Data Quality Score simple y reproducible.

Reutiliza las funciones ya existentes de `database.py` (no se abre una
segunda conexión ni se reimplementa la verificación de la tabla).
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DateType, IntegerType

from src.config import CRITICAL_FIELDS, EXPECTED_FIELDS
from src.profiling import ProfileResult

logger = logging.getLogger(__name__)

ID_COLUMN = "id"
RELEASE_DATE_COLUMN = "release_date"


class ValidationError(Exception):
    """Error de validación que debe detener el pipeline de EA2."""


@dataclass
class SchemaValidationResult:
    """Resultado de comparar las columnas del DataFrame contra las esperadas."""

    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.missing_columns


@dataclass
class FinalValidationResult:
    """Resultado de la validación posterior a la limpieza."""

    duplicate_ids: int
    critical_nulls: int
    id_type_correct: bool
    release_date_type_correct: bool
    issues: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.issues


@dataclass
class QualityScoreResult:
    """Data Quality Score simple, basado en cuatro componentes documentados."""

    components: dict[str, float]
    score_percent: float


# --------------------------------------------------------------------------
# 1. Validaciones de origen (pre-flight, antes de iniciar Spark)
# --------------------------------------------------------------------------
def ensure_database_exists(db_path: Path) -> None:
    """Verifica que el archivo de base de datos de la Actividad 1 exista."""
    if not db_path.exists():
        raise ValidationError(
            f"No se encontró la base de datos de la Actividad 1 en '{db_path}'. "
            "Ejecuta primero 'python src/main.py' (EA1) para generarla."
        )


def ensure_table_has_records(connection: sqlite3.Connection, table_name: str = "games") -> int:
    """
    Verifica que la tabla `games` exista en la base de datos y que
    contenga al menos un registro.

    Args:
        connection: Conexión SQLite activa (ver `database.get_connection`).
        table_name: Nombre de la tabla a verificar.

    Returns:
        Cantidad de registros encontrados.

    Raises:
        ValidationError: si la tabla no existe o está vacía.
    """
    cursor = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,)
    )
    if cursor.fetchone() is None:
        raise ValidationError(
            f"La tabla '{table_name}' no existe en la base de datos. "
            "Verifica que la Actividad 1 se haya ejecutado correctamente."
        )

    count_cursor = connection.execute(f"SELECT COUNT(*) AS total FROM {table_name};")
    total = int(count_cursor.fetchone()["total"])

    if total == 0:
        raise ValidationError(
            f"La tabla '{table_name}' existe pero no contiene registros. "
            "Ejecuta nuevamente la Actividad 1 antes de continuar."
        )

    return total


# --------------------------------------------------------------------------
# 2. Validación de esquema
# --------------------------------------------------------------------------
def validate_schema(df: DataFrame, expected_columns: tuple[str, ...] = EXPECTED_FIELDS) -> SchemaValidationResult:
    """
    Compara las columnas del DataFrame recién cargado contra las columnas
    esperadas (definidas centralmente en `config.EXPECTED_FIELDS`, las
    mismas usadas para validar la respuesta de la API en la Actividad 1).

    No es un error fatal tener columnas adicionales; solo se reporta.
    Faltar columnas esperadas sí se marca como inválido, para que
    `preprocessing.py` decida cómo proceder.
    """
    actual_columns = set(df.columns)
    expected = set(expected_columns)

    missing = sorted(expected - actual_columns)
    extra = sorted(actual_columns - expected)

    if missing:
        logger.warning("Columnas esperadas ausentes en el DataFrame cargado: %s", missing)
    if extra:
        logger.info("Columnas adicionales presentes en el DataFrame: %s", extra)

    return SchemaValidationResult(missing_columns=missing, extra_columns=extra)


# --------------------------------------------------------------------------
# 3. Validación final del dataset limpio
# --------------------------------------------------------------------------
def validate_clean_dataset(df: DataFrame) -> FinalValidationResult:
    """
    Valida el dataset ya limpio, confirmando objetivamente que las
    operaciones de limpieza cumplieron su propósito.

    Comprueba:
    - Que no existan ids duplicados.
    - Que los campos críticos (`id`, `title`) no tengan nulos.
    - Que `id` sea de tipo entero y `release_date` de tipo fecha.

    Args:
        df: DataFrame ya limpio (después de deduplicación, tratamiento de
            nulos y corrección de tipos).

    Returns:
        Un `FinalValidationResult` con el detalle de la validación.
    """
    issues: list[str] = []

    total = df.count()
    duplicate_ids = 0
    if ID_COLUMN in df.columns:
        unique_ids = df.select(ID_COLUMN).distinct().count()
        duplicate_ids = total - unique_ids
        if duplicate_ids > 0:
            issues.append(f"Existen {duplicate_ids} filas con 'id' duplicado tras la limpieza.")

    critical_nulls = 0
    for column in CRITICAL_FIELDS:
        if column not in df.columns:
            continue
        null_count = df.filter(F.col(column).isNull()).count()
        critical_nulls += null_count
        if null_count > 0:
            issues.append(f"El campo crítico '{column}' aún tiene {null_count} valores nulos.")

    id_type_correct = True
    if ID_COLUMN in df.columns:
        id_type_correct = isinstance(df.schema[ID_COLUMN].dataType, IntegerType)
        if not id_type_correct:
            issues.append(f"La columna '{ID_COLUMN}' no quedó como IntegerType.")

    release_date_type_correct = True
    if RELEASE_DATE_COLUMN in df.columns:
        release_date_type_correct = isinstance(df.schema[RELEASE_DATE_COLUMN].dataType, DateType)
        if not release_date_type_correct:
            issues.append(f"La columna '{RELEASE_DATE_COLUMN}' no quedó como DateType.")

    return FinalValidationResult(
        duplicate_ids=duplicate_ids,
        critical_nulls=critical_nulls,
        id_type_correct=id_type_correct,
        release_date_type_correct=release_date_type_correct,
        issues=issues,
    )


# --------------------------------------------------------------------------
# 4. Data Quality Score
# --------------------------------------------------------------------------
def compute_quality_score(before: ProfileResult, after: FinalValidationResult, total_after: int) -> QualityScoreResult:
    """
    Calcula un Data Quality Score simple (0-100%), promediando cuatro
    componentes fácilmente reproducibles:

    1. `sin_duplicados`: proporción de registros finales sin `id` duplicado.
    2. `campos_criticos_completos`: proporción de registros finales sin
       nulos en campos críticos (`id`, `title`).
    3. `tipos_correctos`: 1.0 si `id` quedó como entero y `release_date`
       como fecha, 0.5 si solo una de las dos, 0.0 si ninguna.
    4. `reduccion_problemas`: proporción de problemas detectados en el
       perfilamiento inicial (duplicados + nulos) que fueron resueltos.

    La métrica es intencionalmente simple (promedio no ponderado) para
    que sea trivial de explicar y reproducir manualmente a partir del
    reporte de auditoría.

    Args:
        before: Resultado del perfilamiento inicial (datos crudos).
        after: Resultado de la validación final (datos limpios).
        total_after: Cantidad de registros en el dataset limpio.

    Returns:
        Un `QualityScoreResult` con el score final y sus componentes.
    """
    sin_duplicados = 1.0 if total_after == 0 else max(0.0, 1 - (after.duplicate_ids / total_after))
    campos_completos = 1.0 if total_after == 0 else max(0.0, 1 - (after.critical_nulls / total_after))

    tipos_correctos = (
        (1.0 if after.id_type_correct else 0.0) + (1.0 if after.release_date_type_correct else 0.0)
    ) / 2.0

    problemas_iniciales = before.duplicate_rows + before.total_nulls
    problemas_finales = after.duplicate_ids + after.critical_nulls
    if problemas_iniciales == 0:
        reduccion_problemas = 1.0
    else:
        reduccion_problemas = max(0.0, 1 - (problemas_finales / problemas_iniciales))

    components = {
        "sin_ids_duplicados": round(sin_duplicados * 100, 2),
        "campos_criticos_completos": round(campos_completos * 100, 2),
        "tipos_correctos": round(tipos_correctos * 100, 2),
        "reduccion_de_problemas": round(reduccion_problemas * 100, 2),
    }

    score_percent = round(sum(components.values()) / len(components), 2)

    logger.info("Data Quality Score: %.2f%% (componentes=%s)", score_percent, components)

    return QualityScoreResult(components=components, score_percent=score_percent)
