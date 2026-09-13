"""
preprocessing.py
=================

Punto de entrada de la Actividad 2 - Preprocesamiento, Limpieza y ELT.

Esta actividad NO vuelve a consultar la API de FreeToGame. Su fuente de
datos es exclusivamente la base de datos SQLite generada por la
Actividad 1 (`data/database/games.db`), reutilizando las funciones ya
existentes de `database.py` para leerla (sin duplicar lógica de
conexión SQL).

Flujo ELT implementado:

    EXTRACT : SQLite (games.db) -> lista de registros (vía database.py)
    LOAD    : lista de registros -> Pandas -> PySpark DataFrame
    TRANSFORM: perfilamiento, deduplicación, tratamiento de nulos,
               corrección de tipos, normalización de texto, columna
               derivada `release_year`, análisis de outliers y
               validación final.

Uso:
    python src/preprocessing.py

Código de salida:
    0 si el pipeline se ejecutó y la validación final fue exitosa.
    1 si ocurrió un error durante el pipeline (origen de datos ausente,
      fallo de Spark, fallo generando evidencias, etc.).
    2 si el pipeline se ejecutó pero la validación final detectó
      observaciones (por ejemplo, algún id aún duplicado).
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Permite ejecutar este archivo tanto como `python src/preprocessing.py`
# (script) como `python -m src.preprocessing` (módulo).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import DataFrame  # noqa: E402

from src import config  # noqa: E402
from src.cleaning import (  # noqa: E402
    add_release_year,
    analyze_outliers,
    correct_types,
    deduplicate,
    handle_nulls,
    normalize_text_columns,
)
from src.database import DatabaseError, fetch_all_games, get_connection  # noqa: E402
from src.generate_sample import SampleGenerationError, write_csv_sample  # noqa: E402
from src.profiling import ProfileResult, profile_dataframe, render_profile_report  # noqa: E402
from src.spark_session import get_spark_session, stop_spark_session  # noqa: E402
from src.validation import (  # noqa: E402
    FinalValidationResult,
    ValidationError,
    compute_quality_score,
    ensure_database_exists,
    ensure_table_has_records,
    validate_clean_dataset,
    validate_schema,
)

logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------
# EXTRACT
# --------------------------------------------------------------------------
def extract_from_sqlite(db_path: Path) -> list[dict]:
    """
    Etapa EXTRACT del proceso ELT: lee los registros ya almacenados por la
    Actividad 1 en SQLite.

    Reutiliza `database.get_connection` y `database.fetch_all_games` (las
    mismas funciones usadas por EA1) en vez de reimplementar el acceso a
    SQLite, evitando duplicar lógica de conexión entre actividades.

    Args:
        db_path: Ruta a la base de datos generada por la Actividad 1.

    Returns:
        Lista de registros (diccionarios) tal como están en SQLite.

    Raises:
        ValidationError: si la base de datos, la tabla o los registros no existen.
    """
    ensure_database_exists(db_path)

    connection = get_connection(db_path)
    try:
        ensure_table_has_records(connection)
        games = fetch_all_games(connection)
    finally:
        connection.close()

    logger.info("Registros cargados desde SQLite: %d", len(games))
    return games


# --------------------------------------------------------------------------
# LOAD
# --------------------------------------------------------------------------
def load_into_spark(spark, games: list[dict]) -> DataFrame:
    """
    Etapa LOAD del proceso ELT: carga los registros extraídos en un
    DataFrame de PySpark.

    Estrategia elegida: SQLite -> Pandas -> Spark DataFrame.

    Justificación: SQLite no cuenta con un conector JDBC estándar y
    ampliamente soportado como el de motores como PostgreSQL o MySQL, por
    lo que leerlo directamente desde Spark requeriría dependencias
    adicionales. En cambio, `database.py` ya sabe leer la tabla `games` de
    forma correcta y reutilizable (evitando duplicar esa lógica), y Pandas
    ofrece un puente simple, confiable y ampliamente soportado hacia
    `spark.createDataFrame()`. Para el volumen de datos de este proyecto
    (cientos de registros), este paso intermedio no representa un cuello
    de botella.

    Args:
        spark: Sesión Spark activa.
        games: Lista de registros extraídos desde SQLite.

    Returns:
        DataFrame de PySpark con los datos crudos.
    """
    # `dtype=object` es importante: si se deja que Pandas infiera el tipo
    # de cada columna y `id` contuviera algún valor `None` (caso límite,
    # ya que en SQLite `id` es PRIMARY KEY), Pandas convertiría toda la
    # columna a `float64` representando la ausencia como `NaN`. Spark
    # distingue `NaN` de `NULL` en columnas numéricas, por lo que un `id`
    # ausente terminaría como `NaN` en vez de `NULL` y no sería detectado
    # por `isNull()` en las validaciones posteriores. Construyendo el
    # DataFrame con `dtype=object` se preserva el `None` real de Python en
    # cualquier columna, incluida `id`.
    pdf = pd.DataFrame(games, dtype=object)
    pdf = pdf.where(pd.notnull(pdf), None)

    df = spark.createDataFrame(pdf)
    logger.info("DataFrame de Spark creado: %d filas, %d columnas", df.count(), len(df.columns))
    return df


# --------------------------------------------------------------------------
# Reporte de auditoría de limpieza
# --------------------------------------------------------------------------
def render_cleaning_report(
    *,
    db_path: Path,
    before_profile: ProfileResult,
    after_profile: ProfileResult,
    dedup_result,
    null_result,
    type_result,
    outliers_text: str,
    final_validation: FinalValidationResult,
    quality_score,
) -> str:
    """Construye el texto completo de `output/cleaning_report.txt` a partir de resultados reales."""
    timestamp = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    separator = "=" * 52
    subseparator = "-" * 52

    if final_validation.is_valid:
        final_status = "VALIDACIÓN EXITOSA"
    elif after_profile.total_records > 0:
        final_status = "VALIDACIÓN CON OBSERVACIONES"
    else:
        final_status = "VALIDACIÓN FALLIDA"

    lines: list[str] = [
        separator,
        "AUDITORÍA DE PREPROCESAMIENTO Y LIMPIEZA",
        separator,
        "",
        "Proyecto:",
        "Actividad 2 - Preprocesamiento y Limpieza de Datos (ELT con PySpark)",
        "",
        "Fuente:",
        str(db_path),
        "",
        "Dataset:",
        "games",
        "",
        "Fecha y hora de ejecución:",
        timestamp,
        "",
        subseparator,
        "1. EXTRACCIÓN Y CARGA",
        subseparator,
        "",
        f"Registros extraídos: {before_profile.total_records}",
        f"Columnas: {before_profile.total_columns} ({', '.join(before_profile.columns)})",
        f"Fuente: {db_path} (tabla 'games', generada por la Actividad 1)",
        "",
        subseparator,
        "2. PERFILAMIENTO INICIAL",
        subseparator,
        "",
        render_profile_report(before_profile),
        "",
        subseparator,
        "3. OPERACIONES DE LIMPIEZA",
        subseparator,
        "",
        "Eliminación de duplicados:",
        f"  Duplicados completos eliminados: {dedup_result.full_duplicate_rows_removed}",
        f"  Duplicados por id eliminados: {dedup_result.id_duplicate_rows_removed}",
        f"Resultado: {dedup_result.rows_before} -> {dedup_result.rows_after} registros.",
        "",
        "Tratamiento de valores nulos:",
        f"  Filas eliminadas por nulos en campos críticos (id, title): {null_result.rows_dropped_critical_nulls}",
        f"  Campos importantes imputados con 'Unknown': {null_result.important_fields_imputed}",
        f"  Campos descriptivos imputados con 'Not Available': {null_result.descriptive_fields_imputed}",
        f"Resultado: {null_result.rows_before} -> {null_result.rows_after} registros.",
        "",
        "Corrección de tipos:",
        "  id -> IntegerType, campos de texto -> StringType, release_date -> DateType",
        f"Resultado: release_date con {type_result.release_date_values_parsed} valores válidos "
        f"y {type_result.release_date_values_invalidated} invalidados por formato "
        f"(de {type_result.release_date_values_before} valores no nulos originales).",
        "",
        "Normalización de texto:",
        "  Se recortaron espacios y se colapsaron espacios internos en todas las columnas de texto",
        "  (incluye las variables categóricas genre, platform, publisher, developer).",
        "Resultado: valores categóricos normalizados sin alterar su semántica original.",
        "",
        "Tratamiento de fechas:",
        "  release_date convertida a DateType (formato yyyy-MM-dd); se agregó la columna derivada",
        "  'release_year' a partir de la fecha ya validada.",
        f"Resultado: {type_result.release_date_values_parsed} fechas utilizables para análisis por año.",
        "",
        "Análisis de outliers:",
        f"Resultado: {outliers_text}",
        "",
        subseparator,
        "4. RESULTADOS ANTES Y DESPUÉS",
        subseparator,
        "",
        f"{'Métrica':<22}{'Antes':>12}{'Después':>15}",
        subseparator,
        f"{'Registros':<22}{before_profile.total_records:>12}{after_profile.total_records:>15}",
        f"{'Duplicados':<22}{before_profile.duplicate_rows:>12}{after_profile.duplicate_rows:>15}",
        f"{'IDs únicos':<22}{str(before_profile.unique_ids):>12}{str(after_profile.unique_ids):>15}",
        f"{'Valores nulos':<22}{before_profile.total_nulls:>12}{after_profile.total_nulls:>15}",
        "",
        subseparator,
        "5. VALIDACIÓN FINAL",
        subseparator,
        "",
        f"IDs duplicados: {final_validation.duplicate_ids}",
        f"Valores nulos en campos críticos: {final_validation.critical_nulls}",
        f"Tipo de 'id' correcto (IntegerType): {final_validation.id_type_correct}",
        f"Tipo de 'release_date' correcto (DateType): {final_validation.release_date_type_correct}",
        f"Registros válidos finales: {after_profile.total_records}",
        "",
        f"Data Quality Score: {quality_score.score_percent}%",
        f"  Componentes: {quality_score.components}",
        "",
        subseparator,
        "6. RESULTADO",
        subseparator,
        "",
        final_status,
        "",
        separator,
    ]

    if final_validation.issues:
        lines.insert(-2, "Observaciones detectadas:")
        for issue in final_validation.issues:
            lines.insert(-2, f"  - {issue}")
        lines.insert(-2, "")

    return "\n".join(lines)


# --------------------------------------------------------------------------
# Orquestación del pipeline
# --------------------------------------------------------------------------
def run_pipeline() -> int:
    """Ejecuta el pipeline ELT completo de la Actividad 2."""
    logger.info("Iniciando Actividad 2")

    try:
        config.ensure_directories()

        # EXTRACT
        logger.info("Conectando a SQLite")
        games = extract_from_sqlite(config.DB_PATH)

        # LOAD
        logger.info("Inicializando Spark")
        spark = get_spark_session()
        try:
            raw_df = load_into_spark(spark, games)
            validate_schema(raw_df)

            logger.info("Iniciando perfilamiento")
            before_profile = profile_dataframe(raw_df, label="INICIAL")
            logger.info("Duplicados detectados: %d", before_profile.duplicate_rows)
            logger.info("Valores nulos detectados: %d", before_profile.total_nulls)

            # TRANSFORM
            logger.info("Aplicando limpieza")
            df, dedup_result = deduplicate(raw_df)
            df = normalize_text_columns(df)
            df, null_result = handle_nulls(df)

            logger.info("Corrigiendo tipos")
            df, type_result = correct_types(df)
            df = add_release_year(df)
            outliers_text = analyze_outliers(df)

            logger.info("Validando dataset limpio")
            after_profile = profile_dataframe(df, label="FINAL")
            final_validation = validate_clean_dataset(df)
            quality_score = compute_quality_score(before_profile, final_validation, after_profile.total_records)

            clean_pdf = df.toPandas()
        finally:
            stop_spark_session(spark)

        # Evidencias (Pandas, fuera de Spark)
        logger.info("Generando CSV")
        config.CLEANED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        clean_pdf.to_csv(config.CLEANED_CSV_PATH, index=False, encoding="utf-8")

        sample_result = write_csv_sample(clean_pdf, config.CLEANED_SAMPLE_PATH)

        logger.info("Generando auditoría")
        report_text = render_cleaning_report(
            db_path=config.DB_PATH,
            before_profile=before_profile,
            after_profile=after_profile,
            dedup_result=dedup_result,
            null_result=null_result,
            type_result=type_result,
            outliers_text=outliers_text,
            final_validation=final_validation,
            quality_score=quality_score,
        )
        config.CLEANING_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        config.CLEANING_REPORT_PATH.write_text(report_text, encoding="utf-8")

        _print_summary(before_profile, after_profile, sample_result.sample_size, final_validation, quality_score)

        if final_validation.is_valid:
            logger.info("Proceso finalizado correctamente")
            return 0
        return 2

    except ValidationError as exc:
        logger.error("Fallo de validación en la Actividad 2: %s", exc)
        return 1
    except DatabaseError as exc:
        logger.error("Fallo de base de datos en la Actividad 2: %s", exc)
        return 1
    except SampleGenerationError as exc:
        logger.error("Fallo generando evidencias de la Actividad 2: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - último recurso, se registra y se sale con error
        logger.exception("Error inesperado durante la Actividad 2: %s", exc)
        return 1


def _print_summary(before, after, sample_size, final_validation, quality_score) -> None:
    """Imprime un resumen final legible del resultado del pipeline de EA2."""
    status = "EXITOSA" if final_validation.is_valid else "CON OBSERVACIONES (ver cleaning_report.txt)"
    logger.info(
        "Resumen final -> Registros: %d -> %d | Duplicados eliminados: %d | "
        "Nulos: %d -> %d | Muestra CSV: %d filas | Quality Score: %.2f%% | Validación: %s",
        before.total_records,
        after.total_records,
        before.duplicate_rows,
        before.total_nulls,
        after.total_nulls,
        sample_size,
        quality_score.score_percent,
        status,
    )


def main() -> None:
    config.configure_logging()
    exit_code = run_pipeline()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
