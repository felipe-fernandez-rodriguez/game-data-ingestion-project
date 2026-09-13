"""
test_preprocessing.py
======================

Pruebas de la Actividad 2 (preprocesamiento / ELT con PySpark).

Se usan DataFrames pequeños creados específicamente para cada prueba
(no se consulta la API ni se depende de la ejecución previa de EA1,
salvo en la prueba de extracción, que construye su propia base SQLite
temporal). Una única `SparkSession` se comparte entre todas las pruebas
del módulo para evitar el costo de crear/cerrar Spark repetidamente.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from src.cleaning import (
    add_release_year,
    analyze_outliers,
    correct_types,
    deduplicate,
    handle_nulls,
    normalize_text_columns,
)
from src.database import create_schema, get_connection, insert_games
from src.generate_sample import write_csv_sample
from src.preprocessing import extract_from_sqlite, load_into_spark, render_cleaning_report
from src.profiling import profile_dataframe
from src.spark_session import get_spark_session, stop_spark_session
from src.validation import (
    ValidationError,
    compute_quality_score,
    validate_clean_dataset,
    validate_schema,
)

# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

RAW_RECORDS = [
    {
        "id": 1,
        "title": "  Mock Game One  ",
        "thumbnail": "https://example.com/t1.jpg",
        "short_description": "Descripción de prueba",
        "game_url": "https://example.com/g1",
        "genre": " MMORPG",
        "platform": "PC (Windows)",
        "publisher": "Pub1",
        "developer": "Dev1",
        "release_date": "2020-01-15",
        "freetogame_profile_url": "https://example.com/p1",
    },
    {
        "id": 2,
        "title": "Mock Game Two",
        "thumbnail": None,
        "short_description": "",
        "game_url": "https://example.com/g2",
        "genre": "Shooter",
        "platform": "PC (Windows)  ",
        "publisher": None,
        "developer": "Dev2",
        "release_date": "fecha-invalida",
        "freetogame_profile_url": "https://example.com/p2",
    },
    {
        "id": 2,  # id duplicado a propósito
        "title": "Mock Game Two",
        "thumbnail": None,
        "short_description": "",
        "game_url": "https://example.com/g2",
        "genre": "Shooter",
        "platform": "PC (Windows)",
        "publisher": None,
        "developer": "Dev2",
        "release_date": "fecha-invalida",
        "freetogame_profile_url": "https://example.com/p2",
    },
    {
        "id": None,  # sin id: campo crítico ausente
        "title": None,  # sin title: campo crítico ausente
        "thumbnail": "https://example.com/t4.jpg",
        "short_description": "Registro incompleto",
        "game_url": "https://example.com/g4",
        "genre": "Strategy",
        "platform": "Browser",
        "publisher": "Pub4",
        "developer": "Dev4",
        "release_date": "2019-11-20",
        "freetogame_profile_url": "https://example.com/p4",
    },
]


@pytest.fixture(scope="module")
def spark():
    """SparkSession local compartida entre todas las pruebas del módulo."""
    session = get_spark_session(app_name="EA2-Tests")
    yield session
    stop_spark_session(session)


@pytest.fixture()
def raw_spark_df(spark):
    """DataFrame de Spark construido a partir de RAW_RECORDS, igual que en producción."""
    return load_into_spark(spark, RAW_RECORDS)


# --------------------------------------------------------------------------
# 1. Carga correcta desde SQLite
# --------------------------------------------------------------------------
def test_extract_from_sqlite_reads_existing_records(tmp_path: Path):
    db_path = tmp_path / "games.db"
    connection = get_connection(db_path)
    create_schema(connection)
    insert_games(connection, [r for r in RAW_RECORDS if r["id"] is not None])
    connection.close()

    games = extract_from_sqlite(db_path)

    assert isinstance(games, list)
    assert len(games) > 0
    assert all("id" in g for g in games)


def test_extract_from_sqlite_raises_when_db_missing(tmp_path: Path):
    missing_db = tmp_path / "does_not_exist.db"
    with pytest.raises(ValidationError):
        extract_from_sqlite(missing_db)


def test_extract_from_sqlite_raises_when_table_empty(tmp_path: Path):
    db_path = tmp_path / "empty.db"
    connection = get_connection(db_path)
    create_schema(connection)
    connection.close()

    with pytest.raises(ValidationError):
        extract_from_sqlite(db_path)


# --------------------------------------------------------------------------
# 2. Creación correcta del DataFrame
# --------------------------------------------------------------------------
def test_load_into_spark_creates_dataframe_with_expected_shape(raw_spark_df):
    assert raw_spark_df.count() == len(RAW_RECORDS)
    assert "id" in raw_spark_df.columns
    assert "title" in raw_spark_df.columns


# --------------------------------------------------------------------------
# 3. Detección de duplicados (perfilamiento)
# --------------------------------------------------------------------------
def test_profiling_detects_duplicate_ids(raw_spark_df):
    profile = profile_dataframe(raw_spark_df, label="TEST")

    assert profile.total_records == len(RAW_RECORDS)
    # Dos filas comparten id=2 -> exactamente 1 fila "adicional" por duplicado de id.
    assert profile.duplicate_id_rows == 1


# --------------------------------------------------------------------------
# 4. Eliminación de duplicados
# --------------------------------------------------------------------------
def test_deduplicate_removes_duplicate_ids(raw_spark_df):
    deduped_df, result = deduplicate(raw_spark_df)

    assert result.rows_before == len(RAW_RECORDS)
    assert result.id_duplicate_rows_removed == 1
    ids = [row["id"] for row in deduped_df.select("id").collect()]
    assert len(ids) == len(set(ids))  # sin ids repetidos


# --------------------------------------------------------------------------
# 5. Tratamiento de valores nulos
# --------------------------------------------------------------------------
def test_handle_nulls_drops_critical_and_imputes_others(raw_spark_df):
    deduped_df, _ = deduplicate(raw_spark_df)
    normalized_df = normalize_text_columns(deduped_df)
    clean_df, result = handle_nulls(normalized_df)

    # La fila con id=None/title=None debe eliminarse.
    assert result.rows_dropped_critical_nulls == 1
    assert clean_df.filter(clean_df["id"].isNull()).count() == 0
    assert clean_df.filter(clean_df["title"].isNull()).count() == 0

    # El publisher nulo del registro id=2 debe quedar imputado como "Unknown".
    publisher_values = [row["publisher"] for row in clean_df.filter(clean_df["id"] == 2).collect()]
    assert publisher_values == ["Unknown"]


# --------------------------------------------------------------------------
# 6. Conversión correcta de release_date
# --------------------------------------------------------------------------
def test_correct_types_parses_valid_dates_and_nullifies_invalid(raw_spark_df):
    deduped_df, _ = deduplicate(raw_spark_df)
    normalized_df = normalize_text_columns(deduped_df)
    handled_df, _ = handle_nulls(normalized_df)
    typed_df, type_result = correct_types(handled_df)

    row_1 = typed_df.filter(typed_df["id"] == 1).collect()[0]
    assert row_1["release_date"] == date(2020, 1, 15)

    row_2 = typed_df.filter(typed_df["id"] == 2).collect()[0]
    assert row_2["release_date"] is None  # "fecha-invalida" no es convertible

    assert type_result.release_date_values_invalidated >= 1


def test_add_release_year_uses_parsed_date(raw_spark_df):
    deduped_df, _ = deduplicate(raw_spark_df)
    normalized_df = normalize_text_columns(deduped_df)
    handled_df, _ = handle_nulls(normalized_df)
    typed_df, _ = correct_types(handled_df)
    final_df = add_release_year(typed_df)

    row_1 = final_df.filter(final_df["id"] == 1).collect()[0]
    assert row_1["release_year"] == 2020


def test_analyze_outliers_reports_no_continuous_numeric_variables(raw_spark_df):
    text = analyze_outliers(raw_spark_df)
    assert "No se identifican variables numéricas continuas" in text


# --------------------------------------------------------------------------
# 7. Validación de columnas esperadas
# --------------------------------------------------------------------------
def test_validate_schema_detects_missing_column(spark):
    # Se construye vía Pandas (igual que en producción, ver
    # `load_into_spark`) para pasar por la conversión con Apache Arrow.
    # `spark.createDataFrame()` con una lista de dicts en Python usa una
    # ruta distinta (basada en RDD + cloudpickle) que Arrow no cubre, y
    # que en algunos entornos Windows + Python 3.12 falla con
    # `RecursionError` (ver la nota sobre Arrow en `spark_session.py`).
    pdf = pd.DataFrame([{"id": 1, "title": "Solo estas dos columnas"}], dtype=object)
    incomplete_df = spark.createDataFrame(pdf)
    result = validate_schema(incomplete_df)

    assert result.is_valid is False
    assert "genre" in result.missing_columns


def test_validate_schema_accepts_complete_columns(raw_spark_df):
    result = validate_schema(raw_spark_df)
    assert result.is_valid is True


# --------------------------------------------------------------------------
# 8. Generación del dataset limpio (pipeline de transformación completo)
# --------------------------------------------------------------------------
def test_full_transform_pipeline_produces_clean_dataset(raw_spark_df):
    deduped_df, _ = deduplicate(raw_spark_df)
    normalized_df = normalize_text_columns(deduped_df)
    handled_df, _ = handle_nulls(normalized_df)
    typed_df, _ = correct_types(handled_df)
    clean_df = add_release_year(typed_df)

    validation_result = validate_clean_dataset(clean_df)

    assert validation_result.duplicate_ids == 0
    assert validation_result.critical_nulls == 0
    assert validation_result.id_type_correct is True
    assert validation_result.release_date_type_correct is True
    assert validation_result.is_valid is True


# --------------------------------------------------------------------------
# 9. Generación del CSV de evidencia
# --------------------------------------------------------------------------
def test_write_csv_sample_from_clean_pandas_dataframe(raw_spark_df, tmp_path: Path):
    deduped_df, _ = deduplicate(raw_spark_df)
    normalized_df = normalize_text_columns(deduped_df)
    handled_df, _ = handle_nulls(normalized_df)
    typed_df, _ = correct_types(handled_df)
    clean_df = add_release_year(typed_df)

    clean_pdf: pd.DataFrame = clean_df.toPandas()
    csv_path = tmp_path / "cleaned_games_sample.csv"

    result = write_csv_sample(clean_pdf, csv_path, sample_size=2, random_state=1)

    assert csv_path.exists()
    assert result.sample_size == min(2, len(clean_pdf))


# --------------------------------------------------------------------------
# 10. Generación del reporte TXT
# --------------------------------------------------------------------------
def test_render_cleaning_report_contains_expected_sections(raw_spark_df):
    before_profile = profile_dataframe(raw_spark_df, label="INICIAL")

    deduped_df, dedup_result = deduplicate(raw_spark_df)
    normalized_df = normalize_text_columns(deduped_df)
    handled_df, null_result = handle_nulls(normalized_df)
    typed_df, type_result = correct_types(handled_df)
    clean_df = add_release_year(typed_df)

    after_profile = profile_dataframe(clean_df, label="FINAL")
    final_validation = validate_clean_dataset(clean_df)
    quality_score = compute_quality_score(before_profile, final_validation, after_profile.total_records)
    outliers_text = analyze_outliers(clean_df)

    report_text = render_cleaning_report(
        db_path=Path("data/database/games.db"),
        before_profile=before_profile,
        after_profile=after_profile,
        dedup_result=dedup_result,
        null_result=null_result,
        type_result=type_result,
        outliers_text=outliers_text,
        final_validation=final_validation,
        quality_score=quality_score,
    )

    assert "AUDITORÍA DE PREPROCESAMIENTO Y LIMPIEZA" in report_text
    assert "1. EXTRACCIÓN Y CARGA" in report_text
    assert "2. PERFILAMIENTO INICIAL" in report_text
    assert "3. OPERACIONES DE LIMPIEZA" in report_text
    assert "4. RESULTADOS ANTES Y DESPUÉS" in report_text
    assert "5. VALIDACIÓN FINAL" in report_text
    assert "6. RESULTADO" in report_text
    assert "VALIDACIÓN EXITOSA" in report_text
