"""
test_enrichment.py
====================

Pruebas de la Actividad 3 (enriquecimiento con GamerPower y MMOBomb).

Las llamadas HTTP se simulan con mocks (igual que en `test_ingestion.py`
para EA1), para no depender de la red ni de la disponibilidad de las
APIs externas durante la ejecución de la suite. Se usan datasets
pequeños creados específicamente para cada prueba.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src import gamerpower_client, mmobomb_client
from src.enrichment import (
    EnrichmentError,
    add_comparison_columns,
    ensure_base_dataset_exists,
    left_join_on_title,
    load_base_dataframe,
    prepare_gamerpower_dataframe,
    prepare_mmobomb_dataframe,
)
from src.enrichment_audit import render_enrichment_report
from src.gamerpower_client import GamerPowerClientError
from src.generate_sample import write_csv_sample
from src.mmobomb_client import MMOBombClientError
from src.spark_session import get_spark_session, pandas_to_spark, stop_spark_session
from src.title_matching import add_title_match_column, normalize_title, resolve_duplicate_titles

# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------

BASE_ROWS = [
    {
        "id": 1,
        "title": "World of Tanks",
        "thumbnail": "http://x/1.jpg",
        "short_description": "Tank battles online.",
        "game_url": "http://x/1",
        "genre": "MMO",
        "platform": "PC (Windows)",
        "publisher": "Wargaming",
        "developer": "Wargaming Group",
        "release_date": "2011-04-12",
        "freetogame_profile_url": "http://x/p1",
        "release_year": 2011,
    },
    {
        "id": 2,
        "title": "Sausage Hunter",
        "thumbnail": "http://x/2.jpg",
        "short_description": "A quirky arcade game.",
        "game_url": "http://x/2",
        "genre": "Arcade",
        "platform": "PC (Windows)",
        "publisher": "Indie Pub",
        "developer": "Indie Dev",
        "release_date": "2020-06-01",
        "freetogame_profile_url": "http://x/p2",
        "release_year": 2020,
    },
    {
        "id": 3,
        "title": "Minecraft Clone",
        "thumbnail": "http://x/3.jpg",
        "short_description": "Build and explore.",
        "game_url": "http://x/3",
        "genre": "Sandbox",
        "platform": "Web Browser",
        "publisher": "Pub C",
        "developer": "Dev C",
        "release_date": "2015-09-20",
        "freetogame_profile_url": "http://x/p3",
        "release_year": 2015,
    },
]

GAMERPOWER_RECORDS = [
    {
        "id": 501,
        "title": "World of Tanks",
        "worth": "$19.99",
        "type": "loot",
        "platforms": "PC",
        "status": "active",
        "users": 500,
        "published_date": "2026-01-01",
        "end_date": "N/A",
        "gamerpower_url": "http://gp/501",
    },
    {
        "id": 502,
        "title": "Sausage Hunter (Indiegala) Giveaway",
        "worth": "N/A",
        "type": "game",
        "platforms": "PC",
        "status": "active",
        "users": 200,
        "published_date": "2026-02-01",
        "end_date": "N/A",
        "gamerpower_url": "http://gp/502",
    },
]

MMOBOMB_RECORDS = [
    {
        "id": 9001,
        "title": "World of Tanks",
        "thumbnail": "http://mb/1.jpg",
        "short_description": "MMO tank battles.",
        "game_url": "http://mb/1",
        "genre": "MMO",
        "platform": "PC (Windows)",
        "publisher": "Wargaming",
        "developer": "Wargaming",  # deliberadamente distinto a 'Wargaming Group' (para probar developer_match=False)
        "release_date": "2011-04-12",
        "profile_url": "http://mb/p1",
    },
]


def _mock_response(json_data, status_code: int = 200) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.reason = "OK" if status_code == 200 else "Error"
    mock_resp.json.return_value = json_data
    return mock_resp


@pytest.fixture(scope="module")
def spark():
    session = get_spark_session(app_name="EA3-Tests")
    yield session
    stop_spark_session(session)


@pytest.fixture()
def base_csv(tmp_path: Path) -> Path:
    path = tmp_path / "games_cleaned.csv"
    pd.DataFrame(BASE_ROWS).to_csv(path, index=False)
    return path


@pytest.fixture()
def empty_mapping_df() -> pd.DataFrame:
    from src.title_matching import MAPPING_COLUMNS

    return pd.DataFrame(columns=MAPPING_COLUMNS)


# --------------------------------------------------------------------------
# 1 y 2. Descarga correcta de GamerPower y MMOBomb
# --------------------------------------------------------------------------
def test_gamerpower_download_and_raw_file(tmp_path: Path):
    output_path = tmp_path / "gamerpower" / "giveaways.json"
    with patch("src.gamerpower_client.requests.get", return_value=_mock_response(GAMERPOWER_RECORDS)):
        records = gamerpower_client.download_giveaways(output_path=output_path)

    assert len(records) == 2
    assert output_path.exists()  # 4. existencia de archivo RAW
    assert json.loads(output_path.read_text(encoding="utf-8")) == GAMERPOWER_RECORDS  # RAW intacto


def test_mmobomb_download_and_raw_file(tmp_path: Path):
    output_path = tmp_path / "mmobomb" / "games.json"
    with patch("src.mmobomb_client.requests.get", return_value=_mock_response(MMOBOMB_RECORDS)):
        records = mmobomb_client.download_games(output_path=output_path)

    assert len(records) == 1
    assert output_path.exists()
    assert json.loads(output_path.read_text(encoding="utf-8")) == MMOBOMB_RECORDS


# --------------------------------------------------------------------------
# 3. Validación de JSON (estructura inválida / corrupta)
# --------------------------------------------------------------------------
def test_gamerpower_rejects_invalid_structure():
    with patch("src.gamerpower_client.requests.get", return_value=_mock_response({"unexpected": "shape"})):
        with pytest.raises(GamerPowerClientError):
            gamerpower_client.download_giveaways()


def test_mmobomb_rejects_bad_status_code():
    with patch(
        "src.mmobomb_client.requests.get", return_value=_mock_response(MMOBOMB_RECORDS, status_code=500)
    ):
        with pytest.raises(MMOBombClientError):
            mmobomb_client.download_games()


# --------------------------------------------------------------------------
# 5. Carga correcta del dataset base
# --------------------------------------------------------------------------
def test_load_base_dataframe(base_csv: Path):
    df = load_base_dataframe(base_csv)

    assert len(df) == len(BASE_ROWS)
    assert "title_match" in df.columns
    assert df["title_match"].tolist() == ["world of tanks", "sausage hunter", "minecraft clone"]
    assert df["title"].tolist() == [row["title"] for row in BASE_ROWS]  # title original intacto


def test_ensure_base_dataset_exists_raises_when_missing(tmp_path: Path):
    with pytest.raises(EnrichmentError):
        ensure_base_dataset_exists(tmp_path / "no_existe.csv")


# --------------------------------------------------------------------------
# 6. Generación correcta de title_match
# --------------------------------------------------------------------------
def test_normalize_title_strips_gamerpower_noise():
    assert normalize_title("Sausage Hunter (Indiegala) Giveaway") == "sausage hunter"
    assert normalize_title("World of Tanks") == "world of tanks"
    assert normalize_title(None) == ""


# --------------------------------------------------------------------------
# 7 y 8. Detección y resolución de duplicados de las fuentes
# --------------------------------------------------------------------------
def test_resolve_duplicate_titles_keeps_most_recent():
    df = pd.DataFrame(
        {
            "id": [10, 11],
            "title": ["World of Tanks (Steam) Giveaway", "World of Tanks (Epic Games) Giveaway"],
            "published_date": ["2026-01-01", "2026-03-01"],
        }
    )
    df = add_title_match_column(df)

    deduped, summary = resolve_duplicate_titles(df, "gamerpower", priority_column="published_date")

    assert summary.records_before == 2
    assert summary.duplicate_titles == 1
    assert summary.records_after == 1
    assert deduped["id"].iloc[0] == 11  # el más reciente


# --------------------------------------------------------------------------
# 9 y 10. LEFT JOIN correcto + conservación de registros base
# --------------------------------------------------------------------------
def test_left_join_on_title_preserves_base_and_reports_coverage(
    spark, base_csv: Path, empty_mapping_df: pd.DataFrame
):
    base_pdf = load_base_dataframe(base_csv)
    base_df = pandas_to_spark(spark, base_pdf)

    gp_prepared, _, _ = prepare_gamerpower_dataframe(GAMERPOWER_RECORDS, empty_mapping_df)
    gp_df = pandas_to_spark(spark, gp_prepared)

    joined_df, matched, unmatched = left_join_on_title(base_df, gp_df, "gamerpower")

    assert joined_df.count() == base_df.count()  # se conservan los 3 registros base
    assert matched == 2  # World of Tanks y Sausage Hunter coinciden
    assert unmatched == 1  # Minecraft Clone no tiene giveaway
    assert "gamerpower_id" in joined_df.columns


# --------------------------------------------------------------------------
# 11. Ausencia de duplicados finales (pipeline con ambas fuentes)
# --------------------------------------------------------------------------
def test_full_two_source_join_has_no_duplicate_ids(spark, base_csv: Path, empty_mapping_df: pd.DataFrame):
    base_pdf = load_base_dataframe(base_csv)
    base_df = pandas_to_spark(spark, base_pdf)

    gp_prepared, _, _ = prepare_gamerpower_dataframe(GAMERPOWER_RECORDS, empty_mapping_df)
    mb_prepared, _, _ = prepare_mmobomb_dataframe(MMOBOMB_RECORDS, empty_mapping_df)

    gp_df = pandas_to_spark(spark, gp_prepared)
    mb_df = pandas_to_spark(spark, mb_prepared)

    accumulated_df, _, _ = left_join_on_title(base_df, gp_df, "gamerpower")
    accumulated_df, mb_matched, _ = left_join_on_title(accumulated_df, mb_df, "mmobomb")
    accumulated_df = add_comparison_columns(accumulated_df)

    final_records = accumulated_df.count()
    final_unique_ids = accumulated_df.select("id").distinct().count()

    assert final_records == len(BASE_ROWS)
    assert final_unique_ids == final_records
    assert mb_matched == 1  # solo World of Tanks coincide en MMOBomb

    # developer difiere a propósito entre base y MMOBomb -> developer_match debe ser False
    row = accumulated_df.filter(accumulated_df["id"] == 1).collect()[0]
    assert row["developer_match"] is False
    assert row["genre_match"] is True

    # Sin coincidencia MMOBomb -> *_match debe ser None, no False
    row_no_match = accumulated_df.filter(accumulated_df["id"] == 3).collect()[0]
    assert row_no_match["developer_match"] is None


# --------------------------------------------------------------------------
# 12. Generación del dataset enriquecido (CSV)
# --------------------------------------------------------------------------
def test_enriched_csv_is_written(spark, tmp_path: Path, base_csv: Path, empty_mapping_df: pd.DataFrame):
    base_pdf = load_base_dataframe(base_csv)
    base_df = pandas_to_spark(spark, base_pdf)

    gp_prepared, _, _ = prepare_gamerpower_dataframe(GAMERPOWER_RECORDS, empty_mapping_df)
    gp_df = pandas_to_spark(spark, gp_prepared)
    enriched_df, _, _ = left_join_on_title(base_df, gp_df, "gamerpower")

    enriched_pdf = enriched_df.toPandas()
    output_path = tmp_path / "games_enriched.csv"
    enriched_pdf.to_csv(output_path, index=False)

    assert output_path.exists()
    reloaded = pd.read_csv(output_path)
    assert len(reloaded) == len(BASE_ROWS)
    assert "gamerpower_id" in reloaded.columns


# --------------------------------------------------------------------------
# 13. Generación de la muestra
# --------------------------------------------------------------------------
def test_enriched_sample_is_written(spark, tmp_path: Path, base_csv: Path, empty_mapping_df: pd.DataFrame):
    base_pdf = load_base_dataframe(base_csv)
    base_df = pandas_to_spark(spark, base_pdf)
    gp_prepared, _, _ = prepare_gamerpower_dataframe(GAMERPOWER_RECORDS, empty_mapping_df)
    gp_df = pandas_to_spark(spark, gp_prepared)
    enriched_df, _, _ = left_join_on_title(base_df, gp_df, "gamerpower")

    enriched_pdf = enriched_df.toPandas()
    sample_path = tmp_path / "enriched_games_sample.csv"
    result = write_csv_sample(enriched_pdf, sample_path, sample_size=2, random_state=1)

    assert sample_path.exists()
    assert result.sample_size == min(2, len(enriched_pdf))


# --------------------------------------------------------------------------
# 14. Generación del reporte de auditoría
# --------------------------------------------------------------------------
def test_render_enrichment_report_contains_expected_sections():
    from src.enrichment_audit import CardinalityCheckpoint, EnrichmentResult, SourceIntegrationResult
    from src.title_matching import DeduplicationSummary

    gp_dedup = DeduplicationSummary("gamerpower", records_before=2, unique_titles=2, duplicate_titles=0, records_after=2)
    mb_dedup = DeduplicationSummary("mmobomb", records_before=1, unique_titles=1, duplicate_titles=0, records_after=1)

    gp_result = SourceIntegrationResult(
        source_name="gamerpower",
        raw_path=Path("data/raw/gamerpower/giveaways.json"),
        records_downloaded=2,
        unique_titles=2,
        dedup_summary=gp_dedup,
        base_records=3,
        records_matched=2,
        records_unmatched=1,
        new_columns=("gamerpower_id", "gamerpower_worth"),
    )
    mb_result = SourceIntegrationResult(
        source_name="mmobomb",
        raw_path=Path("data/raw/mmobomb/games.json"),
        records_downloaded=1,
        unique_titles=1,
        dedup_summary=mb_dedup,
        base_records=3,
        records_matched=1,
        records_unmatched=2,
        new_columns=("mmobomb_id", "mmobomb_genre"),
    )
    result = EnrichmentResult(
        base_records=3,
        base_columns=13,
        base_unique_ids=3,
        gamerpower=gp_result,
        mmobomb=mb_result,
        homologation_notes=["Nota de ejemplo."],
        cardinality_checkpoints=[
            CardinalityCheckpoint("Dataset base", 3, 3),
            CardinalityCheckpoint("Después de GamerPower", 3, 3),
            CardinalityCheckpoint("Después de MMOBomb", 3, 3),
        ],
        final_records=3,
        final_columns=20,
        final_duplicate_ids=0,
        granularity_preserved=True,
        ambiguous_matches=0,
        consistency_issues=[],
    )

    report_text = render_enrichment_report(base_csv_path=Path("data/processed/games_cleaned.csv"), result=result)

    assert "AUDITORÍA DE ENRIQUECIMIENTO" in report_text
    assert "FUENTES UTILIZADAS" in report_text
    assert "1. DATASET BASE" in report_text
    assert "2. GAMERPOWER" in report_text
    assert "3. MMOBOMB" in report_text
    assert "4. HOMOLOGACIÓN" in report_text
    assert "5. CONTROL DE DUPLICADOS" in report_text
    assert "6. CONTROL DE CARDINALIDAD" in report_text
    assert "7. DATASET ENRIQUECIDO" in report_text
    assert "8. VALIDACIÓN" in report_text
    assert "9. RESULTADO" in report_text
    assert "ENRIQUECIMIENTO EXITOSO" in report_text
    assert "10. DATA LINEAGE" in report_text
