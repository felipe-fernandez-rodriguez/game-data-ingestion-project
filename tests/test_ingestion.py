"""
test_ingestion.py
==================

Pruebas básicas del pipeline EA1 - Ingestión de Datos desde un API.

Las pruebas que involucran la API utilizan mocks (unittest.mock) para no
depender de la red ni de la disponibilidad del servicio externo durante
la ejecución de la suite (por ejemplo, en GitHub Actions).
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.audit import build_audit_result, write_audit_report
from src.database import (
    count_games,
    create_schema,
    fetch_all_games,
    get_connection,
    insert_games,
)
from src.extract import ExtractionError, fetch_games, save_raw_response
from src.generate_sample import generate_csv_sample

# --------------------------------------------------------------------------
# Datos de prueba (fixtures)
# --------------------------------------------------------------------------

SAMPLE_GAMES = [
    {
        "id": 1,
        "title": "Mock Game One",
        "thumbnail": "https://example.com/thumb1.jpg",
        "short_description": "Un juego de prueba con acentos: áéíóú y ñ.",
        "game_url": "https://example.com/games/mock-one",
        "genre": "MMORPG",
        "platform": "PC (Windows)",
        "publisher": "Mock Publisher",
        "developer": "Mock Developer",
        "release_date": "2020-01-15",
        "freetogame_profile_url": "https://example.com/mock-one",
    },
    {
        "id": 2,
        "title": "Mock Game Two",
        "thumbnail": "https://example.com/thumb2.jpg",
        "short_description": None,
        "game_url": "https://example.com/games/mock-two",
        "genre": "Shooter",
        "platform": "Browser",
        "publisher": "Mock Publisher 2",
        "developer": "Mock Developer 2",
        "release_date": "2021-06-01",
        "freetogame_profile_url": "https://example.com/mock-two",
    },
    {
        "id": 3,
        "title": "Mock Game Three",
        "thumbnail": "https://example.com/thumb3.jpg",
        "short_description": "Tercer juego de prueba.",
        "game_url": "https://example.com/games/mock-three",
        "genre": "Strategy",
        "platform": "PC (Windows)",
        "publisher": "Mock Publisher 3",
        "developer": "Mock Developer 3",
        "release_date": "2019-11-20",
        "freetogame_profile_url": "https://example.com/mock-three",
    },
]


def _mock_response(json_data, status_code: int = 200) -> MagicMock:
    """Crea un mock de requests.Response con el JSON y código dados."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.reason = "OK" if status_code == 200 else "Error"
    mock_resp.json.return_value = json_data
    return mock_resp


@pytest.fixture()
def db_connection(tmp_path: Path):
    """Provee una base de datos SQLite temporal con el esquema ya creado."""
    db_path = tmp_path / "games_test.db"
    connection = get_connection(db_path)
    create_schema(connection)
    yield connection
    connection.close()


# --------------------------------------------------------------------------
# 1. La API devuelve una estructura válida
# --------------------------------------------------------------------------

def test_fetch_games_returns_valid_structure():
    with patch("src.extract.requests.get", return_value=_mock_response(SAMPLE_GAMES)):
        games = fetch_games(url="https://fake-api.test/games")

    assert isinstance(games, list)
    assert len(games) == 3
    assert all("id" in game and "title" in game for game in games)


def test_fetch_games_raises_on_invalid_structure():
    with patch("src.extract.requests.get", return_value=_mock_response({"error": "not a list"})):
        with pytest.raises(ExtractionError):
            fetch_games(url="https://fake-api.test/games")


def test_fetch_games_raises_on_bad_status_code():
    with patch("src.extract.requests.get", return_value=_mock_response(SAMPLE_GAMES, status_code=500)):
        with pytest.raises(ExtractionError):
            fetch_games(url="https://fake-api.test/games")


def test_save_raw_response_writes_json(tmp_path: Path):
    output_path = tmp_path / "raw" / "games.json"
    save_raw_response(SAMPLE_GAMES, output_path)

    assert output_path.exists()
    saved = json.loads(output_path.read_text(encoding="utf-8"))
    assert len(saved) == len(SAMPLE_GAMES)


# --------------------------------------------------------------------------
# 2 y 3. La base de datos y la tabla `games` se crean correctamente
# --------------------------------------------------------------------------

def test_database_and_table_are_created(tmp_path: Path):
    db_path = tmp_path / "games.db"
    connection = get_connection(db_path)
    create_schema(connection)

    cursor = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='games';"
    )
    table = cursor.fetchone()
    connection.close()

    assert db_path.exists()
    assert table is not None
    assert table["name"] == "games"


# --------------------------------------------------------------------------
# 4. El número de registros insertados es mayor que cero
# --------------------------------------------------------------------------

def test_insert_games_inserts_records(db_connection):
    inserted = insert_games(db_connection, SAMPLE_GAMES)
    total = count_games(db_connection)

    assert inserted == len(SAMPLE_GAMES)
    assert total > 0
    assert total == len(SAMPLE_GAMES)


def test_insert_games_is_idempotent(db_connection):
    insert_games(db_connection, SAMPLE_GAMES)
    insert_games(db_connection, SAMPLE_GAMES)  # segunda ejecución, mismos datos

    total = count_games(db_connection)
    assert total == len(SAMPLE_GAMES)  # no se duplican registros


# --------------------------------------------------------------------------
# 5. El archivo CSV es generado
# --------------------------------------------------------------------------

def test_generate_csv_sample_creates_file(db_connection, tmp_path: Path):
    insert_games(db_connection, SAMPLE_GAMES)
    csv_path = tmp_path / "output" / "games_sample.csv"

    result = generate_csv_sample(db_connection, csv_path, sample_size=2, random_state=1)

    assert csv_path.exists()
    assert result.total_records == len(SAMPLE_GAMES)
    assert result.sample_size == 2
    assert "title" in result.columns


# --------------------------------------------------------------------------
# 6. El archivo TXT de auditoría es generado
# --------------------------------------------------------------------------

def test_write_audit_report_creates_file(db_connection, tmp_path: Path):
    insert_games(db_connection, SAMPLE_GAMES)
    db_games = fetch_all_games(db_connection)
    audit_path = tmp_path / "output" / "audit_report.txt"

    result = write_audit_report(SAMPLE_GAMES, db_games, audit_path)

    assert audit_path.exists()
    content = audit_path.read_text(encoding="utf-8")
    assert "AUDITORÍA DE INGESTIÓN" in content
    assert result.is_successful is True
    assert "VALIDACIÓN EXITOSA" in content


# --------------------------------------------------------------------------
# 7. La auditoría detecta correctamente una diferencia sencilla
# --------------------------------------------------------------------------

def test_audit_detects_missing_record():
    api_games = SAMPLE_GAMES
    db_games = SAMPLE_GAMES[:-1]  # simula que falta el último registro en SQLite

    result = build_audit_result(api_games, db_games)

    assert result.count_matches is False
    assert result.is_successful is False
    assert SAMPLE_GAMES[-1]["id"] in result.missing_in_db


def test_audit_detects_field_difference():
    api_games = SAMPLE_GAMES
    db_games = [dict(g) for g in SAMPLE_GAMES]
    db_games[0]["publisher"] = "Publisher Distinto"  # diferencia deliberada

    result = build_audit_result(api_games, db_games)

    assert result.count_matches is True
    assert result.is_successful is False
    assert len(result.records_with_differences) == 1
    assert result.records_with_differences[0]["id"] == SAMPLE_GAMES[0]["id"]


def test_audit_reports_success_when_data_matches():
    api_games = SAMPLE_GAMES
    db_games = [dict(g) for g in SAMPLE_GAMES]

    result = build_audit_result(api_games, db_games)

    assert result.is_successful is True
    assert result.missing_in_db == set()
    assert result.extra_in_db == set()
    assert result.records_with_differences == []
