"""
database.py
===========

Responsable de la capa de persistencia (SQLite) del pipeline.

Contiene la creación de la base de datos, la definición del esquema de la
tabla `games` y la lógica de inserción idempotente de registros.
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Error genérico durante operaciones sobre la base de datos SQLite."""


# --------------------------------------------------------------------------
# Esquema de la tabla `games`
# --------------------------------------------------------------------------
# Se analizó la estructura real del JSON devuelto por la API (ver punto 1
# del enunciado) y se diseñaron columnas que reflejan directamente esos
# campos, sin inventar información adicional.
#
# - `id` es la clave primaria natural: la API ya provee un identificador
#   único y estable por juego.
# - `release_date` se almacena como TEXT en formato ISO (YYYY-MM-DD), tal
#   como lo entrega la API, evitando conversiones innecesarias.
# - El resto de los campos son TEXT porque corresponden a cadenas de texto
#   (títulos, URLs, descripciones, etc.).
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS games (
    id                      INTEGER PRIMARY KEY,
    title                   TEXT NOT NULL,
    thumbnail               TEXT,
    short_description       TEXT,
    game_url                TEXT,
    genre                   TEXT,
    platform                TEXT,
    publisher               TEXT,
    developer               TEXT,
    release_date            TEXT,
    freetogame_profile_url  TEXT,
    ingested_at             TEXT NOT NULL DEFAULT (datetime('now'))
);
"""

# Columnas de datos propiamente dichas (excluye la marca de tiempo interna).
GAME_COLUMNS: tuple[str, ...] = (
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

# `INSERT OR REPLACE` se eligió por sobre `INSERT ... ON CONFLICT DO NOTHING`
# porque el objetivo del pipeline es que la base de datos siempre refleje el
# estado MÁS RECIENTE devuelto por la API (por ejemplo, si cambia el
# `short_description` o el `publisher` de un juego). `OR REPLACE` reemplaza
# la fila completa cuando el `id` ya existe, logrando así tanto idempotencia
# (no se generan duplicados) como actualización de datos existentes.
_INSERT_SQL = f"""
INSERT OR REPLACE INTO games ({", ".join(GAME_COLUMNS)})
VALUES ({", ".join(f":{col}" for col in GAME_COLUMNS)});
"""


def get_connection(db_path: Path) -> sqlite3.Connection:
    """
    Abre (o crea) la base de datos SQLite en `db_path`.

    Args:
        db_path: Ruta al archivo .db.

    Returns:
        Conexión sqlite3 abierta.

    Raises:
        DatabaseError: si no es posible abrir/crear el archivo de base de datos.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        connection = sqlite3.connect(db_path)
        connection.row_factory = sqlite3.Row
        return connection
    except sqlite3.Error as exc:
        raise DatabaseError(f"No fue posible abrir la base de datos '{db_path}': {exc}") from exc


def create_schema(connection: sqlite3.Connection) -> None:
    """Crea la tabla `games` si aún no existe."""
    logger.info("Creando/verificando esquema de la base de datos SQLite")
    try:
        with connection:
            connection.execute(CREATE_TABLE_SQL)
    except sqlite3.Error as exc:
        raise DatabaseError(f"Error creando el esquema de la base de datos: {exc}") from exc


def _row_from_game(game: dict[str, Any]) -> dict[str, Any]:
    """
    Construye el diccionario de parámetros para la sentencia INSERT a
    partir de un registro crudo de la API, rellenando con None los
    campos ausentes (maneja campos nulos / API cambiante).
    """
    return {column: game.get(column) for column in GAME_COLUMNS}


def insert_games(connection: sqlite3.Connection, games: list[dict[str, Any]]) -> int:
    """
    Inserta (o actualiza) los registros de juegos en la tabla `games`
    dentro de una única transacción.

    La operación es idempotente: ejecutar el pipeline varias veces con
    los mismos datos no genera filas duplicadas, gracias a `INSERT OR
    REPLACE` sobre la clave primaria `id`.

    Args:
        connection: Conexión SQLite activa.
        games: Lista de registros obtenidos desde la API.

    Returns:
        Número de registros procesados (insertados o actualizados).

    Raises:
        DatabaseError: si ocurre un error durante la transacción. En ese
            caso se realiza rollback y no se confirma ningún cambio.
    """
    rows = [_row_from_game(game) for game in games if game.get("id") is not None]

    skipped = len(games) - len(rows)
    if skipped:
        logger.warning("Se omitieron %d registros sin campo 'id' válido", skipped)

    try:
        with connection:  # commit automático al salir del bloque; rollback si hay excepción
            connection.executemany(_INSERT_SQL, rows)
    except sqlite3.Error as exc:
        raise DatabaseError(f"Error insertando registros en SQLite: {exc}") from exc

    logger.info("Registros insertados/actualizados: %d", len(rows))
    return len(rows)


def count_games(connection: sqlite3.Connection) -> int:
    """Devuelve la cantidad total de registros almacenados en `games`."""
    cursor = connection.execute("SELECT COUNT(*) AS total FROM games;")
    return int(cursor.fetchone()["total"])


def fetch_all_ids(connection: sqlite3.Connection) -> set[int]:
    """Devuelve el conjunto de todos los `id` almacenados en `games`."""
    cursor = connection.execute("SELECT id FROM games;")
    return {row["id"] for row in cursor.fetchall()}


def fetch_all_games(connection: sqlite3.Connection) -> list[dict[str, Any]]:
    """Devuelve todos los registros almacenados en `games` como diccionarios."""
    cursor = connection.execute(
        f"SELECT {', '.join(GAME_COLUMNS)} FROM games;"
    )
    return [dict(row) for row in cursor.fetchall()]
