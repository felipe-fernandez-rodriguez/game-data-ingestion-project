"""
extract.py
==========

Responsable de la extracción (etapa "Extract" del pipeline ETL) de datos
desde la API pública de FreeToGame.

Este módulo NO conoce nada sobre SQLite, Pandas ni el resto del pipeline:
su única responsabilidad es obtener y validar los datos crudos desde la
API, y opcionalmente persistir la respuesta original en disco para
trazabilidad.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

from src.config import API_URL, EXPECTED_FIELDS, REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Error genérico durante la extracción de datos desde la API."""


def fetch_games(url: str = API_URL, timeout: int = REQUEST_TIMEOUT_SECONDS) -> list[dict[str, Any]]:
    """
    Consulta la API de FreeToGame y devuelve la lista de juegos.

    Args:
        url: Endpoint de la API a consultar.
        timeout: Tiempo máximo de espera (segundos) para la petición HTTP.

    Returns:
        Lista de diccionarios, cada uno representando un videojuego.

    Raises:
        ExtractionError: si ocurre cualquier problema de red, timeout,
            código HTTP inválido, JSON corrupto o estructura inesperada.
    """
    logger.info("Consultando API FreeToGame: %s", url)

    try:
        response = requests.get(url, timeout=timeout)
    except requests.exceptions.Timeout as exc:
        raise ExtractionError(f"Timeout al consultar la API tras {timeout}s") from exc
    except requests.exceptions.ConnectionError as exc:
        raise ExtractionError(f"Error de conexión al consultar la API: {exc}") from exc
    except requests.exceptions.RequestException as exc:
        raise ExtractionError(f"Error inesperado en la petición HTTP: {exc}") from exc

    _validate_http_status(response)
    payload = _parse_json(response)
    games = _validate_structure(payload)
    _warn_on_unexpected_fields(games)

    logger.info("Registros obtenidos: %d", len(games))
    return games


def _validate_http_status(response: requests.Response) -> None:
    """Valida que el código de estado HTTP sea 200 (éxito)."""
    if response.status_code != 200:
        raise ExtractionError(
            f"Código HTTP inesperado: {response.status_code} - {response.reason}"
        )


def _parse_json(response: requests.Response) -> Any:
    """Parsea el cuerpo de la respuesta como JSON, controlando errores de formato."""
    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise ExtractionError(f"La respuesta de la API no es un JSON válido: {exc}") from exc


def _validate_structure(payload: Any) -> list[dict[str, Any]]:
    """
    Verifica que el payload sea una lista de registros (diccionarios).

    La API puede, en teoría, devolver un objeto de error en vez de una
    lista (por ejemplo, ante un endpoint mal formado). Esta validación
    evita que un payload inesperado provoque fallos silenciosos más
    adelante en el pipeline.
    """
    if not isinstance(payload, list):
        raise ExtractionError(
            f"Se esperaba una lista de juegos, se recibió: {type(payload).__name__}"
        )

    if not payload:
        raise ExtractionError("La API devolvió una lista vacía de juegos.")

    if not all(isinstance(item, dict) for item in payload):
        raise ExtractionError("No todos los elementos de la respuesta son objetos JSON válidos.")

    return payload


def _warn_on_unexpected_fields(games: list[dict[str, Any]]) -> None:
    """
    Informa (sin interrumpir el proceso) si la API agregó o quitó campos
    respecto a los esperados en EXPECTED_FIELDS.

    Esto cumple el requisito de no asumir que los datos son estáticos:
    el proyecto sigue funcionando ante cambios menores, mientras deja
    evidencia clara en el log.
    """
    sample_keys = set(games[0].keys())
    expected = set(EXPECTED_FIELDS)

    missing = expected - sample_keys
    extra = sample_keys - expected

    if missing:
        logger.warning("Campos esperados ausentes en la respuesta de la API: %s", sorted(missing))
    if extra:
        logger.warning("Campos adicionales no documentados detectados en la API: %s", sorted(extra))


def save_raw_response(games: list[dict[str, Any]], path: Path) -> None:
    """
    Guarda la respuesta original (ya parseada) de la API como JSON en disco.

    Conservar la respuesta cruda permite auditar exactamente qué se recibió
    en cada ejecución, independientemente de cómo se haya transformado
    posteriormente para SQLite.

    Args:
        games: Lista de registros de juegos.
        path: Ruta destino del archivo JSON.
    """
    logger.info("Guardando respuesta JSON original en: %s", path)
    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(games, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        raise ExtractionError(f"No fue posible escribir el archivo JSON crudo: {exc}") from exc
