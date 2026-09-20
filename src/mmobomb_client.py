"""
mmobomb_client.py
===================

Cliente para la API de MMOBomb.

- Documentación: https://www.mmobomb.com/api
- Endpoint usado: https://www.mmobomb.com/api1/games

Sigue exactamente el mismo patrón que `gamerpower_client.py` (separación
entre "guardar RAW tal cual" y "extraer registros para procesamiento"),
para no duplicar la explicación de esa decisión de diseño.

Atribución: los datos provienen de MMOBomb.com.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

from src.config import MMOBOMB_API_URL, REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

_WRAPPER_KEY = "games"
_SOURCE_NAME = "MMOBomb"


class MMOBombClientError(Exception):
    """Error genérico consultando, validando o guardando datos de MMOBomb."""


def fetch_raw_payload(url: str = MMOBOMB_API_URL, timeout: int = REQUEST_TIMEOUT_SECONDS) -> Any:
    """Realiza la petición GET a MMOBomb y devuelve el payload JSON tal cual (ver `gamerpower_client.fetch_raw_payload`)."""
    logger.info("Consultando API MMOBomb: %s", url)

    try:
        response = requests.get(url, timeout=timeout)
    except requests.exceptions.Timeout as exc:
        raise MMOBombClientError(f"Timeout al consultar MMOBomb tras {timeout}s") from exc
    except requests.exceptions.ConnectionError as exc:
        raise MMOBombClientError(f"Error de conexión al consultar MMOBomb: {exc}") from exc
    except requests.exceptions.RequestException as exc:
        raise MMOBombClientError(f"Error inesperado en la petición HTTP a MMOBomb: {exc}") from exc

    if response.status_code != 200:
        raise MMOBombClientError(f"Código HTTP inesperado de MMOBomb: {response.status_code} - {response.reason}")

    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise MMOBombClientError(f"La respuesta de MMOBomb no es un JSON válido: {exc}") from exc


def extract_records(payload: Any) -> list[dict[str, Any]]:
    """Interpreta el payload RAW y devuelve la lista de juegos que contiene (ver `gamerpower_client.extract_records`)."""
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get(_WRAPPER_KEY), list):
        records = payload[_WRAPPER_KEY]
    else:
        raise MMOBombClientError(
            f"Estructura de respuesta inesperada de {_SOURCE_NAME}: se esperaba una lista "
            f"o un objeto con la clave '{_WRAPPER_KEY}'."
        )

    if not records:
        raise MMOBombClientError(f"{_SOURCE_NAME} devolvió una lista vacía de juegos.")

    if not all(isinstance(item, dict) for item in records):
        raise MMOBombClientError(
            f"No todos los elementos de la respuesta de {_SOURCE_NAME} son objetos JSON válidos."
        )

    logger.info("Registros interpretados de %s: %d", _SOURCE_NAME, len(records))
    return records


def save_raw_json(payload: Any, path: Path) -> None:
    """Guarda `payload` exactamente como fue recibido (ver `gamerpower_client.save_raw_json`)."""
    logger.info("Guardando respuesta RAW de MMOBomb en: %s", path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        raise MMOBombClientError(f"No fue posible escribir el archivo RAW de MMOBomb: {exc}") from exc


def download_games(
    url: str = MMOBOMB_API_URL,
    output_path: Path | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """Flujo completo: descarga, guarda el RAW tal cual, y devuelve los registros ya interpretados."""
    payload = fetch_raw_payload(url, timeout)
    if output_path is not None:
        save_raw_json(payload, output_path)
    return extract_records(payload)
