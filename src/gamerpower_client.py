"""
gamerpower_client.py
=====================

Cliente para la API de GamerPower.

- Documentación: https://www.gamerpower.com/api-read
- Endpoint usado: https://www.gamerpower.com/api/giveaways

Este módulo separa deliberadamente dos responsabilidades:

1. `fetch_raw_payload` + `save_raw_json`: obtienen la respuesta de la API
   y la guardan EXACTAMENTE como fue recibida (misma estructura, mismos
   nombres de campo, mismos valores), sin ninguna transformación. Esto
   preserva la procedencia de los datos (punto 6 del enunciado de EA3).
2. `extract_records`: deriva, a partir de ese payload ya guardado, una
   lista de diccionarios apta para cargarla en Pandas/Spark. Esta función
   NO modifica el archivo RAW; solo interpreta su forma (lista plana u
   objeto con una clave contenedora) para uso en memoria.

Atribución: los datos provienen de GamerPower.com, tal como exige su
política de uso (atribución obligatoria, sin necesidad de API key).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import requests

from src.config import GAMERPOWER_API_URL, REQUEST_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

_WRAPPER_KEY = "giveaways"
_SOURCE_NAME = "GamerPower"


class GamerPowerClientError(Exception):
    """Error genérico consultando, validando o guardando datos de GamerPower."""


def fetch_raw_payload(url: str = GAMERPOWER_API_URL, timeout: int = REQUEST_TIMEOUT_SECONDS) -> Any:
    """
    Realiza la petición GET a GamerPower y devuelve el payload JSON
    exactamente como la API lo entrega (puede ser una lista o un objeto
    con una clave contenedora, según la respuesta real).

    Args:
        url: Endpoint de GamerPower a consultar.
        timeout: Tiempo máximo de espera (segundos).

    Returns:
        El payload JSON ya parseado, sin ninguna transformación.

    Raises:
        GamerPowerClientError: ante timeout, error de conexión, código
            HTTP distinto de 200, o respuesta que no sea JSON válido.
    """
    logger.info("Consultando API GamerPower: %s", url)

    try:
        response = requests.get(url, timeout=timeout)
    except requests.exceptions.Timeout as exc:
        raise GamerPowerClientError(f"Timeout al consultar GamerPower tras {timeout}s") from exc
    except requests.exceptions.ConnectionError as exc:
        raise GamerPowerClientError(f"Error de conexión al consultar GamerPower: {exc}") from exc
    except requests.exceptions.RequestException as exc:
        raise GamerPowerClientError(f"Error inesperado en la petición HTTP a GamerPower: {exc}") from exc

    if response.status_code != 200:
        raise GamerPowerClientError(
            f"Código HTTP inesperado de GamerPower: {response.status_code} - {response.reason}"
        )

    try:
        return response.json()
    except json.JSONDecodeError as exc:
        raise GamerPowerClientError(f"La respuesta de GamerPower no es un JSON válido: {exc}") from exc


def extract_records(payload: Any) -> list[dict[str, Any]]:
    """
    Interpreta el payload RAW (sin modificarlo) y devuelve la lista de
    registros de giveaways que contiene.

    Admite tanto una lista plana como un objeto `{"giveaways": [...]}`,
    ya que la forma exacta de la respuesta debe verificarse en tiempo de
    ejecución (no se asume una estructura fija, ver punto 8 del enunciado).

    Args:
        payload: El JSON ya parseado (ver `fetch_raw_payload`).

    Returns:
        Lista de diccionarios, uno por giveaway.

    Raises:
        GamerPowerClientError: si la estructura no es reconocible o está vacía.
    """
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get(_WRAPPER_KEY), list):
        records = payload[_WRAPPER_KEY]
    else:
        raise GamerPowerClientError(
            f"Estructura de respuesta inesperada de {_SOURCE_NAME}: se esperaba una lista "
            f"o un objeto con la clave '{_WRAPPER_KEY}'."
        )

    if not records:
        raise GamerPowerClientError(f"{_SOURCE_NAME} devolvió una lista vacía de giveaways.")

    if not all(isinstance(item, dict) for item in records):
        raise GamerPowerClientError(
            f"No todos los elementos de la respuesta de {_SOURCE_NAME} son objetos JSON válidos."
        )

    logger.info("Registros interpretados de %s: %d", _SOURCE_NAME, len(records))
    return records


def save_raw_json(payload: Any, path: Path) -> None:
    """
    Guarda `payload` exactamente como fue recibido de la API (misma
    estructura, mismos nombres de campo, mismos valores). No se aplica
    ninguna limpieza, normalización ni eliminación de duplicados: esa
    etapa ocurre únicamente sobre la estructura ya cargada en memoria
    (ver `enrichment.py`), nunca sobre este archivo.

    Args:
        payload: Payload JSON a persistir (ver `fetch_raw_payload`).
        path: Ruta destino (`data/raw/gamerpower/giveaways.json`).
    """
    logger.info("Guardando respuesta RAW de GamerPower en: %s", path)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as exc:
        raise GamerPowerClientError(f"No fue posible escribir el archivo RAW de GamerPower: {exc}") from exc


def download_giveaways(
    url: str = GAMERPOWER_API_URL,
    output_path: Path | None = None,
    timeout: int = REQUEST_TIMEOUT_SECONDS,
) -> list[dict[str, Any]]:
    """
    Flujo completo: descarga, guarda el RAW tal cual, y devuelve los
    registros ya interpretados para su uso en el pipeline.

    Args:
        url: Endpoint de GamerPower.
        output_path: Ruta donde guardar el JSON crudo. Si es `None`, no
            se guarda (útil para pruebas).
        timeout: Tiempo máximo de espera (segundos).

    Returns:
        Lista de registros (diccionarios) de giveaways.
    """
    payload = fetch_raw_payload(url, timeout)
    if output_path is not None:
        save_raw_json(payload, output_path)
    return extract_records(payload)
