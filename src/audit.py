"""
audit.py
========

Genera un reporte de auditoría que compara los datos obtenidos
directamente de la API con los datos efectivamente almacenados en
SQLite, usando `id` como identificador de comparación.

El objetivo es dejar evidencia trazable de que la ingesta fue completa
y correcta (o, en su defecto, documentar exactamente qué falló).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config import API_URL
from src.database import GAME_COLUMNS

logger = logging.getLogger(__name__)

# Campos que se comparan valor a valor entre API y SQLite. Se excluyen
# columnas internas (como `ingested_at`) que no provienen de la API y por
# lo tanto no son relevantes para detectar diferencias reales de contenido.
COMPARABLE_FIELDS: tuple[str, ...] = GAME_COLUMNS


@dataclass
class AuditResult:
    """Resultado estructurado de la auditoría, usado también en pruebas."""

    api_count: int
    db_count: int
    api_ids: set[int]
    db_ids: set[int]
    missing_in_db: set[int] = field(default_factory=set)
    extra_in_db: set[int] = field(default_factory=set)
    records_with_differences: list[dict[str, Any]] = field(default_factory=list)

    @property
    def count_matches(self) -> bool:
        return self.api_count == self.db_count

    @property
    def is_successful(self) -> bool:
        return (
            not self.missing_in_db
            and not self.extra_in_db
            and not self.records_with_differences
        )


def _normalize(value: Any) -> Any:
    """Normaliza un valor para comparación, evitando falsos positivos por
    diferencias irrelevantes de tipo (por ejemplo None vs cadena vacía)."""
    if value is None:
        return ""
    return str(value).strip()


def _diff_record(api_record: dict[str, Any], db_record: dict[str, Any]) -> dict[str, tuple[Any, Any]]:
    """Devuelve un diccionario {campo: (valor_api, valor_db)} para los
    campos cuyo valor normalizado difiere entre ambas fuentes."""
    differences: dict[str, tuple[Any, Any]] = {}
    for field_name in COMPARABLE_FIELDS:
        api_value = _normalize(api_record.get(field_name))
        db_value = _normalize(db_record.get(field_name))
        if api_value != db_value:
            differences[field_name] = (api_record.get(field_name), db_record.get(field_name))
    return differences


def build_audit_result(
    api_games: list[dict[str, Any]],
    db_games: list[dict[str, Any]],
) -> AuditResult:
    """
    Compara los registros crudos de la API contra los registros
    almacenados en SQLite y construye un `AuditResult`.

    Args:
        api_games: Registros obtenidos directamente de la API.
        db_games: Registros leídos desde la tabla `games`.

    Returns:
        Un `AuditResult` con el detalle completo de la comparación.
    """
    api_by_id = {g["id"]: g for g in api_games if g.get("id") is not None}
    db_by_id = {g["id"]: g for g in db_games if g.get("id") is not None}

    api_ids = set(api_by_id.keys())
    db_ids = set(db_by_id.keys())

    missing_in_db = api_ids - db_ids
    extra_in_db = db_ids - api_ids

    records_with_differences: list[dict[str, Any]] = []
    for shared_id in api_ids & db_ids:
        differences = _diff_record(api_by_id[shared_id], db_by_id[shared_id])
        if differences:
            records_with_differences.append({"id": shared_id, "differences": differences})

    return AuditResult(
        api_count=len(api_games),
        db_count=len(db_games),
        api_ids=api_ids,
        db_ids=db_ids,
        missing_in_db=missing_in_db,
        extra_in_db=extra_in_db,
        records_with_differences=records_with_differences,
    )


def _format_id_set(ids: set[int], limit: int = 30) -> str:
    """Formatea un conjunto de IDs para el reporte, truncando si es muy largo."""
    if not ids:
        return "Ninguno"
    ordered = sorted(ids)
    if len(ordered) <= limit:
        return ", ".join(str(i) for i in ordered)
    shown = ", ".join(str(i) for i in ordered[:limit])
    return f"{shown} ... (+{len(ordered) - limit} adicionales)"


def render_audit_report(result: AuditResult, api_url: str = API_URL) -> str:
    """Construye el texto completo del reporte de auditoría."""
    timestamp = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    count_status = "OK" if result.count_matches else "ERROR"
    final_status = "VALIDACIÓN EXITOSA" if result.is_successful else "VALIDACIÓN CON ERRORES"

    lines: list[str] = [
        "AUDITORÍA DE INGESTIÓN",
        "======================",
        "",
        "Fuente:",
        api_url,
        "",
        "Fecha/hora de ejecución:",
        timestamp,
        "",
        "Registros extraídos desde API:",
        str(result.api_count),
        "",
        "Registros almacenados en SQLite:",
        str(result.db_count),
        "",
        "Resultado de conteo:",
        count_status,
        "",
        "IDs extraídos desde API:",
        _format_id_set(result.api_ids),
        "",
        "IDs almacenados en SQLite:",
        _format_id_set(result.db_ids),
        "",
        "IDs faltantes en SQLite:",
        _format_id_set(result.missing_in_db),
        "",
        "IDs adicionales en SQLite:",
        _format_id_set(result.extra_in_db),
        "",
        "Registros con diferencias:",
    ]

    if not result.records_with_differences:
        lines.append("Ninguno. No se detectaron diferencias de contenido entre API y SQLite.")
    else:
        for entry in result.records_with_differences:
            lines.append(f"- ID {entry['id']}:")
            for field_name, (api_value, db_value) in entry["differences"].items():
                lines.append(f"    {field_name}: API='{api_value}' | SQLite='{db_value}'")

    lines.extend(["", "Resultado final:", final_status, ""])

    return "\n".join(lines)


def write_audit_report(
    api_games: list[dict[str, Any]],
    db_games: list[dict[str, Any]],
    output_path: Path,
) -> AuditResult:
    """
    Genera y persiste el reporte de auditoría en `output_path`.

    Args:
        api_games: Registros obtenidos directamente de la API.
        db_games: Registros leídos desde SQLite.
        output_path: Ruta destino del archivo .txt.

    Returns:
        El `AuditResult` calculado (útil para pruebas y para el resumen
        final que imprime `main.py`).
    """
    logger.info("Generando auditoría")

    result = build_audit_result(api_games, db_games)
    report_text = render_audit_report(result)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(report_text, encoding="utf-8")

    if result.is_successful:
        logger.info("Validación completada correctamente")
    else:
        logger.warning(
            "Validación con diferencias: %d faltantes, %d adicionales, %d con diferencias",
            len(result.missing_in_db),
            len(result.extra_in_db),
            len(result.records_with_differences),
        )

    return result
