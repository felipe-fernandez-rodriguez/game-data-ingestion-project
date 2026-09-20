"""
enrichment_audit.py
====================

Métricas y reporte de auditoría del proceso de enriquecimiento (EA3) con
las APIs de GamerPower y MMOBomb.

Contiene las estructuras de datos que registran, con cifras reales
calculadas durante la ejecución (nunca inventadas), el resultado de
integrar ambas fuentes con el dataset base de la Actividad 2, así como
la función que renderiza `output/enrichment_report.txt`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from src.config import (
    DATA_LINEAGE_EA3,
    GAMERPOWER_API_URL,
    MMOBOMB_API_URL,
)
from src.title_matching import DeduplicationSummary

logger = logging.getLogger(__name__)


@dataclass
class SourceIntegrationResult:
    """Resumen de la integración de UNA fuente (GamerPower o MMOBomb) con el dataset base."""

    source_name: str
    raw_path: Path
    records_downloaded: int
    unique_titles: int
    dedup_summary: DeduplicationSummary
    base_records: int
    records_matched: int
    records_unmatched: int
    new_columns: tuple[str, ...]

    @property
    def duplicate_titles(self) -> int:
        return self.dedup_summary.duplicate_titles

    @property
    def coverage_percent(self) -> float:
        """Cobertura = registros_coincidentes / registros_base * 100 (punto 26 del enunciado)."""
        if self.base_records == 0:
            return 0.0
        return round(self.records_matched / self.base_records * 100, 2)


@dataclass
class CardinalityCheckpoint:
    """Registros antes/después de una etapa del pipeline, para el control de cardinalidad (punto 20)."""

    label: str
    records_before: int
    records_after: int

    @property
    def change(self) -> int:
        return self.records_after - self.records_before


@dataclass
class EnrichmentResult:
    """Resultado consolidado de toda la Actividad 3, usado para construir el reporte final."""

    base_records: int
    base_columns: int
    base_unique_ids: int
    gamerpower: SourceIntegrationResult
    mmobomb: SourceIntegrationResult
    homologation_notes: list[str] = field(default_factory=list)
    cardinality_checkpoints: list[CardinalityCheckpoint] = field(default_factory=list)
    final_records: int = 0
    final_columns: int = 0
    final_duplicate_ids: int = 0
    granularity_preserved: bool = True
    ambiguous_matches: int = 0
    consistency_issues: list[str] = field(default_factory=list)

    @property
    def base_records_preserved(self) -> bool:
        return self.final_records == self.base_records

    @property
    def is_successful(self) -> bool:
        return (
            self.base_records_preserved
            and self.final_duplicate_ids == 0
            and self.granularity_preserved
            and self.ambiguous_matches == 0
            and not self.consistency_issues
        )

    @property
    def new_columns_total(self) -> int:
        return self.final_columns - self.base_columns


def _render_source_section(result: SourceIntegrationResult) -> list[str]:
    return [
        f"Registros descargados: {result.records_downloaded}",
        f"Títulos únicos: {result.unique_titles}",
        f"Duplicados detectados: {result.duplicate_titles}",
        f"Coincidencias: {result.records_matched}",
        f"No coincidencias: {result.records_unmatched}",
        f"Cobertura: {result.coverage_percent}%",
        "",
        f"Columnas integradas: {', '.join(result.new_columns)}",
        "",
    ]


def render_enrichment_report(
    *,
    base_csv_path: Path,
    result: EnrichmentResult,
) -> str:
    """Construye el texto completo de `output/enrichment_report.txt` a partir de resultados reales."""
    timestamp = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    separator = "=" * 52
    subseparator = "-" * 52

    if result.is_successful:
        conclusion = "ENRIQUECIMIENTO EXITOSO"
    elif result.final_records > 0:
        conclusion = "ENRIQUECIMIENTO CON OBSERVACIONES"
    else:
        conclusion = "ENRIQUECIMIENTO FALLIDO"

    lines: list[str] = [
        separator,
        "AUDITORÍA DE ENRIQUECIMIENTO",
        separator,
        "",
        "Actividad:",
        "EA3 - Enriquecimiento de Datos",
        "",
        "Dataset base:",
        str(base_csv_path),
        "",
        subseparator,
        "FUENTES UTILIZADAS",
        subseparator,
        "",
        "GamerPower:",
        GAMERPOWER_API_URL,
        "",
        "Archivo RAW:",
        str(result.gamerpower.raw_path),
        "",
        "MMOBomb:",
        MMOBOMB_API_URL,
        "",
        "Archivo RAW:",
        str(result.mmobomb.raw_path),
        "",
        "Fecha y hora:",
        timestamp,
        "",
        subseparator,
        "1. DATASET BASE",
        subseparator,
        "",
        f"Registros: {result.base_records}",
        f"Columnas: {result.base_columns}",
        f"IDs únicos: {result.base_unique_ids}",
        "",
        subseparator,
        "2. GAMERPOWER",
        subseparator,
        "",
    ]

    lines.extend(_render_source_section(result.gamerpower))

    lines.extend([subseparator, "3. MMOBOMB", subseparator, ""])
    lines.extend(_render_source_section(result.mmobomb))

    lines.extend(
        [
            subseparator,
            "4. HOMOLOGACIÓN",
            subseparator,
            "",
            "Clave principal:",
            "title",
            "",
            "Clave auxiliar:",
            "title_match",
            "",
            "Transformaciones:",
        ]
    )
    if result.homologation_notes:
        lines.extend(f"- {note}" for note in result.homologation_notes)
    else:
        lines.append("(Sin transformaciones adicionales registradas)")
    lines.append("")

    lines.extend(
        [
            subseparator,
            "5. CONTROL DE DUPLICADOS",
            subseparator,
            "",
            f"Duplicados fuente GamerPower: {result.gamerpower.duplicate_titles}",
            f"Duplicados fuente MMOBomb: {result.mmobomb.duplicate_titles}",
            "",
            f"Duplicados resultado (ids duplicados en el dataset final): {result.final_duplicate_ids}",
            "",
        ]
    )

    lines.extend([subseparator, "6. CONTROL DE CARDINALIDAD", subseparator, ""])
    for checkpoint in result.cardinality_checkpoints:
        lines.append(
            f"{checkpoint.label}: {checkpoint.records_before} -> {checkpoint.records_after} "
            f"(cambio: {checkpoint.change:+d})"
        )
    checkpoint_by_label = {c.label: c for c in result.cardinality_checkpoints}
    after_gamerpower = checkpoint_by_label.get("Después de GamerPower")
    after_mmobomb = checkpoint_by_label.get("Después de MMOBomb")
    lines.extend(
        [
            "",
            f"Registros base: {result.base_records}",
            f"Después de GamerPower: {after_gamerpower.records_after if after_gamerpower else result.base_records}",
            f"Después de MMOBomb: {after_mmobomb.records_after if after_mmobomb else result.final_records}",
            f"Registros finales: {result.final_records}",
            "",
        ]
    )

    enrichment_pct = round(result.new_columns_total / result.base_columns * 100, 2) if result.base_columns else 0
    lines.extend(
        [
            subseparator,
            "7. DATASET ENRIQUECIDO",
            subseparator,
            "",
            f"Columnas originales: {result.base_columns}",
            f"Nuevas columnas: {result.new_columns_total}",
            f"Columnas finales: {result.final_columns}",
            f"Registros finales: {result.final_records}",
            f"Porcentaje de enriquecimiento: {enrichment_pct}% más columnas respecto al dataset base",
            "",
        ]
    )

    lines.extend(
        [
            subseparator,
            "8. VALIDACIÓN",
            subseparator,
            "",
            f"¿Se conservaron los registros base?: {'Sí' if result.base_records_preserved else 'No'}",
            f"¿Se mantuvo 1 juego = 1 registro?: {'Sí' if result.granularity_preserved else 'No'}",
            "¿Existen IDs duplicados?: "
            + (f"Sí ({result.final_duplicate_ids})" if result.final_duplicate_ids else "No"),
            "¿Existen coincidencias ambiguas?: "
            + (f"Sí ({result.ambiguous_matches})" if result.ambiguous_matches else "No"),
            "",
        ]
    )
    if result.consistency_issues:
        lines.append("Observaciones adicionales:")
        lines.extend(f"  - {issue}" for issue in result.consistency_issues)
        lines.append("")

    lines.extend(
        [
            subseparator,
            "9. RESULTADO",
            subseparator,
            "",
            conclusion,
            "",
            subseparator,
            "10. DATA LINEAGE (columnas nuevas de EA3)",
            subseparator,
            "",
        ]
    )
    for column, origin in DATA_LINEAGE_EA3.items():
        lines.append(f"  {column:<26} -> {origin}")

    lines.append(separator)

    return "\n".join(lines)
