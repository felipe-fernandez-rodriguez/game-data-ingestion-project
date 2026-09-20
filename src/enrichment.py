"""
enrichment.py
==============

Punto de entrada de la Actividad 3 - Enriquecimiento de Datos.

Esta actividad NO vuelve a consultar FreeToGame ni SQLite. Su fuente
principal es el dataset limpio de la Actividad 2
(`data/processed/games_cleaned.csv`), enriquecido con dos APIs
adicionales:

- GamerPower (giveaways): https://www.gamerpower.com/api/giveaways
- MMOBomb (catálogo de juegos): https://www.mmobomb.com/api1/games

Los IDs de cada proveedor NO se cruzan entre sí (cada uno tiene su
propio sistema de identificación, ver punto 13 del enunciado). La clave
lógica de integración es el **título del juego**, normalizado en la
columna auxiliar `title_match` (ver `title_matching.py`).

Flujo implementado:

    1. Descargar GamerPower y MMOBomb, guardando el RAW tal cual (JSON).
    2. Cargar el dataset base (EA2) y calcular `title_match` para las
       tres fuentes (en Pandas, ver `title_matching.py`).
    3. Aplicar el mapping manual de títulos, si existe.
    4. Consolidar duplicados de `title_match` por fuente (granularidad
       "1 juego = 1 registro", con una regla de selección justificada).
    5. Convertir a Spark y hacer LEFT JOIN por `title_match`, primero con
       GamerPower y luego con MMOBomb, midiendo cobertura.
    6. Comparar campos de MMOBomb contra FreeToGame donde hubo coincidencia.
    7. Validar el resultado final (cardinalidad, duplicados, consistencia).
    8. Generar `data/enriched/games_enriched.csv`, la muestra y la auditoría.

Uso:
    python src/enrichment.py

Código de salida:
    0 si el pipeline se ejecutó y la validación final fue exitosa.
    1 si ocurrió un error durante el pipeline.
    2 si el pipeline se ejecutó pero la validación final detectó observaciones.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import DataFrame  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from src import config  # noqa: E402
from src import gamerpower_client  # noqa: E402
from src import mmobomb_client  # noqa: E402
from src.enrichment_audit import (  # noqa: E402
    CardinalityCheckpoint,
    EnrichmentResult,
    SourceIntegrationResult,
    render_enrichment_report,
)
from src.gamerpower_client import GamerPowerClientError  # noqa: E402
from src.generate_sample import SampleGenerationError, write_csv_sample  # noqa: E402
from src.mmobomb_client import MMOBombClientError  # noqa: E402
from src.spark_session import get_spark_session, pandas_to_spark, stop_spark_session  # noqa: E402
from src.title_matching import (  # noqa: E402
    DeduplicationSummary,
    add_title_match_column,
    apply_title_mapping,
    load_title_mapping,
    resolve_duplicate_titles,
)

logger = logging.getLogger(__name__)


class EnrichmentError(Exception):
    """Error de la Actividad 3 que debe detener el pipeline."""


# --------------------------------------------------------------------------
# Validaciones de origen (pre-flight)
# --------------------------------------------------------------------------
def ensure_base_dataset_exists(csv_path: Path) -> None:
    """Verifica que el dataset limpio de la Actividad 2 exista antes de iniciar Spark."""
    if not csv_path.exists():
        raise EnrichmentError(
            f"No se encontró el dataset limpio de la Actividad 2 en '{csv_path}'. "
            "Ejecuta primero 'python src/preprocessing.py' (EA2)."
        )


# --------------------------------------------------------------------------
# Carga y preparación (Pandas) de cada fuente
# --------------------------------------------------------------------------
def load_base_dataframe(csv_path: Path) -> pd.DataFrame:
    """Carga `games_cleaned.csv` (EA2) y agrega la columna auxiliar `title_match`."""
    pdf = pd.read_csv(csv_path)
    if pdf.empty:
        raise EnrichmentError(f"'{csv_path}' existe pero no contiene registros.")
    return add_title_match_column(pdf, title_column="title")


def _select_and_prefix(df: pd.DataFrame, column_mapping: dict[str, str]) -> pd.DataFrame:
    """Selecciona y renombra columnas de una fuente ya con prefijo (evita sobrescribir columnas del dataset base)."""
    available = {src: dst for src, dst in column_mapping.items() if src in df.columns}
    missing = set(column_mapping) - set(available)
    if missing:
        logger.warning("Columnas no disponibles en la fuente (se omiten): %s", sorted(missing))
    selected = df[list(available.keys())].rename(columns=available)
    selected["title_match"] = df["title_match"].values
    return selected


def prepare_gamerpower_dataframe(
    records: list[dict],
    mapping_df: pd.DataFrame,
) -> tuple[pd.DataFrame, int, DeduplicationSummary]:
    """
    Prepara el DataFrame de proceso de GamerPower: `title_match`, mapping
    manual, consolidación de duplicados (más reciente por
    `published_date`) y selección/prefijo de columnas.

    Returns:
        Tupla (DataFrame listo para el join, títulos únicos antes de
        deduplicar, resumen de la deduplicación).
    """
    pdf = pd.DataFrame(records)
    pdf = add_title_match_column(pdf, title_column="title")
    pdf = apply_title_mapping(pdf, mapping_df, source_title_column="title", mapping_column="gamerpower_title")

    unique_titles_raw = int(pdf.loc[pdf["title_match"] != "", "title_match"].nunique())

    deduped, dedup_summary = resolve_duplicate_titles(
        pdf, source_name="gamerpower", priority_column="published_date", id_column="id"
    )
    prepared = _select_and_prefix(deduped, config.GAMERPOWER_SELECTED_COLUMNS)
    return prepared, unique_titles_raw, dedup_summary


def prepare_mmobomb_dataframe(
    records: list[dict], mapping_df: pd.DataFrame
) -> tuple[pd.DataFrame, int, DeduplicationSummary]:
    """Análogo a `prepare_gamerpower_dataframe`, para MMOBomb (más reciente por `release_date`)."""
    pdf = pd.DataFrame(records)
    pdf = add_title_match_column(pdf, title_column="title")
    pdf = apply_title_mapping(pdf, mapping_df, source_title_column="title", mapping_column="mmobomb_title")

    unique_titles_raw = int(pdf.loc[pdf["title_match"] != "", "title_match"].nunique())

    deduped, dedup_summary = resolve_duplicate_titles(
        pdf, source_name="mmobomb", priority_column="release_date", id_column="id"
    )

    # Se conservan también los campos comparables (sin prefijo, en
    # columnas temporales) para poder generar las columnas *_match luego
    # del join, sin sobrescribir los campos originales de FreeToGame.
    comparison_df = deduped[["title_match", *[c for c in config.MMOBOMB_COMPARISON_FIELDS if c in deduped.columns]]]
    comparison_df = comparison_df.rename(
        columns={c: f"_cmp_mmobomb_{c}" for c in config.MMOBOMB_COMPARISON_FIELDS if c in comparison_df.columns}
    )

    prepared = _select_and_prefix(deduped, config.MMOBOMB_SELECTED_COLUMNS)
    prepared = prepared.merge(comparison_df, on="title_match", how="left")
    return prepared, unique_titles_raw, dedup_summary


# --------------------------------------------------------------------------
# LEFT JOIN por title_match, con medición de cobertura
# --------------------------------------------------------------------------
def left_join_on_title(
    accumulated_df: DataFrame,
    source_df: DataFrame,
    source_name: str,
) -> tuple[DataFrame, int, int]:
    """
    LEFT JOIN entre el dataset acumulado y una fuente, por `title_match`,
    midiendo cuántos registros base encontraron coincidencia.

    Igual que en EA2/versión anterior de EA3: se renombra temporalmente
    la clave de la fuente para poder distinguir con certeza un valor
    `NULL` real (no hubo coincidencia) incluso si las columnas nuevas de
    la fuente pudieran ser legítimamente nulas.
    """
    src_key_col = f"_src_title_match_{source_name}"
    aliased_source = source_df.withColumnRenamed("title_match", src_key_col)

    joined = accumulated_df.join(
        aliased_source,
        on=accumulated_df["title_match"] == aliased_source[src_key_col],
        how="left",
    )

    total_base = accumulated_df.count()
    matched = joined.filter(F.col(src_key_col).isNotNull()).count()
    unmatched = total_base - matched

    joined = joined.drop(src_key_col)

    return joined, matched, unmatched


def add_comparison_columns(df: DataFrame, fields: tuple[str, ...] = config.MMOBOMB_COMPARISON_FIELDS) -> DataFrame:
    """
    Agrega columnas booleanas `<campo>_match` comparando cada campo de
    FreeToGame contra su equivalente de MMOBomb (punto 24 del enunciado).

    La comparación es insensible a mayúsculas/espacios. Si MMOBomb no
    tiene valor para ese campo (no hubo coincidencia, o el campo vino
    nulo), la columna queda en `NULL` en vez de `False`: `NULL` significa
    "no se pudo comparar", `False` significaría "se comparó y difiere" —
    distinción importante para no sugerir una discrepancia inexistente.
    """
    result = df
    for field_name in fields:
        cmp_col = f"_cmp_mmobomb_{field_name}"
        match_col = f"{field_name}_match"
        if field_name not in df.columns or cmp_col not in df.columns:
            continue
        result = result.withColumn(
            match_col,
            F.when(
                F.col(cmp_col).isNotNull(),
                F.lower(F.trim(F.col(field_name))) == F.lower(F.trim(F.col(cmp_col))),
            ).otherwise(F.lit(None)),
        )
    drop_cols = [f"_cmp_mmobomb_{f}" for f in fields if f"_cmp_mmobomb_{f}" in result.columns]
    if drop_cols:
        result = result.drop(*drop_cols)
    return result


# --------------------------------------------------------------------------
# Orquestación
# --------------------------------------------------------------------------
def run_pipeline() -> int:
    """Ejecuta el pipeline de enriquecimiento completo de la Actividad 3."""
    logger.info("Iniciando Actividad 3")

    try:
        config.ensure_directories()
        ensure_base_dataset_exists(config.CLEANED_CSV_PATH)

        logger.info("Cargando dataset limpio")
        base_pdf = load_base_dataframe(config.CLEANED_CSV_PATH)
        base_records = len(base_pdf)
        base_columns = base_pdf.shape[1]
        base_unique_ids = int(base_pdf["id"].nunique())
        logger.info("Registros base: %d", base_records)

        homologation_notes: list[str] = [
            "title_match se calculó normalizando 'title' (minúsculas, espacios colapsados, "
            "eliminación de sufijos 'Giveaway' y anotaciones de distribuidor entre paréntesis "
            "propias de GamerPower), sin modificar el campo 'title' original.",
        ]

        duplicate_base_titles = int(
            base_pdf.loc[base_pdf["title_match"] != "", "title_match"].duplicated().sum()
        )
        if duplicate_base_titles:
            homologation_notes.append(
                f"{duplicate_base_titles} juegos del dataset base comparten el mismo title_match "
                "con otro juego base (p. ej. reediciones); cada uno se integra de forma "
                "independiente por su propio 'id'."
            )

        mapping_df = load_title_mapping(config.TITLE_MAPPING_PATH)
        if not mapping_df.empty:
            homologation_notes.append(
                f"Se aplicó un mapping manual de {len(mapping_df)} equivalencia(s) de título "
                f"desde '{config.TITLE_MAPPING_PATH}'."
            )

        logger.info("Descargando GamerPower")
        gp_payload = gamerpower_client.fetch_raw_payload(config.GAMERPOWER_API_URL)
        gamerpower_client.save_raw_json(gp_payload, config.GAMERPOWER_JSON_PATH)
        gp_records = gamerpower_client.extract_records(gp_payload)
        logger.info("Registros GamerPower: %d", len(gp_records))

        logger.info("Descargando MMOBomb")
        mb_payload = mmobomb_client.fetch_raw_payload(config.MMOBOMB_API_URL)
        mmobomb_client.save_raw_json(mb_payload, config.MMOBOMB_JSON_PATH)
        mb_records = mmobomb_client.extract_records(mb_payload)
        logger.info("Registros MMOBomb: %d", len(mb_records))

        gp_prepared, gp_unique_titles, gp_dedup = prepare_gamerpower_dataframe(gp_records, mapping_df)
        mb_prepared, mb_unique_titles, mb_dedup = prepare_mmobomb_dataframe(mb_records, mapping_df)

        for summary in (gp_dedup, mb_dedup):
            if summary.duplicate_titles:
                homologation_notes.append(
                    f"Fuente '{summary.source_name}': {summary.duplicate_titles} títulos duplicados "
                    f"consolidados a 1 registro representativo (más reciente)."
                )

        logger.info("Inicializando Spark")
        spark = get_spark_session(app_name="EA3-Enriquecimiento-FreeToGame")
        try:
            base_df = pandas_to_spark(spark, base_pdf)
            gp_df = pandas_to_spark(spark, gp_prepared)
            mb_df = pandas_to_spark(spark, mb_prepared)

            original_columns = list(base_df.columns)

            cardinality_checkpoints = [CardinalityCheckpoint("Dataset base", base_records, base_records)]

            logger.info("Ejecutando integración")
            records_before_gp = base_df.count()
            accumulated_df, gp_matched, gp_unmatched = left_join_on_title(base_df, gp_df, "gamerpower")
            records_after_gp = accumulated_df.count()
            cardinality_checkpoints.append(
                CardinalityCheckpoint("Después de GamerPower", records_before_gp, records_after_gp)
            )
            logger.info("Coincidencias GamerPower: %d", gp_matched)

            records_before_mb = accumulated_df.count()
            accumulated_df, mb_matched, mb_unmatched = left_join_on_title(accumulated_df, mb_df, "mmobomb")
            records_after_mb = accumulated_df.count()
            cardinality_checkpoints.append(
                CardinalityCheckpoint("Después de MMOBomb", records_before_mb, records_after_mb)
            )
            logger.info("Coincidencias MMOBomb: %d", mb_matched)

            accumulated_df = add_comparison_columns(accumulated_df)

            logger.info("Validando cardinalidad")
            final_records = accumulated_df.count()
            final_columns = len(accumulated_df.columns)
            final_unique_ids = accumulated_df.select("id").distinct().count()
            final_duplicate_ids = final_records - final_unique_ids
            granularity_preserved = final_records == base_records and final_duplicate_ids == 0

            # Ver docstring de `left_join_on_title`: como ambas fuentes ya
            # se deduplican por title_match ANTES del join (clave única
            # del lado derecho), un LEFT JOIN nunca puede producir una
            # coincidencia ambigua (una fila base emparejada con más de
            # un registro de la fuente). La ambigüedad, si existiera, ya
            # se resolvió de forma determinística en `resolve_duplicate_titles`.
            ambiguous_matches = 0

            consistency_issues: list[str] = []
            altered_rows = base_df.exceptAll(accumulated_df.select(*original_columns)).count()
            if altered_rows:
                consistency_issues.append(
                    f"{altered_rows} filas presentan columnas originales de EA2 con valores "
                    "distintos a los del dataset base."
                )

            gamerpower_result = SourceIntegrationResult(
                source_name="gamerpower",
                raw_path=config.GAMERPOWER_JSON_PATH,
                records_downloaded=len(gp_records),
                unique_titles=gp_unique_titles,
                dedup_summary=gp_dedup,
                base_records=base_records,
                records_matched=gp_matched,
                records_unmatched=gp_unmatched,
                new_columns=tuple(config.GAMERPOWER_SELECTED_COLUMNS.values()),
            )
            mmobomb_result = SourceIntegrationResult(
                source_name="mmobomb",
                raw_path=config.MMOBOMB_JSON_PATH,
                records_downloaded=len(mb_records),
                unique_titles=mb_unique_titles,
                dedup_summary=mb_dedup,
                base_records=base_records,
                records_matched=mb_matched,
                records_unmatched=mb_unmatched,
                new_columns=tuple(config.MMOBOMB_SELECTED_COLUMNS.values())
                + tuple(f"{f}_match" for f in config.MMOBOMB_COMPARISON_FIELDS),
            )

            result = EnrichmentResult(
                base_records=base_records,
                base_columns=base_columns,
                base_unique_ids=base_unique_ids,
                gamerpower=gamerpower_result,
                mmobomb=mmobomb_result,
                homologation_notes=homologation_notes,
                cardinality_checkpoints=cardinality_checkpoints,
                final_records=final_records,
                final_columns=final_columns,
                final_duplicate_ids=final_duplicate_ids,
                granularity_preserved=granularity_preserved,
                ambiguous_matches=ambiguous_matches,
                consistency_issues=consistency_issues,
            )

            logger.info("Generando dataset enriquecido")
            enriched_pdf = accumulated_df.toPandas()
        finally:
            stop_spark_session(spark)

        config.ENRICHED_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        enriched_pdf.to_csv(config.ENRICHED_CSV_PATH, index=False, encoding="utf-8")

        logger.info("Generando muestra")
        sample_result = write_csv_sample(enriched_pdf, config.ENRICHED_SAMPLE_PATH)

        logger.info("Generando auditoría")
        report_text = render_enrichment_report(base_csv_path=config.CLEANED_CSV_PATH, result=result)
        config.ENRICHMENT_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        config.ENRICHMENT_REPORT_PATH.write_text(report_text, encoding="utf-8")

        _print_summary(result, sample_result.sample_size)

        if result.is_successful:
            logger.info("Actividad 3 completada correctamente")
            return 0
        return 2

    except EnrichmentError as exc:
        logger.error("Fallo en la Actividad 3: %s", exc)
        return 1
    except GamerPowerClientError as exc:
        logger.error("Fallo consultando GamerPower: %s", exc)
        return 1
    except MMOBombClientError as exc:
        logger.error("Fallo consultando MMOBomb: %s", exc)
        return 1
    except SampleGenerationError as exc:
        logger.error("Fallo generando evidencias de la Actividad 3: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001 - último recurso, se registra y se sale con error
        logger.exception("Error inesperado durante la Actividad 3: %s", exc)
        return 1


def _print_summary(result: EnrichmentResult, sample_size: int) -> None:
    """Imprime un resumen final legible del resultado del pipeline de EA3."""
    status = "EXITOSO" if result.is_successful else "CON OBSERVACIONES (ver enrichment_report.txt)"
    logger.info(
        "Resumen final -> Registros: %d -> %d | Columnas: %d -> %d | Cobertura GamerPower: %s%% | "
        "Cobertura MMOBomb: %s%% | Muestra CSV: %d filas | Resultado: %s",
        result.base_records,
        result.final_records,
        result.base_columns,
        result.final_columns,
        result.gamerpower.coverage_percent,
        result.mmobomb.coverage_percent,
        sample_size,
        status,
    )


def main() -> None:
    config.configure_logging()
    exit_code = run_pipeline()
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
