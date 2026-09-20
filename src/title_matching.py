"""
title_matching.py
===================

Normalización de títulos y resolución de duplicados de clave, usadas
para integrar FreeToGame con GamerPower y MMOBomb por **nombre de
juego** en vez de por `id` (los tres proveedores tienen sus propios
sistemas de identificación, ver punto 13 del enunciado).

Todo este módulo trabaja en **Pandas**, deliberadamente, antes de crear
cualquier DataFrame de Spark: expresar "normalizar un texto" o "elegir un
registro representativo por título duplicado" como una UDF de PySpark
obligaría a serializar funciones Python con `cloudpickle` hacia la JVM,
la misma ruta que causó los errores de `RecursionError` en Windows
documentados en `spark_session.py`. Haciendo esto en Pandas, el
DataFrame que finalmente se convierte a Spark (vía
`spark_session.pandas_to_spark`) ya llega con `title_match` calculado y
sin duplicados, y el join en Spark es una comparación de columnas simple
(sin UDFs).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------
# Normalización de títulos
# --------------------------------------------------------------------------
# Anotaciones de plataforma/distribuidor que GamerPower agrega entre
# paréntesis junto al nombre del juego (ver punto 16 del enunciado).
# Lista cerrada y explícita: solo se eliminan estos casos conocidos, no
# cualquier texto entre paréntesis (que podría ser parte real del título).
_KNOWN_DISTRIBUTOR_ANNOTATIONS: tuple[str, ...] = (
    "steam",
    "epic games store",
    "epic games",
    "gog",
    "indiegala",
    "origin",
    "ea origin",
    "ubisoft connect",
    "ubisoft",
    "humble bundle",
    "drm-free",
    "itch.io",
    "battle.net",
    "playstation",
    "xbox",
    "switch",
)
_DISTRIBUTOR_PATTERN = re.compile(
    r"\(\s*(?:" + "|".join(re.escape(a) for a in _KNOWN_DISTRIBUTOR_ANNOTATIONS) + r")\s*\)",
    re.IGNORECASE,
)
_GIVEAWAY_SUFFIX_PATTERN = re.compile(r"\bgiveaway\b\s*$", re.IGNORECASE)
_MULTI_SPACE_PATTERN = re.compile(r"\s+")


def normalize_title(title: object) -> str:
    """
    Normaliza un título a una forma canónica para comparación (`title_match`).

    Reglas aplicadas, en este orden (determinísticas, sin aleatoriedad):

    1. `None`/valor no textual -> cadena vacía.
    2. Elimina el sufijo final "Giveaway" (propio de GamerPower).
    3. Elimina anotaciones de distribuidor/plataforma conocidas entre
       paréntesis (p. ej. "(Steam)", "(Indiegala)").
    4. Convierte a minúsculas.
    5. Recorta espacios al inicio/fin y colapsa espacios múltiples.

    Nunca modifica el título original (`title`); solo produce el valor
    derivado que se guarda en `title_match`.

    Ejemplo:
        "Sausage Hunter (Indiegala) Giveaway" -> "sausage hunter"

    Args:
        title: Valor original del campo `title` (se admite cualquier tipo;
            valores no textuales o nulos producen cadena vacía).

    Returns:
        El título normalizado, listo para comparación exacta.
    """
    if title is None or (isinstance(title, float) and pd.isna(title)):
        return ""

    normalized = str(title)
    normalized = _GIVEAWAY_SUFFIX_PATTERN.sub("", normalized)
    normalized = _DISTRIBUTOR_PATTERN.sub(" ", normalized)
    normalized = normalized.lower()
    normalized = _MULTI_SPACE_PATTERN.sub(" ", normalized).strip()
    return normalized


def add_title_match_column(df: pd.DataFrame, title_column: str = "title") -> pd.DataFrame:
    """
    Agrega la columna auxiliar `title_match` a una copia del DataFrame,
    aplicando `normalize_title` sobre `title_column`. No modifica
    `title_column` en ningún momento.

    Args:
        df: DataFrame de origen (dataset base, GamerPower o MMOBomb).
        title_column: Nombre de la columna con el título original.

    Returns:
        Copia de `df` con la columna `title_match` agregada.
    """
    result = df.copy()
    result["title_match"] = result[title_column].map(normalize_title)
    return result


# --------------------------------------------------------------------------
# Mapping manual (Nivel 3 de matching, ver punto 17 del enunciado)
# --------------------------------------------------------------------------
MAPPING_COLUMNS: tuple[str, ...] = ("freetogame_title", "gamerpower_title", "mmobomb_title", "match_method")


def load_title_mapping(path: Path) -> pd.DataFrame:
    """
    Carga el mapping manual de equivalencias de título
    (`data/mappings/title_mapping.csv`), si existe.

    El archivo puede no existir o estar vacío (solo encabezado): el
    proyecto no incluye equivalencias inventadas por defecto (punto 43
    del enunciado, "nunca inventes... equivalencias"). Se agregan
    manualmente solo casos verificados.

    Args:
        path: Ruta al CSV de mapping.

    Returns:
        DataFrame con las columnas de `MAPPING_COLUMNS` (vacío si el
        archivo no existe o no tiene filas de datos).
    """
    if not path.exists():
        logger.info("No existe mapping manual de títulos en '%s' (se continúa sin él).", path)
        return pd.DataFrame(columns=MAPPING_COLUMNS)

    mapping_df = pd.read_csv(path, dtype=str).fillna("")
    missing = [c for c in MAPPING_COLUMNS if c not in mapping_df.columns]
    if missing:
        raise ValueError(f"El mapping de títulos '{path}' no tiene las columnas esperadas: {missing}")

    logger.info("Mapping manual de títulos cargado: %d equivalencias", len(mapping_df))
    return mapping_df


def apply_title_mapping(
    df: pd.DataFrame,
    mapping_df: pd.DataFrame,
    source_title_column: str,
    mapping_column: str,
) -> pd.DataFrame:
    """
    Sobrescribe `title_match` para las filas cuyo título original
    coincide EXACTAMENTE con una entrada del mapping manual, usando el
    `title_match` correspondiente al lado FreeToGame de esa equivalencia.

    Esto implementa el "Nivel 3" de matching (punto 17): un mapping
    explícito, verificado manualmente, para casos que la normalización
    automática no resuelve. Nunca se generan equivalencias nuevas aquí;
    solo se aplican las ya presentes en `mapping_df`.

    Args:
        df: DataFrame de la fuente (GamerPower o MMOBomb), ya con `title_match`.
        mapping_df: Resultado de `load_title_mapping`.
        source_title_column: Columna de `df` con el título original de la fuente.
        mapping_column: Columna de `mapping_df` a comparar (`gamerpower_title` o `mmobomb_title`).

    Returns:
        Copia de `df` con `title_match` ajustado donde aplique.
    """
    if mapping_df.empty:
        return df

    relevant = mapping_df[mapping_df[mapping_column] != ""]
    if relevant.empty:
        return df

    override_map = {
        row[mapping_column]: normalize_title(row["freetogame_title"]) for _, row in relevant.iterrows()
    }

    result = df.copy()
    overridden = result[source_title_column].isin(override_map)
    result.loc[overridden, "title_match"] = result.loc[overridden, source_title_column].map(override_map)

    if overridden.any():
        logger.info(
            "Mapping manual aplicado: %d registros de '%s' recibieron un title_match explícito.",
            int(overridden.sum()),
            source_title_column,
        )

    return result


# --------------------------------------------------------------------------
# Resolución de duplicados de title_match (para el DataFrame de proceso,
# NUNCA sobre el archivo RAW — ver punto 19 del enunciado)
# --------------------------------------------------------------------------
@dataclass
class DeduplicationSummary:
    """Resumen de la consolidación de registros duplicados por `title_match`."""

    source_name: str
    records_before: int
    unique_titles: int
    duplicate_titles: int
    records_after: int


def resolve_duplicate_titles(
    df: pd.DataFrame,
    source_name: str,
    priority_column: str | None = None,
    id_column: str = "id",
) -> tuple[pd.DataFrame, DeduplicationSummary]:
    """
    Consolida el DataFrame de una fuente a un único registro por
    `title_match` (granularidad "1 juego = 1 registro"), cuando existan
    varios (p. ej. varios giveaways activos para el mismo juego en
    GamerPower).

    Regla de selección (determinística y documentada):

    1. Si se indica `priority_column` (p. ej. `published_date` en
       GamerPower, `release_date` en MMOBomb) y existe en `df`, se ordena
       por esa columna de forma descendente (más reciente primero;
       valores nulos al final) y se conserva el primer registro de cada
       grupo — el más reciente/relevante.
    2. Como desempate final (o si `priority_column` no está disponible),
       se ordena por `id_column` ascendente y se conserva el primero —
       determinístico y reproducible.

    `title_match == ""` (título vacío tras normalizar) NUNCA se
    considera para deduplicar por título: esos registros simplemente no
    participarán del join por título (se documentan aparte, no se
    descartan silenciosamente).

    Args:
        df: DataFrame de la fuente, ya con `title_match`.
        source_name: Nombre lógico de la fuente, para logs/reportes.
        priority_column: Columna que determina cuál registro es "más
            representativo" cuando hay varios para el mismo `title_match`.
        id_column: Columna de desempate final.

    Returns:
        Tupla (DataFrame consolidado, resumen de la operación).
    """
    records_before = len(df)

    with_title = df[df["title_match"] != ""].copy()
    without_title = df[df["title_match"] == ""].copy()

    unique_titles = with_title["title_match"].nunique()
    duplicate_titles = int(with_title["title_match"].duplicated().sum())

    sort_columns = []
    ascending = []
    if priority_column and priority_column in with_title.columns:
        sort_columns.append(priority_column)
        ascending.append(False)  # más reciente primero
    if id_column in with_title.columns:
        sort_columns.append(id_column)
        ascending.append(True)

    if sort_columns:
        with_title = with_title.sort_values(by=sort_columns, ascending=ascending, na_position="last")

    deduped = with_title.drop_duplicates(subset=["title_match"], keep="first")

    result_df = pd.concat([deduped, without_title], ignore_index=True)
    records_after = len(result_df)

    summary = DeduplicationSummary(
        source_name=source_name,
        records_before=records_before,
        unique_titles=unique_titles,
        duplicate_titles=duplicate_titles,
        records_after=records_after,
    )

    if duplicate_titles:
        logger.info(
            "Fuente '%s': %d títulos duplicados consolidados a 1 registro representativo cada uno "
            "(regla: %s más reciente, desempate por '%s').",
            source_name,
            duplicate_titles,
            priority_column or "N/D",
            id_column,
        )

    return result_df, summary
