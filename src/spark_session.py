"""
spark_session.py
=================

Gestión del ciclo de vida de la `SparkSession` utilizada en la
Actividad 2 (preprocesamiento / ELT).

Este módulo NO conoce nada sobre SQLite, limpieza ni auditoría: su única
responsabilidad es crear y cerrar correctamente una sesión de Spark
configurada para ejecución local.

¿Por qué `local[*]` simula procesamiento distribuido en una máquina local?
---------------------------------------------------------------------------
`local[*]` le indica a Spark que ejecute en modo local pero utilizando
tantos hilos de ejecución ("particiones lógicas") como núcleos de CPU
estén disponibles en la máquina. Aunque no existe un clúster real de
varios nodos físicos, Spark sigue:

- dividiendo los datos en particiones,
- distribuyendo el trabajo entre varios hilos en paralelo,
- utilizando el mismo motor de planificación (Catalyst) y las mismas
  APIs (`DataFrame`, transformaciones perezosas/lazy, acciones) que
  usaría en un clúster real (por ejemplo, en Databricks o EMR).

Esto permite practicar y demostrar el paradigma de procesamiento
distribuido de Big Data sin necesidad de contratar infraestructura cloud,
cumpliendo el requisito de "simulación local" de esta actividad.
"""

from __future__ import annotations

import logging

# --------------------------------------------------------------------------
# Compatibilidad con Python 3.12+ (workaround oficial de Apache Spark)
# --------------------------------------------------------------------------
# PySpark 3.5.x todavía usa internamente `distutils.version.LooseVersion`
# para comparar versiones de Pandas (`require_minimum_pandas_version`).
# `distutils` fue eliminado de la librería estándar de Python en la
# versión 3.12 (PEP 632), por lo que en Python 3.12+ esa llamada lanza
# `ModuleNotFoundError: No module named 'distutils'` la primera vez que se
# usa una función de Spark que la dispara (por ejemplo,
# `spark.createDataFrame()` a partir de un DataFrame de Pandas).
#
# `setuptools` incluye una copia vendorizada de `distutils` y, al
# importarse, registra un "import hook" que hace que cualquier
# `import distutils` posterior se resuelva contra esa copia. Este es el
# workaround oficial documentado por el propio proyecto Apache Spark
# (ver https://issues.apache.org/jira/browse/SPARK-47613): "As a
# workaround we can import setuptools before creating the session".
# Se aplica aquí, en el primer módulo de EA2 que importa `pyspark`, para
# que quede activo antes de cualquier operación de Spark. La solución
# definitiva (sin necesidad de este workaround) es actualizar a
# PySpark >= 4.0, donde Apache Spark eliminó por completo la dependencia
# de `distutils` (ver https://issues.apache.org/jira/browse/SPARK-44120).
try:
    import setuptools  # noqa: F401
except ImportError:
    logging.getLogger(__name__).warning(
        "No se encontró 'setuptools' instalado. En Python 3.12+ esto puede "
        "causar 'ModuleNotFoundError: No module named distutils' al usar "
        "PySpark 3.5.x. Solución: pip install setuptools (ya incluido en "
        "requirements.txt) o actualizar a pyspark>=4.0."
    )

from pyspark.sql import SparkSession

from src.config import SPARK_APP_NAME, SPARK_MASTER

logger = logging.getLogger(__name__)


def get_spark_session(
    app_name: str = SPARK_APP_NAME,
    master: str = SPARK_MASTER,
) -> SparkSession:
    """
    Crea (o recupera, si ya existe) una única `SparkSession` local.

    `SparkSession.builder.getOrCreate()` garantiza que, dentro del mismo
    proceso, exista una única sesión activa: si ya hay una creada con una
    configuración compatible, la reutiliza en vez de crear una nueva.

    Args:
        app_name: Nombre de la aplicación Spark (visible en logs y UI).
        master: URL del master de Spark. `local[*]` = modo local usando
            todos los núcleos disponibles.

    Returns:
        Una `SparkSession` lista para usar.
    """
    logger.info("Inicializando Spark (master=%s, app=%s)", master, app_name)

    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Con volúmenes de datos pequeños (cientos de registros), un
        # número alto de particiones de shuffle solo agrega overhead.
        # Se reduce a un valor razonable para un entorno local/CI.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        # --------------------------------------------------------------
        # Conversión Pandas <-> Spark vía Apache Arrow
        # --------------------------------------------------------------
        # Sin esta configuración, `spark.createDataFrame(pandas_df)` y
        # `spark_df.toPandas()` usan una ruta antigua basada en RDDs: cada
        # fila se envuelve en una función Python que se serializa con
        # `cloudpickle` para enviarla a la JVM. En Windows, con versiones
        # recientes de Python (3.12+) y ciertas combinaciones de
        # PySpark/cloudpickle, esa serialización puede entrar en una
        # recursión que agota la pila y falla con:
        #   `_pickle.PicklingError: Could not serialize object:
        #    RecursionError: Stack overflow`
        # Habilitar Arrow evita esa ruta por completo: la conversión se
        # hace en formato columnar binario (sin cloudpickle de por medio),
        # lo cual es además más rápido. Requiere el paquete `pyarrow`
        # (ver requirements.txt). `fallback.enabled=false` asegura que, si
        # Arrow no pudiera usarse por algún motivo, el error sea explícito
        # en vez de caer silenciosamente en la ruta antigua y reproducir
        # el mismo problema.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def stop_spark_session(spark: SparkSession) -> None:
    """
    Cierra correctamente la `SparkSession`, liberando los recursos del
    proceso (hilos, memoria) que Spark reservó al iniciar.

    Args:
        spark: Sesión Spark activa a cerrar.
    """
    logger.info("Cerrando Spark")
    spark.stop()
