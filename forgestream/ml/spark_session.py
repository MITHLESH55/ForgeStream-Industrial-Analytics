"""SparkSession factory and lifecycle manager for ForgeStream distributed ML."""

import os
import sys
import logging
from typing import Optional
from pyspark.sql import SparkSession
from forgestream.ml.config import SparkConfig

logger = logging.getLogger("forgestream.ml.spark")

_GLOBAL_SPARK_SESSION: Optional[SparkSession] = None


def get_spark_session(config: Optional[SparkConfig] = None) -> SparkSession:
    """Retrieve or create an active Apache Spark session configured for local MLlib execution.

    Ensures correct Python worker binding and JVM reflection parameters on Java 21 LTS.
    """
    global _GLOBAL_SPARK_SESSION

    if _GLOBAL_SPARK_SESSION is not None and not _GLOBAL_SPARK_SESSION.sparkContext._jsc.sc().isStopped():
        return _GLOBAL_SPARK_SESSION

    if config is None:
        config = SparkConfig()

    # Ensure valid JAVA_HOME is set if missing or pointing to non-existent java executable
    java_home = os.environ.get("JAVA_HOME")
    if not java_home or not (os.path.exists(os.path.join(java_home, "bin", "java.exe")) or os.path.exists(os.path.join(java_home, "bin", "java"))):
        for candidate in [r"C:\Program Files\Java\jdk-24", r"C:\Program Files\Java\jdk-21"]:
            if os.path.exists(os.path.join(candidate, "bin", "java.exe")) or os.path.exists(os.path.join(candidate, "bin", "java")):
                os.environ["JAVA_HOME"] = candidate
                break

    # Bind Python executable paths for PySpark workers
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    builder = (
        SparkSession.builder.appName(config.app_name)
        .master(config.master)
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.driver.extraJavaOptions", config.extra_java_options)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    logger.info("Initializing SparkSession with master=%s, memory=%s", config.master, config.driver_memory)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    _GLOBAL_SPARK_SESSION = spark
    return spark


def stop_spark_session() -> None:
    """Safely terminate the active SparkSession and release resources."""
    global _GLOBAL_SPARK_SESSION
    if _GLOBAL_SPARK_SESSION is not None:
        try:
            _GLOBAL_SPARK_SESSION.stop()
            logger.info("SparkSession stopped successfully.")
        except Exception as e:
            logger.warning("Error stopping SparkSession: %s", e)
        finally:
            _GLOBAL_SPARK_SESSION = None
