"""SparkSession factory and lifecycle helpers."""

from __future__ import annotations

import logging
import os
import shutil
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from src.config import Settings, get_settings
from src.exceptions import JavaNotFoundError, SparkInitializationError
from src.logging_config import format_fields

logger = logging.getLogger(__name__)

JAVA_HELP = (
    "PySpark requires a Java runtime (JDK 17 or 21). Install one, e.g.\n"
    "  winget install EclipseAdoptium.Temurin.21.JDK\n"
    "then set JAVA_HOME (system environment or the project .env file), e.g.\n"
    "  JAVA_HOME=C:\\Program Files\\Eclipse Adoptium\\jdk-21.x.x-hotspot"
)


def find_java_executable() -> Path:
    """Locate the ``java`` executable via JAVA_HOME or PATH.

    Raises:
        JavaNotFoundError: if no Java runtime can be found.
    """
    executable = "java.exe" if os.name == "nt" else "java"
    java_home = os.getenv("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / executable
        if candidate.is_file():
            return candidate
        raise JavaNotFoundError(
            f"JAVA_HOME is set to '{java_home}' but '{candidate}' does not exist.\n{JAVA_HELP}"
        )

    on_path = shutil.which("java")
    if on_path:
        return Path(on_path)
    raise JavaNotFoundError(f"Java was not found on PATH and JAVA_HOME is not set.\n{JAVA_HELP}")


def _configure_python_executables() -> None:
    """Make Spark's Python workers use the current interpreter (important on Windows)."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


def create_spark_session(settings: Settings | None = None, app_name: str | None = None):
    """Create (or reuse) a configured SparkSession.

    Args:
        settings: application settings; defaults to the cached global settings.
        app_name: optional override of the Spark application name.

    Returns:
        pyspark.sql.SparkSession
    """
    settings = settings or get_settings()
    java = find_java_executable()
    _configure_python_executables()

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise SparkInitializationError(
            "PySpark is not installed. Run: pip install -r requirements.txt"
        ) from exc

    name = app_name or settings.spark_app_name
    logger.info(
        "Starting Spark session %s",
        format_fields(app=name, master=settings.spark_master, java=java),
    )
    try:
        spark = (
            SparkSession.builder.appName(name)
            .master(settings.spark_master)
            .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
    except Exception as exc:
        raise SparkInitializationError(
            f"Failed to start Spark ({type(exc).__name__}: {exc}). "
            f"Check that Java 17/21 is installed and JAVA_HOME is correct.\n{JAVA_HELP}"
        ) from exc

    spark.sparkContext.setLogLevel(settings.spark_log_level)
    logger.info("Spark session started %s", format_fields(version=spark.version))
    return spark


def stop_spark_session(spark) -> None:
    """Stop a SparkSession, logging (not raising) any shutdown problems."""
    if spark is None:
        return
    try:
        spark.stop()
        logger.info("Spark session stopped")
    except Exception:  # pragma: no cover - defensive
        logger.exception("Error while stopping Spark session")


@contextmanager
def spark_session_scope(settings: Settings | None = None) -> Iterator:
    """Context manager that guarantees the SparkSession is stopped."""
    spark = create_spark_session(settings)
    try:
        yield spark
    finally:
        stop_spark_session(spark)
