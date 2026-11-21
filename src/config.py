"""Centralised application configuration.

All settings are read from environment variables (optionally loaded from a
``.env`` file in the project root). Relative paths are always resolved against
the project root, so the project never depends on machine-specific absolute
paths.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Load .env once at import time. Real environment variables take precedence.
load_dotenv(PROJECT_ROOT / ".env", override=False)

# Output file names produced by the pipeline and consumed by the API.
CLEANED_SALES_FILE = "cleaned_sales.csv"
SALES_BY_CATEGORY_FILE = "sales_by_category.csv"
SALES_BY_REGION_FILE = "sales_by_region.csv"
MONTHLY_SALES_FILE = "monthly_sales.csv"
TOP_PRODUCTS_FILE = "top_products.csv"
OVERALL_SUMMARY_FILE = "overall_summary.json"
PIPELINE_REPORT_FILE = "pipeline_report.json"
REJECTED_RECORDS_FILE = "rejected_records.csv"

VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
VALID_WRITE_MODES = {"auto", "spark", "local"}


def _resolve_path(value: str | None, default: str) -> Path:
    """Resolve a (possibly relative) path against the project root."""
    path = Path(value or default).expanduser()
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path.resolve()


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"Environment variable {name} must be an integer, got {raw!r}") from exc


@dataclass(frozen=True)
class Settings:
    """Immutable application settings."""

    raw_data_path: Path
    processed_data_path: Path
    reports_path: Path
    log_dir: Path
    log_level: str
    spark_master: str
    spark_app_name: str
    spark_shuffle_partitions: int
    spark_log_level: str
    output_write_mode: str

    @property
    def log_file(self) -> Path:
        return self.log_dir / "pipeline.log"

    @classmethod
    def from_env(cls) -> "Settings":
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        if log_level not in VALID_LOG_LEVELS:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(VALID_LOG_LEVELS)}, got {log_level!r}")

        write_mode = os.getenv("OUTPUT_WRITE_MODE", "auto").lower()
        if write_mode not in VALID_WRITE_MODES:
            raise ValueError(
                f"OUTPUT_WRITE_MODE must be one of {sorted(VALID_WRITE_MODES)}, got {write_mode!r}"
            )

        return cls(
            raw_data_path=_resolve_path(os.getenv("RAW_DATA_PATH"), "data/raw/sales_data.csv"),
            processed_data_path=_resolve_path(os.getenv("PROCESSED_DATA_PATH"), "data/processed"),
            reports_path=_resolve_path(os.getenv("REPORTS_PATH"), "data/reports"),
            log_dir=_resolve_path(os.getenv("LOG_DIR"), "logs"),
            log_level=log_level,
            spark_master=os.getenv("SPARK_MASTER", "local[*]"),
            spark_app_name=os.getenv("SPARK_APP_NAME", "SalesDataPipeline"),
            spark_shuffle_partitions=_int_env("SPARK_SHUFFLE_PARTITIONS", 4),
            spark_log_level=os.getenv("SPARK_LOG_LEVEL", "ERROR").upper(),
            output_write_mode=write_mode,
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return the cached application settings."""
    return Settings.from_env()
