"""Extract step: read raw sales CSV data with an explicit schema."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.exceptions import InputDataError, SchemaValidationError
from src.logging_config import format_fields
from src.schemas import CORRUPT_RECORD_COLUMN, REQUIRED_COLUMNS, build_schema_for_header

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IngestionResult:
    """Raw data plus ingestion statistics."""

    dataframe: DataFrame
    record_count: int
    malformed_count: int
    source_files: list[str]


def resolve_input_files(input_path: Path) -> list[Path]:
    """Return the CSV files referenced by ``input_path`` (a file or a directory).

    Raises:
        InputDataError: if the path is missing, not a CSV file, or contains no CSV files.
    """
    if not input_path.exists():
        raise InputDataError(
            f"Input path not found: {input_path}. "
            "Generate sample data with: python scripts/generate_sample_data.py"
        )
    if input_path.is_dir():
        files = sorted(p for p in input_path.glob("*.csv") if p.is_file())
        if not files:
            raise InputDataError(f"No CSV files found in directory: {input_path}")
        return files
    if input_path.suffix.lower() != ".csv":
        raise InputDataError(f"Input file must be a .csv file: {input_path}")
    return [input_path]


def read_csv_header(csv_file: Path) -> list[str]:
    """Read and normalise the header row of a CSV file."""
    try:
        with csv_file.open("r", encoding="utf-8-sig") as handle:
            first_line = handle.readline()
    except OSError as exc:
        raise InputDataError(f"Cannot read input file {csv_file}: {exc}") from exc
    if not first_line.strip():
        raise InputDataError(f"Input file is empty or has no header row: {csv_file}")
    return [column.strip().strip('"').lower() for column in first_line.rstrip("\r\n").split(",")]


def validate_required_columns(columns: list[str], source: str = "input") -> None:
    """Ensure every required column is present.

    Raises:
        SchemaValidationError: listing any missing columns.
    """
    missing = [column for column in REQUIRED_COLUMNS if column not in columns]
    if missing:
        raise SchemaValidationError(
            f"{source} is missing required columns: {missing}. "
            f"Expected columns: {list(REQUIRED_COLUMNS)}"
        )
    extra = [column for column in columns if column not in REQUIRED_COLUMNS]
    if extra:
        logger.warning("Ignoring unexpected columns %s", format_fields(source=source, columns=extra))


def load_sales_data(spark: SparkSession, input_path: Path | str) -> IngestionResult:
    """Load raw sales data from a CSV file or a directory of CSV files.

    * Uses an explicit schema whose order follows the file header.
    * Uses PERMISSIVE mode: unparseable values become null and the raw line is
      kept in ``_corrupt_record`` so malformed rows can be counted and handled
      during cleaning instead of crashing the job.
    """
    input_path = Path(input_path)
    files = resolve_input_files(input_path)
    logger.info("Reading raw sales data %s", format_fields(path=input_path, files=len(files)))

    header = read_csv_header(files[0])
    for other in files[1:]:
        if read_csv_header(other) != header:
            raise SchemaValidationError(f"CSV header of {other} differs from {files[0]}")
    validate_required_columns(header, source=str(files[0].name))

    schema = build_schema_for_header(header)
    raw_df = (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_RECORD_COLUMN)
        .option("ignoreLeadingWhiteSpace", True)
        .option("ignoreTrailingWhiteSpace", True)
        .option("encoding", "UTF-8")
        .schema(schema)
        .csv([str(path) for path in files])
    )
    # Caching is required by Spark before filtering on the corrupt-record column
    # and avoids re-reading the file for every count.
    raw_df = raw_df.cache()

    record_count = raw_df.count()
    if record_count == 0:
        raise InputDataError(f"Input contains a header but no data rows: {input_path}")
    malformed_count = raw_df.filter(F.col(CORRUPT_RECORD_COLUMN).isNotNull()).count()

    logger.info(
        "Raw data loaded %s",
        format_fields(records=record_count, malformed_records=malformed_count),
    )
    if malformed_count:
        logger.warning(
            "Malformed rows detected; unparseable fields set to null and handled during cleaning %s",
            format_fields(malformed_records=malformed_count),
        )

    selected = raw_df.select(*REQUIRED_COLUMNS, F.col(CORRUPT_RECORD_COLUMN).isNotNull().alias("is_malformed"))
    return IngestionResult(
        dataframe=selected,
        record_count=record_count,
        malformed_count=malformed_count,
        source_files=[str(path) for path in files],
    )
