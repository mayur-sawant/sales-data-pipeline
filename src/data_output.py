"""Load step: persist processed DataFrames and summaries to disk.

Two CSV writing strategies are supported:

``spark``  Native distributed writer (``df.write.csv``) coalesced to a single part
           file which is then renamed to the requested file name. On Windows this
           requires Hadoop's ``winutils.exe`` (``HADOOP_HOME``).
``local``  Streams rows partition-by-partition to the driver with
           ``toLocalIterator()`` and writes them with the ``csv`` module. Only one
           partition is held in driver memory at a time, so the full dataset is
           never collected. Works on Windows without winutils.

``auto`` (default) uses ``spark`` when it is supported, otherwise ``local``.
"""

from __future__ import annotations

import csv
import datetime as dt
import json
import logging
import os
import shutil
from pathlib import Path

from pyspark.sql import DataFrame

from src.logging_config import format_fields

logger = logging.getLogger(__name__)


def native_spark_write_supported() -> bool:
    """Return True if Spark's Hadoop-based file writer can run on this machine."""
    if os.name != "nt":
        return True
    hadoop_home = os.getenv("HADOOP_HOME")
    return bool(hadoop_home) and (Path(hadoop_home) / "bin" / "winutils.exe").is_file()


def resolve_write_mode(mode: str) -> str:
    """Translate ``auto`` into a concrete write strategy."""
    if mode == "auto":
        return "spark" if native_spark_write_supported() else "local"
    return mode


def _format_value(value: object) -> object:
    if value is None:
        return ""
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return value


def _write_csv_local(df: DataFrame, target: Path) -> int:
    tmp_file = target.with_suffix(target.suffix + ".tmp")
    rows_written = 0
    with tmp_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(df.columns)
        for row in df.toLocalIterator():
            writer.writerow([_format_value(value) for value in row])
            rows_written += 1
    os.replace(tmp_file, target)
    return rows_written


def _write_csv_spark(df: DataFrame, target: Path) -> int:
    tmp_dir = target.parent / f".{target.stem}_spark_tmp"
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .option("dateFormat", "yyyy-MM-dd")
        .csv(str(tmp_dir))
    )
    part_files = sorted(tmp_dir.glob("part-*.csv"))
    if not part_files:
        raise OSError(f"Spark produced no part file in {tmp_dir}")
    os.replace(part_files[0], target)
    shutil.rmtree(tmp_dir, ignore_errors=True)
    with target.open("r", encoding="utf-8") as handle:
        return max(sum(1 for _ in handle) - 1, 0)


def write_dataframe_csv(df: DataFrame, target: Path, mode: str = "auto") -> Path:
    """Write a DataFrame to a single CSV file with a header row."""
    target.parent.mkdir(parents=True, exist_ok=True)
    strategy = resolve_write_mode(mode)
    rows = _write_csv_spark(df, target) if strategy == "spark" else _write_csv_local(df, target)
    logger.info("Output written %s", format_fields(path=target, rows=rows, writer=strategy))
    return target


def write_json(data: dict, target: Path) -> Path:
    """Write a small dict (e.g. an aggregated summary) as pretty-printed JSON."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp_file = target.with_suffix(target.suffix + ".tmp")
    with tmp_file.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, default=str)
    os.replace(tmp_file, target)
    logger.info("Output written %s", format_fields(path=target, writer="json"))
    return target
