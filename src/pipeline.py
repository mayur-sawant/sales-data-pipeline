"""ETL pipeline orchestration."""

from __future__ import annotations

import datetime as dt
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from src import config
from src.aggregations import build_aggregations
from src.config import Settings, get_settings
from src.data_cleaning import clean_sales_data
from src.data_ingestion import load_sales_data
from src.data_output import resolve_write_mode, write_dataframe_csv, write_json
from src.data_transformation import transform_sales_data
from src.logging_config import format_fields, setup_logging
from src.spark_session import create_spark_session, stop_spark_session

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Outcome of a pipeline run."""

    success: bool
    duration_seconds: float = 0.0
    statistics: dict = field(default_factory=dict)
    outputs: dict[str, str] = field(default_factory=dict)
    error: str | None = None

    @property
    def status(self) -> str:
        return "SUCCESS" if self.success else "FAILED"


def run_pipeline(
    settings: Settings | None = None,
    input_path: Path | None = None,
    output_dir: Path | None = None,
) -> PipelineResult:
    """Run the full ETL workflow and return a :class:`PipelineResult`.

    Steps: init Spark -> load -> validate -> clean -> transform -> aggregate ->
    save -> log statistics -> stop Spark. Exceptions are logged and reported as a
    failed result instead of propagating, so callers get a clear status.
    """
    settings = settings or get_settings()
    setup_logging(settings.log_level, settings.log_file)
    input_path = Path(input_path or settings.raw_data_path)
    output_dir = Path(output_dir or settings.processed_data_path)
    reports_dir = settings.reports_path

    started = time.perf_counter()
    started_at = dt.datetime.now(dt.timezone.utc)
    result = PipelineResult(success=False)
    spark = None
    logger.info("=" * 70)
    logger.info("Pipeline started %s", format_fields(input=input_path, output=output_dir))

    try:
        # Step 1: Spark
        spark = create_spark_session(settings)

        # Step 2-3: Extract + validate input
        ingestion = load_sales_data(spark, input_path)

        # Step 4: Clean
        cleaning = clean_sales_data(ingestion.dataframe)

        # Step 5: Transform
        transformed, discount_report = transform_sales_data(cleaning.cleaned)

        # Step 6: Aggregate
        aggregates = build_aggregations(spark, transformed)

        # Step 7: Load / save outputs
        mode = settings.output_write_mode
        logger.info("Writing outputs %s", format_fields(writer=resolve_write_mode(mode)))
        outputs = {
            "cleaned_sales": write_dataframe_csv(transformed, output_dir / config.CLEANED_SALES_FILE, mode),
            "sales_by_category": write_dataframe_csv(
                aggregates.sales_by_category, output_dir / config.SALES_BY_CATEGORY_FILE, mode
            ),
            "sales_by_region": write_dataframe_csv(
                aggregates.sales_by_region, output_dir / config.SALES_BY_REGION_FILE, mode
            ),
            "monthly_sales": write_dataframe_csv(
                aggregates.monthly_sales, output_dir / config.MONTHLY_SALES_FILE, mode
            ),
            "top_products": write_dataframe_csv(
                aggregates.top_products, output_dir / config.TOP_PRODUCTS_FILE, mode
            ),
            "rejected_records": write_dataframe_csv(
                cleaning.rejected, reports_dir / config.REJECTED_RECORDS_FILE, mode
            ),
        }
        summary = {**aggregates.overall_summary, "generated_at": started_at.isoformat()}
        outputs["overall_summary"] = write_json(summary, output_dir / config.OVERALL_SUMMARY_FILE)

        # Step 8: Statistics
        result.success = True
        result.statistics = {
            "input_files": ingestion.source_files,
            "input_records": ingestion.record_count,
            "malformed_records": ingestion.malformed_count,
            "cleaning": cleaning.report.to_dict(),
            "discounts": discount_report.to_dict(),
            "output_records": cleaning.report.records_after,
        }
        result.outputs = {name: str(path) for name, path in outputs.items()}
    except Exception as exc:
        result.error = f"{type(exc).__name__}: {exc}"
        logger.exception("Pipeline failed %s", format_fields(error=type(exc).__name__))
    finally:
        # Step 9: Stop Spark
        stop_spark_session(spark)
        result.duration_seconds = round(time.perf_counter() - started, 2)

    _write_run_report(result, started_at, input_path, reports_dir)
    logger.info(
        "Pipeline finished %s",
        format_fields(status=result.status, duration_seconds=result.duration_seconds),
    )
    return result


def _write_run_report(result: PipelineResult, started_at: dt.datetime, input_path: Path, reports_dir: Path) -> None:
    """Persist run metadata for auditing; never fails the pipeline."""
    report = {
        "status": result.status,
        "started_at": started_at.isoformat(),
        "duration_seconds": result.duration_seconds,
        "input_path": str(input_path),
        "statistics": result.statistics,
        "outputs": result.outputs,
        "error": result.error,
    }
    try:
        write_json(report, reports_dir / config.PIPELINE_REPORT_FILE)
    except OSError:
        logger.exception("Could not write pipeline report")
