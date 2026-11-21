"""Transform step (part 1): data quality checks and cleaning.

Every cleaning operation logs how many records it touched. Records that cannot
be processed reliably are not silently dropped: they are separated into a
``rejected`` DataFrame with a ``rejection_reason`` so they can be audited.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from src.logging_config import format_fields
from src.schemas import (
    DATE_FORMATS,
    REQUIRED_COLUMNS,
    UNKNOWN_CATEGORY,
    UNKNOWN_CUSTOMER,
    UNKNOWN_PRODUCT,
    UNKNOWN_REGION,
    VALID_CATEGORIES,
    VALID_REGIONS,
)

logger = logging.getLogger(__name__)

NULL_PLACEHOLDERS = ("", "null", "none", "n/a", "na", "nan", "-")
STRING_COLUMNS = (
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "region",
    "payment_method",
)
BUSINESS_KEY = ("order_id", "product_id")


@dataclass
class CleaningReport:
    """Record counts for each cleaning operation."""

    records_before: int = 0
    records_after: int = 0
    records_rejected: int = 0
    steps: dict[str, int] = field(default_factory=dict)
    rejection_reasons: dict[str, int] = field(default_factory=dict)

    def record(self, step: str, affected: int, **extra: object) -> None:
        self.steps[step] = affected
        logger.info("Cleaning step %s", format_fields(step=step, affected_records=affected, **extra))

    def to_dict(self) -> dict:
        return {
            "records_before": self.records_before,
            "records_after": self.records_after,
            "records_rejected": self.records_rejected,
            "steps": dict(self.steps),
            "rejection_reasons": dict(self.rejection_reasons),
        }


@dataclass
class CleaningResult:
    cleaned: DataFrame
    rejected: DataFrame
    report: CleaningReport


# ---------------------------------------------------------------------------
# Individual cleaning operations (pure DataFrame -> DataFrame functions)
# ---------------------------------------------------------------------------


def standardize_nulls(df: DataFrame) -> DataFrame:
    """Trim string columns and convert placeholder values ('', 'N/A', 'null', ...) to null."""
    for column in STRING_COLUMNS:
        trimmed = F.trim(F.col(column))
        df = df.withColumn(
            column,
            F.when(F.lower(trimmed).isin(*NULL_PLACEHOLDERS), F.lit(None)).otherwise(trimmed),
        )
    return df


def normalize_text_columns(df: DataFrame) -> DataFrame:
    """Normalise casing of category and region ('ELECTRONICS', ' electronics' -> 'Electronics').

    Unknown regions become ``Unknown``; unknown categories become null so they can
    be imputed from the product catalogue later.
    """
    category = F.initcap(F.lower(F.col("category")))
    region = F.initcap(F.lower(F.col("region")))
    return df.withColumn(
        "category", F.when(category.isin(*VALID_CATEGORIES), category).otherwise(F.lit(None))
    ).withColumn(
        "region", F.when(region.isin(*VALID_REGIONS), region).otherwise(F.lit(UNKNOWN_REGION))
    )


def parse_order_dates(df: DataFrame, column: str = "order_date") -> DataFrame:
    """Parse string dates using the accepted formats; invalid dates become null.

    ``try_to_timestamp`` returns null instead of failing on bad input even when
    ANSI mode is enabled (the Spark 4 default).
    """
    parsed = F.coalesce(
        *[F.try_to_timestamp(F.col(column), F.lit(fmt)).cast("date") for fmt in DATE_FORMATS]
    )
    return df.withColumn(column, parsed)


def remove_exact_duplicates(df: DataFrame) -> DataFrame:
    """Drop rows that are identical across all business columns."""
    return df.dropDuplicates(list(REQUIRED_COLUMNS))


def remove_business_key_duplicates(df: DataFrame) -> DataFrame:
    """Keep one row per (order_id, product_id), preferring the most complete row.

    Rows with a missing key are left untouched (they are rejected later).
    """
    # Rank by completeness of the business columns only (helper columns are ignored).
    completeness = sum(F.col(c).isNotNull().cast("int") for c in REQUIRED_COLUMNS)
    window = Window.partitionBy(*BUSINESS_KEY).orderBy(
        F.desc("_completeness"), F.asc_nulls_last("order_date"), F.asc("customer_id")
    )
    has_key = F.col("order_id").isNotNull() & F.col("product_id").isNotNull()

    keyed = (
        df.filter(has_key)
        .withColumn("_completeness", completeness)
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_completeness", "_rank")
    )
    return keyed.unionByName(df.filter(~has_key))


def _most_common_value_by_product(df: DataFrame, column: str) -> DataFrame:
    """Return (product_id, ref_<column>) holding the most frequent non-null value per product."""
    window = Window.partitionBy("product_id").orderBy(F.desc("count"), F.asc(column))
    return (
        df.filter(F.col("product_id").isNotNull() & F.col(column).isNotNull())
        .groupBy("product_id", column)
        .count()
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .select("product_id", F.col(column).alias(f"ref_{column}"))
    )


def impute_from_product_catalog(df: DataFrame, column: str, default: str) -> DataFrame:
    """Fill a missing product attribute using the value seen for the same product_id.

    Values that cannot be derived fall back to ``default``.
    """
    reference = F.broadcast(_most_common_value_by_product(df, column))
    return (
        df.join(reference, on="product_id", how="left")
        .withColumn(column, F.coalesce(F.col(column), F.col(f"ref_{column}"), F.lit(default)))
        .drop(f"ref_{column}")
    )


def fill_missing_customers(df: DataFrame) -> DataFrame:
    """Replace missing customer IDs with a GUEST placeholder (the sale is still valid revenue)."""
    return df.withColumn("customer_id", F.coalesce(F.col("customer_id"), F.lit(UNKNOWN_CUSTOMER)))


def impute_invalid_prices(df: DataFrame) -> DataFrame:
    """Replace null / non-positive unit prices with the product's median valid price."""
    valid_price = F.col("unit_price").isNotNull() & (F.col("unit_price") > 0)
    medians = F.broadcast(
        df.filter(valid_price & F.col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(F.round(F.median("unit_price"), 2).alias("ref_unit_price"))
    )
    return (
        df.join(medians, on="product_id", how="left")
        .withColumn("unit_price", F.when(valid_price, F.col("unit_price")).otherwise(F.col("ref_unit_price")))
        .drop("ref_unit_price")
    )


def add_rejection_reason(df: DataFrame) -> DataFrame:
    """Tag rows that cannot be processed reliably with the first failing rule."""
    reason = (
        F.when(F.col("order_id").isNull(), "missing_order_id")
        .when(F.col("product_id").isNull(), "missing_product_id")
        .when(F.col("order_date").isNull(), "invalid_order_date")
        .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity")
        .when(F.col("unit_price").isNull() | (F.col("unit_price") <= 0), "invalid_unit_price")
    )
    return df.withColumn("rejection_reason", reason)


# ---------------------------------------------------------------------------
# Orchestration of the cleaning steps
# ---------------------------------------------------------------------------


def _count_where(df: DataFrame, condition) -> int:
    return df.filter(condition).count()


def clean_sales_data(raw_df: DataFrame) -> CleaningResult:
    """Run all cleaning operations and return cleaned rows, rejected rows and a report."""
    report = CleaningReport()
    report.records_before = raw_df.count()
    logger.info("Cleaning started %s", format_fields(records_before=report.records_before))

    df = raw_df
    if "is_malformed" in df.columns:
        report.record("malformed_rows_flagged", _count_where(df, F.col("is_malformed")))
        df = df.drop("is_malformed")

    # 1. Placeholder values -> null
    df = standardize_nulls(df).cache()
    missing_dates = _count_where(df, F.col("order_date").isNull())

    # 2. Casing normalisation
    before_norm = df
    df = normalize_text_columns(df).cache()
    casing_fixed = (
        before_norm.filter(
            F.col("category").isNotNull()
            & (F.initcap(F.lower("category")) != F.col("category"))
        ).count()
        + before_norm.filter(
            F.col("region").isNotNull() & (F.initcap(F.lower("region")) != F.col("region"))
        ).count()
    )
    report.record("normalized_category_region_casing", casing_fixed)
    report.record("unknown_region_values", _count_where(df, F.col("region") == UNKNOWN_REGION))

    # 3. Date parsing
    raw_dates_present = _count_where(df, F.col("order_date").isNotNull())
    df = parse_order_dates(df.withColumn("raw_order_date", F.col("order_date"))).cache()
    invalid_dates = raw_dates_present - _count_where(df, F.col("order_date").isNotNull())
    report.record("invalid_dates", invalid_dates, missing_dates=missing_dates)

    # 4. Duplicate removal (exact, then business key)
    count_before = df.count()
    df = remove_exact_duplicates(df).cache()
    count_after_exact = df.count()
    report.record("exact_duplicates_removed", count_before - count_after_exact)

    df = remove_business_key_duplicates(df).cache()
    report.record("business_key_duplicates_removed", count_after_exact - df.count())

    # 5. Missing product attributes
    missing_names = _count_where(df, F.col("product_name").isNull())
    missing_categories = _count_where(df, F.col("category").isNull())
    df = impute_from_product_catalog(df, "product_name", UNKNOWN_PRODUCT)
    df = impute_from_product_catalog(df, "category", UNKNOWN_CATEGORY).cache()
    report.record(
        "missing_product_names_filled",
        missing_names,
        unresolved=_count_where(df, F.col("product_name") == UNKNOWN_PRODUCT),
    )
    report.record(
        "missing_categories_filled",
        missing_categories,
        unresolved=_count_where(df, F.col("category") == UNKNOWN_CATEGORY),
    )

    # 6. Missing customers
    report.record("missing_customer_ids_filled", _count_where(df, F.col("customer_id").isNull()))
    df = fill_missing_customers(df)

    # 7. Invalid prices
    invalid_prices = _count_where(df, F.col("unit_price").isNull() | (F.col("unit_price") <= 0))
    df = impute_invalid_prices(df).cache()
    unresolved_prices = _count_where(df, F.col("unit_price").isNull())
    report.record(
        "invalid_prices_imputed", invalid_prices - unresolved_prices, unresolved=unresolved_prices
    )
    report.record(
        "invalid_quantities", _count_where(df, F.col("quantity").isNull() | (F.col("quantity") <= 0))
    )

    # 8. Split valid / rejected rows
    tagged = add_rejection_reason(df).cache()
    cleaned = tagged.filter(F.col("rejection_reason").isNull()).drop("rejection_reason", "raw_order_date")
    # Rejected rows keep the original date string so the problem can be audited.
    rejected = tagged.filter(F.col("rejection_reason").isNotNull()).select(
        *[c for c in REQUIRED_COLUMNS if c != "order_date"], "raw_order_date", "rejection_reason"
    )

    reasons = {row["rejection_reason"]: row["count"] for row in rejected.groupBy("rejection_reason").count().collect()}
    report.rejection_reasons = dict(sorted(reasons.items()))
    report.records_rejected = sum(reasons.values())
    report.records_after = cleaned.count()

    for reason, count in report.rejection_reasons.items():
        logger.warning("Rejected records %s", format_fields(reason=reason, records=count))
    logger.info(
        "Cleaning completed %s",
        format_fields(
            records_before=report.records_before,
            records_after=report.records_after,
            records_rejected=report.records_rejected,
            duplicates_removed=report.steps["exact_duplicates_removed"]
            + report.steps["business_key_duplicates_removed"],
        ),
    )
    return CleaningResult(cleaned=cleaned, rejected=rejected, report=report)
