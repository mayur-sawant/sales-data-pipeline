"""Transform step (part 2): derived monetary and calendar columns."""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.exceptions import DataValidationError
from src.logging_config import format_fields
from src.schemas import CLEANED_COLUMNS

logger = logging.getLogger(__name__)

MAX_DISCOUNT = 0.9  # discounts above 90% are treated as data errors
MONEY_TOLERANCE = 0.01


@dataclass(frozen=True)
class DiscountReport:
    missing_discounts: int
    percentage_discounts_converted: int
    invalid_discounts_reset: int

    def to_dict(self) -> dict:
        return self.__dict__.copy()


def _discount_as_fraction():
    """Discount expressed as a fraction: percentages (1 < d <= 100) are divided by 100."""
    discount = F.col("discount")
    return F.when((discount > 1) & (discount <= 100), discount / 100).otherwise(discount)


def _is_valid_fraction(expr):
    return (expr >= 0) & (expr <= MAX_DISCOUNT)


def normalize_discount(df: DataFrame) -> DataFrame:
    """Validate discount rates so they are always a fraction in [0, MAX_DISCOUNT].

    * null                       -> 0.0 (no discount)
    * 1 < value <= 100           -> value / 100 (given as a percentage, e.g. 15 -> 0.15)
    * negative or otherwise bad  -> 0.0
    """
    fraction = _discount_as_fraction()
    return df.withColumn(
        "discount",
        F.round(F.when(_is_valid_fraction(fraction), fraction).otherwise(F.lit(0.0)), 4),
    )


def profile_discounts(df: DataFrame) -> DiscountReport:
    """Count discount values that :func:`normalize_discount` will change."""
    discount = F.col("discount")
    fraction = _discount_as_fraction()
    is_percentage = (discount > 1) & (discount <= 100)
    row = df.agg(
        F.sum(discount.isNull().cast("int")).alias("missing"),
        F.sum((is_percentage & _is_valid_fraction(fraction)).cast("int")).alias("pct"),
        F.sum((discount.isNotNull() & ~_is_valid_fraction(fraction)).cast("int")).alias("invalid"),
    ).first()
    return DiscountReport(
        missing_discounts=int(row["missing"] or 0),
        percentage_discounts_converted=int(row["pct"] or 0),
        invalid_discounts_reset=int(row["invalid"] or 0),
    )


def add_sales_metrics(df: DataFrame) -> DataFrame:
    """Add gross_amount, discount_amount and net_sales (rounded to 2 decimals)."""
    gross = F.col("quantity") * F.col("unit_price")
    return (
        df.withColumn("gross_amount", F.round(gross, 2))
        .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount"), 2))
        .withColumn("net_sales", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
    )


def add_date_parts(df: DataFrame) -> DataFrame:
    """Add order_year, order_month and order_quarter from order_date."""
    return (
        df.withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_quarter", F.quarter("order_date"))
    )


def validate_sales_metrics(df: DataFrame) -> None:
    """Fail fast if any monetary calculation violates a business rule.

    Raises:
        DataValidationError: describing the number of violating rows.
    """
    violations = df.filter(
        F.col("gross_amount").isNull()
        | F.col("net_sales").isNull()
        | (F.col("gross_amount") <= 0)
        | (F.col("net_sales") < 0)
        | (F.col("net_sales") > F.col("gross_amount"))
        | (F.col("discount") < 0)
        | (F.col("discount") > MAX_DISCOUNT)
        | (
            F.abs(F.col("gross_amount") - F.col("discount_amount") - F.col("net_sales"))
            > MONEY_TOLERANCE
        )
    ).count()
    if violations:
        raise DataValidationError(f"{violations} rows failed monetary validation after transformation")


def transform_sales_data(cleaned_df: DataFrame) -> tuple[DataFrame, DiscountReport]:
    """Apply all transformations and validations to the cleaned dataset."""
    discount_report = profile_discounts(cleaned_df)
    logger.info("Discount validation %s", format_fields(**discount_report.to_dict()))

    transformed = add_date_parts(add_sales_metrics(normalize_discount(cleaned_df)))
    transformed = transformed.select(*CLEANED_COLUMNS).cache()
    validate_sales_metrics(transformed)
    logger.info("Transformation completed %s", format_fields(records=transformed.count()))
    return transformed, discount_report
