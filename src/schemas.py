"""Spark schema definitions and domain constants for raw sales data."""

from __future__ import annotations

from pyspark.sql.types import (
    DataType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

CORRUPT_RECORD_COLUMN = "_corrupt_record"

# Expected raw column types. ``order_date`` is read as a string on purpose: raw
# dates arrive in mixed / invalid formats and are parsed explicitly during cleaning.
RAW_COLUMN_TYPES: dict[str, DataType] = {
    "order_id": StringType(),
    "order_date": StringType(),
    "customer_id": StringType(),
    "product_id": StringType(),
    "product_name": StringType(),
    "category": StringType(),
    "region": StringType(),
    "quantity": IntegerType(),
    "unit_price": DoubleType(),
    "discount": DoubleType(),
    "payment_method": StringType(),
}

REQUIRED_COLUMNS: tuple[str, ...] = tuple(RAW_COLUMN_TYPES)

RAW_SALES_SCHEMA = StructType(
    [StructField(name, dtype, nullable=True) for name, dtype in RAW_COLUMN_TYPES.items()]
)

VALID_CATEGORIES = ("Electronics", "Furniture", "Clothing", "Accessories")
VALID_REGIONS = ("North", "South", "East", "West")

UNKNOWN_CUSTOMER = "GUEST"
UNKNOWN_PRODUCT = "Unknown Product"
UNKNOWN_CATEGORY = "Uncategorized"
UNKNOWN_REGION = "Unknown"

# Accepted raw date formats, tried in order.
DATE_FORMATS = ("yyyy-MM-dd", "yyyy/MM/dd", "dd-MM-yyyy")

# Column order of the cleaned / transformed dataset written to disk.
CLEANED_COLUMNS = [
    "order_id",
    "order_date",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "region",
    "quantity",
    "unit_price",
    "discount",
    "payment_method",
    "gross_amount",
    "discount_amount",
    "net_sales",
    "order_year",
    "order_month",
    "order_quarter",
]


def build_schema_for_header(header: list[str]) -> StructType:
    """Build a schema whose field order matches the CSV header.

    Spark maps a user-supplied CSV schema to columns *by position*, so the schema
    must follow the header order. Known columns get their expected types; any
    extra columns are read as strings. A corrupt-record column captures rows that
    fail to parse.
    """
    fields = [
        StructField(name, RAW_COLUMN_TYPES.get(name, StringType()), nullable=True)
        for name in header
    ]
    fields.append(StructField(CORRUPT_RECORD_COLUMN, StringType(), nullable=True))
    return StructType(fields)
