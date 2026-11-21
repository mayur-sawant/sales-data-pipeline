"""Shared pytest fixtures: a lightweight local SparkSession and small datasets."""

from __future__ import annotations

import pytest

from src.config import Settings, get_settings
from src.exceptions import JavaNotFoundError


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    """Session-scoped local SparkSession with minimal parallelism for fast tests."""
    from src.spark_session import create_spark_session, find_java_executable

    try:
        find_java_executable()
    except JavaNotFoundError as exc:
        pytest.skip(f"Spark tests skipped: {exc}")

    base = get_settings()
    settings = Settings(
        raw_data_path=base.raw_data_path,
        processed_data_path=tmp_path_factory.mktemp("processed"),
        reports_path=tmp_path_factory.mktemp("reports"),
        log_dir=tmp_path_factory.mktemp("logs"),
        log_level="WARNING",
        spark_master="local[1]",
        spark_app_name="SalesDataPipelineTests",
        spark_shuffle_partitions=1,
        spark_log_level="ERROR",
        output_write_mode="local",
    )
    session = create_spark_session(settings)
    yield session
    session.stop()


def raw_row(**overrides) -> dict:
    """A valid raw sales row; override any field to build edge cases."""
    row = {
        "order_id": "ORD-1",
        "order_date": "2024-03-15",
        "customer_id": "CUST-1",
        "product_id": "P1",
        "product_name": "Wireless Mouse",
        "category": "Electronics",
        "region": "North",
        "quantity": 2,
        "unit_price": 10.0,
        "discount": 0.1,
        "payment_method": "UPI",
    }
    row.update(overrides)
    return row


@pytest.fixture
def make_raw_df(spark):
    """Factory building a raw DataFrame (as produced by ingestion) from dict rows."""

    from pyspark.sql import functions as F

    from src.schemas import RAW_SALES_SCHEMA

    def _make(rows: list[dict]):
        df = spark.createDataFrame(rows, schema=RAW_SALES_SCHEMA)

        return df.withColumn("is_malformed", F.lit(False))

    return _make


@pytest.fixture
def transformed_rows() -> list[dict]:
    """Small, already-cleaned dataset used by aggregation tests."""
    return [
        # order_id, date, customer, product, name, category, region, qty, price, discount
        raw_row(order_id="O1", order_date="2024-01-05", customer_id="C1", product_id="P1",
                product_name="Mouse", category="Electronics", region="North", quantity=2, unit_price=10.0, discount=0.0),
        raw_row(order_id="O1", order_date="2024-01-05", customer_id="C1", product_id="P2",
                product_name="Chair", category="Furniture", region="North", quantity=1, unit_price=100.0, discount=0.1),
        raw_row(order_id="O2", order_date="2024-02-10", customer_id="C2", product_id="P1",
                product_name="Mouse", category="Electronics", region="South", quantity=3, unit_price=10.0, discount=0.0),
        raw_row(order_id="O3", order_date="2024-02-20", customer_id="GUEST", product_id="P3",
                product_name="Shirt", category="Clothing", region="South", quantity=4, unit_price=25.0, discount=0.2),
    ]


@pytest.fixture
def sales_df(make_raw_df, transformed_rows):
    """Transformed DataFrame (dates parsed, metrics added) for aggregation tests."""
    from src.data_cleaning import parse_order_dates
    from src.data_transformation import transform_sales_data

    cleaned = parse_order_dates(make_raw_df(transformed_rows).drop("is_malformed"))
    transformed, _ = transform_sales_data(cleaned)
    return transformed
