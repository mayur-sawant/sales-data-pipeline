"""Tests for derived columns, revenue calculations and discount validation."""

from __future__ import annotations

import pytest

from src.data_cleaning import parse_order_dates
from src.data_output import write_dataframe_csv
from src.data_transformation import (
    add_sales_metrics,
    normalize_discount,
    profile_discounts,
    transform_sales_data,
    validate_sales_metrics,
)
from src.exceptions import DataValidationError
from tests.conftest import raw_row


def test_revenue_calculations(make_raw_df):
    df = make_raw_df([raw_row(quantity=3, unit_price=19.99, discount=0.15)])
    row = add_sales_metrics(df).first()
    assert row.gross_amount == pytest.approx(59.97)
    assert row.discount_amount == pytest.approx(9.0)  # 8.9955 rounded
    assert row.net_sales == pytest.approx(50.97)
    assert row.gross_amount - row.discount_amount == pytest.approx(row.net_sales)


def test_discount_normalization(make_raw_df):
    df = make_raw_df([
        raw_row(order_id="valid", discount=0.2),
        raw_row(order_id="missing", discount=None),
        raw_row(order_id="percent", discount=15.0),
        raw_row(order_id="negative", discount=-0.1),
        raw_row(order_id="too_high", discount=0.95),
        raw_row(order_id="nonsense", discount=500.0),
    ])
    report = profile_discounts(df)
    assert (report.missing_discounts, report.percentage_discounts_converted, report.invalid_discounts_reset) == (1, 1, 3)

    discounts = {r.order_id: r.discount for r in normalize_discount(df).collect()}
    assert discounts == {
        "valid": 0.2, "missing": 0.0, "percent": 0.15, "negative": 0.0, "too_high": 0.0, "nonsense": 0.0,
    }


def test_transform_adds_date_parts_and_columns(make_raw_df):
    df = parse_order_dates(make_raw_df([raw_row(order_date="2024-08-20")]).drop("is_malformed"))
    transformed, _ = transform_sales_data(df)
    row = transformed.first()
    assert (row.order_year, row.order_month, row.order_quarter) == (2024, 8, 3)
    assert {"gross_amount", "discount_amount", "net_sales"} <= set(transformed.columns)
    assert "is_malformed" not in transformed.columns


def test_validation_rejects_bad_monetary_values(make_raw_df):
    df = add_sales_metrics(make_raw_df([raw_row(discount=1.5)]))  # discount not normalised
    with pytest.raises(DataValidationError):
        validate_sales_metrics(df)


def test_local_csv_writer_streams_rows(sales_df, tmp_path):
    target = write_dataframe_csv(sales_df, tmp_path / "out" / "cleaned.csv", mode="local")
    lines = target.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("order_id,order_date,")
    assert len(lines) == 1 + sales_df.count()
    assert "2024-01-05" in lines[1]
