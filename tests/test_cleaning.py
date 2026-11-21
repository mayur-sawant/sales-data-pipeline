"""Tests for data cleaning: duplicates, missing values, normalisation and validation."""

from __future__ import annotations

import datetime as dt

from pyspark.sql import functions as F

from src.data_cleaning import (
    clean_sales_data,
    fill_missing_customers,
    impute_from_product_catalog,
    impute_invalid_prices,
    normalize_text_columns,
    parse_order_dates,
    remove_business_key_duplicates,
    remove_exact_duplicates,
    standardize_nulls,
)
from src.schemas import UNKNOWN_CATEGORY, UNKNOWN_CUSTOMER, UNKNOWN_PRODUCT, UNKNOWN_REGION
from tests.conftest import raw_row


def test_standardize_nulls_converts_placeholders(make_raw_df):
    df = make_raw_df([
        raw_row(customer_id="N/A"),
        raw_row(customer_id="  "),
        raw_row(customer_id="NULL"),
        raw_row(customer_id=" CUST-9 "),
    ])
    values = [r.customer_id for r in standardize_nulls(df).collect()]
    assert values == [None, None, None, "CUST-9"]


def test_normalize_text_columns_fixes_casing(make_raw_df):
    df = make_raw_df([
        raw_row(category="ELECTRONICS", region="north"),
        raw_row(category="furniture", region="SOUTH"),
        raw_row(category="Toys", region="Mars"),
    ])
    rows = normalize_text_columns(standardize_nulls(df)).collect()
    assert [(r.category, r.region) for r in rows] == [
        ("Electronics", "North"),
        ("Furniture", "South"),
        (None, UNKNOWN_REGION),
    ]


def test_parse_order_dates_handles_formats_and_invalid_values(make_raw_df):
    df = make_raw_df([
        raw_row(order_date="2024-03-15"),
        raw_row(order_date="2024/03/16"),
        raw_row(order_date="17-03-2024"),
        raw_row(order_date="2024-13-45"),
        raw_row(order_date="2025-02-30"),
        raw_row(order_date="not_a_date"),
    ])
    dates = [r.order_date for r in parse_order_dates(df).collect()]
    assert dates == [dt.date(2024, 3, 15), dt.date(2024, 3, 16), dt.date(2024, 3, 17), None, None, None]


def test_remove_exact_duplicates(make_raw_df):
    df = make_raw_df([raw_row(), raw_row(), raw_row(order_id="ORD-2")])
    assert remove_exact_duplicates(df.drop("is_malformed")).count() == 2


def test_business_key_duplicates_keep_most_complete_row(make_raw_df):
    df = make_raw_df([
        raw_row(order_id="ORD-1", customer_id=None),
        raw_row(order_id="ORD-1", customer_id="CUST-1"),
        raw_row(order_id="ORD-1", product_id="P2"),  # different line of the same order
    ]).drop("is_malformed")
    result = remove_business_key_duplicates(df).collect()
    assert len(result) == 2
    p1 = next(r for r in result if r.product_id == "P1")
    assert p1.customer_id == "CUST-1"


def test_missing_product_attributes_imputed_from_catalog(make_raw_df):
    df = make_raw_df([
        raw_row(product_id="P1", product_name="Wireless Mouse", category="Electronics"),
        raw_row(order_id="ORD-2", product_id="P1", product_name=None, category=None),
        raw_row(order_id="ORD-3", product_id="P9", product_name=None, category=None),
    ]).drop("is_malformed")
    df = impute_from_product_catalog(df, "product_name", UNKNOWN_PRODUCT)
    df = impute_from_product_catalog(df, "category", UNKNOWN_CATEGORY)
    rows = {r.order_id: r for r in df.collect()}
    assert (rows["ORD-2"].product_name, rows["ORD-2"].category) == ("Wireless Mouse", "Electronics")
    assert (rows["ORD-3"].product_name, rows["ORD-3"].category) == (UNKNOWN_PRODUCT, UNKNOWN_CATEGORY)


def test_missing_customer_ids_become_guest(make_raw_df):
    df = make_raw_df([raw_row(customer_id=None), raw_row(order_id="ORD-2")])
    values = sorted(r.customer_id for r in fill_missing_customers(df).collect())
    assert values == ["CUST-1", UNKNOWN_CUSTOMER]


def test_invalid_prices_imputed_with_product_median(make_raw_df):
    df = make_raw_df([
        raw_row(order_id="A", unit_price=10.0),
        raw_row(order_id="B", unit_price=12.0),
        raw_row(order_id="C", unit_price=-5.0),
        raw_row(order_id="D", unit_price=None),
        raw_row(order_id="E", product_id="P9", unit_price=0.0),
    ]).drop("is_malformed")
    prices = {r.order_id: r.unit_price for r in impute_invalid_prices(df).collect()}
    assert prices["C"] == prices["D"] == 11.0
    assert prices["E"] is None  # no valid reference price -> cannot be recovered


def test_clean_sales_data_end_to_end(make_raw_df):
    rows = [
        raw_row(order_id="ORD-1"),
        raw_row(order_id="ORD-1"),  # exact duplicate
        raw_row(order_id="ORD-2", category="electronics", region=" SOUTH "),
        raw_row(order_id="ORD-3", customer_id=""),
        raw_row(order_id="ORD-4", quantity=-2),
        raw_row(order_id="ORD-5", quantity=None),
        raw_row(order_id="ORD-6", order_date="2024-02-31"),
        raw_row(order_id=None),
        raw_row(order_id="ORD-7", product_id="P9", unit_price=-1.0),
    ]
    result = clean_sales_data(make_raw_df(rows))
    report = result.report

    assert report.records_before == 9
    assert report.steps["exact_duplicates_removed"] == 1
    assert report.steps["missing_customer_ids_filled"] == 1
    assert report.rejection_reasons == {
        "invalid_order_date": 1,
        "invalid_quantity": 2,
        "invalid_unit_price": 1,
        "missing_order_id": 1,
    }
    assert report.records_after == 3
    assert report.records_before == (
        report.records_after + report.records_rejected + report.steps["exact_duplicates_removed"]
        + report.steps["business_key_duplicates_removed"]
    )

    cleaned = {r.order_id: r for r in result.cleaned.collect()}
    assert set(cleaned) == {"ORD-1", "ORD-2", "ORD-3"}
    assert (cleaned["ORD-2"].category, cleaned["ORD-2"].region) == ("Electronics", "South")
    assert cleaned["ORD-3"].customer_id == UNKNOWN_CUSTOMER
    assert result.rejected.filter(F.col("rejection_reason").isNull()).count() == 0
