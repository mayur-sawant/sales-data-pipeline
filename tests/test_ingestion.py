"""Tests for CSV ingestion and input validation."""

from __future__ import annotations

import pytest

from src.data_ingestion import load_sales_data
from src.exceptions import InputDataError, SchemaValidationError

HEADER = "order_id,order_date,customer_id,product_id,product_name,category,region,quantity,unit_price,discount,payment_method"


def test_missing_file_raises_clear_error(spark, tmp_path):
    with pytest.raises(InputDataError, match="not found"):
        load_sales_data(spark, tmp_path / "missing.csv")


def test_missing_required_columns(spark, tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("order_id,order_date\nO1,2024-01-01\n", encoding="utf-8")
    with pytest.raises(SchemaValidationError, match="missing required columns"):
        load_sales_data(spark, path)


def test_header_only_file_raises(spark, tmp_path):
    path = tmp_path / "empty.csv"
    path.write_text(HEADER + "\n", encoding="utf-8")
    with pytest.raises(InputDataError, match="no data rows"):
        load_sales_data(spark, path)


def test_loads_with_explicit_schema_and_flags_malformed_rows(spark, tmp_path):
    path = tmp_path / "sales.csv"
    path.write_text(
        HEADER + "\n"
        "O1,2024-01-01,C1,P1,Mouse,Electronics,North,2,10.5,0.1,UPI\n"
        "O2,2024-01-02,C2,P1,Mouse,Electronics,South,two,10.5,0.1,UPI\n",
        encoding="utf-8",
    )
    result = load_sales_data(spark, path)
    assert result.record_count == 2
    assert result.malformed_count == 1
    types = dict(result.dataframe.dtypes)
    assert types["quantity"] == "int" and types["unit_price"] == "double"
    rows = {r.order_id: r for r in result.dataframe.collect()}
    assert rows["O1"].quantity == 2 and rows["O1"].is_malformed is False
    assert rows["O2"].quantity is None and rows["O2"].is_malformed is True


def test_column_order_follows_header(spark, tmp_path):
    columns = HEADER.split(",")
    reordered = list(reversed(columns))
    values = dict(zip(columns, "O1,2024-01-01,C1,P1,Mouse,Electronics,North,3,9.99,0,UPI".split(",")))
    path = tmp_path / "reordered.csv"
    path.write_text(",".join(reordered) + "\n" + ",".join(values[c] for c in reordered) + "\n", encoding="utf-8")

    row = load_sales_data(spark, path).dataframe.first()
    assert (row.order_id, row.quantity, row.unit_price) == ("O1", 3, 9.99)
