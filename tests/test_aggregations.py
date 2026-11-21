"""Tests for DataFrame and Spark SQL aggregations on a tiny known dataset.

Dataset (net_sales): O1/P1 Electronics North 20 | O1/P2 Furniture North 90 |
                     O2/P1 Electronics South 30 | O3/P3 Clothing South 80 (GUEST)
"""

from __future__ import annotations

import pytest

from src.aggregations import (
    build_aggregations,
    monthly_sales,
    overall_summary,
    register_sales_view,
    sales_by_category,
    sales_by_region,
    top_products,
)


def _by_key(df, key):
    return {row[key]: row.asDict() for row in df.collect()}


def test_sales_by_category(sales_df):
    result = _by_key(sales_by_category(sales_df), "category")
    assert result["Electronics"] == {
        "category": "Electronics", "total_revenue": 50.0, "total_quantity": 5,
        "total_orders": 2, "average_order_value": 25.0,
    }
    assert result["Furniture"]["total_revenue"] == 90.0
    assert result["Clothing"]["total_revenue"] == 80.0
    # Ordered by revenue descending
    assert [r.category for r in sales_by_category(sales_df).collect()] == ["Furniture", "Clothing", "Electronics"]


def test_sales_by_region_sql(spark, sales_df):
    register_sales_view(sales_df)
    result = _by_key(sales_by_region(spark), "region")
    assert result["North"] == {"region": "North", "total_revenue": 110.0, "total_orders": 1, "average_order_value": 110.0}
    assert result["South"] == {"region": "South", "total_revenue": 110.0, "total_orders": 2, "average_order_value": 55.0}


def test_monthly_sales_sql(spark, sales_df):
    register_sales_view(sales_df)
    rows = [r.asDict() for r in monthly_sales(spark).collect()]
    assert rows == [
        {"year": 2024, "month": 1, "total_revenue": 110.0, "total_orders": 1},
        {"year": 2024, "month": 2, "total_revenue": 110.0, "total_orders": 2},
    ]


def test_top_products(sales_df):
    rows = top_products(sales_df, limit=2).collect()
    assert [(r.product_id, r.total_revenue) for r in rows] == [("P2", 90.0), ("P3", 80.0)]
    all_rows = _by_key(top_products(sales_df), "product_id")
    assert all_rows["P1"]["quantity_sold"] == 5


def test_overall_summary_excludes_guest_customers(spark, sales_df):
    register_sales_view(sales_df)
    summary = overall_summary(spark)
    assert summary["total_revenue"] == 220.0
    assert summary["total_orders"] == 3
    assert summary["total_customers"] == 2
    assert summary["total_products"] == 3
    assert summary["average_order_value"] == pytest.approx(73.33)
    assert (summary["first_order_date"], summary["last_order_date"]) == ("2024-01-05", "2024-02-20")


def test_build_aggregations_totals_are_consistent(spark, sales_df):
    results = build_aggregations(spark, sales_df)
    total = results.overall_summary["total_revenue"]
    assert sum(r.total_revenue for r in results.sales_by_category.collect()) == pytest.approx(total)
    assert sum(r.total_revenue for r in results.sales_by_region.collect()) == pytest.approx(total)
    assert sum(r.total_revenue for r in results.monthly_sales.collect()) == pytest.approx(total)
