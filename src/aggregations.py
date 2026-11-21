"""Analytical aggregations using both the DataFrame API and Spark SQL.

Revenue throughout the project means ``net_sales`` (after discounts).

* DataFrame API: sales by category, top products
* Spark SQL:     sales by region, monthly sales, overall summary
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.schemas import UNKNOWN_CUSTOMER

logger = logging.getLogger(__name__)

SALES_VIEW = "sales"

REGION_SALES_SQL = f"""
SELECT
    region,
    ROUND(SUM(net_sales), 2)                                     AS total_revenue,
    COUNT(DISTINCT order_id)                                     AS total_orders,
    ROUND(SUM(net_sales) / COUNT(DISTINCT order_id), 2)          AS average_order_value
FROM {SALES_VIEW}
GROUP BY region
ORDER BY total_revenue DESC
"""

MONTHLY_SALES_SQL = f"""
SELECT
    order_year                       AS year,
    order_month                      AS month,
    ROUND(SUM(net_sales), 2)         AS total_revenue,
    COUNT(DISTINCT order_id)         AS total_orders
FROM {SALES_VIEW}
GROUP BY order_year, order_month
ORDER BY year, month
"""

OVERALL_SUMMARY_SQL = f"""
SELECT
    ROUND(COALESCE(SUM(net_sales), 0), 2)                                       AS total_revenue,
    ROUND(COALESCE(SUM(gross_amount), 0), 2)                                    AS total_gross_sales,
    ROUND(COALESCE(SUM(discount_amount), 0), 2)                                 AS total_discount,
    COALESCE(SUM(quantity), 0)                                                  AS total_quantity,
    COUNT(DISTINCT order_id)                                                    AS total_orders,
    COUNT(DISTINCT CASE WHEN customer_id <> '{UNKNOWN_CUSTOMER}' THEN customer_id END)
                                                                                AS total_customers,
    COUNT(DISTINCT product_id)                                                  AS total_products,
    ROUND(COALESCE(TRY_DIVIDE(SUM(net_sales), COUNT(DISTINCT order_id)), 0), 2) AS average_order_value,
    CAST(MIN(order_date) AS STRING)                                             AS first_order_date,
    CAST(MAX(order_date) AS STRING)                                             AS last_order_date
FROM {SALES_VIEW}
"""


@dataclass
class AggregationResults:
    sales_by_category: DataFrame
    sales_by_region: DataFrame
    monthly_sales: DataFrame
    top_products: DataFrame
    overall_summary: dict


def register_sales_view(df: DataFrame, view_name: str = SALES_VIEW) -> None:
    """Expose the transformed dataset to Spark SQL as a temporary view."""
    df.createOrReplaceTempView(view_name)


def sales_by_category(df: DataFrame) -> DataFrame:
    """Revenue, quantity, orders and average order value per category (DataFrame API)."""
    return (
        df.groupBy("category")
        .agg(
            F.round(F.sum("net_sales"), 2).alias("total_revenue"),
            F.sum("quantity").alias("total_quantity"),
            F.countDistinct("order_id").alias("total_orders"),
        )
        .withColumn("average_order_value", F.round(F.col("total_revenue") / F.col("total_orders"), 2))
        .orderBy(F.desc("total_revenue"))
    )


def top_products(df: DataFrame, limit: int = 10) -> DataFrame:
    """Top products by revenue (DataFrame API)."""
    return (
        df.groupBy("product_id", "product_name", "category")
        .agg(
            F.sum("quantity").alias("quantity_sold"),
            F.round(F.sum("net_sales"), 2).alias("total_revenue"),
        )
        .orderBy(F.desc("total_revenue"), F.asc("product_id"))
        .limit(limit)
    )


def sales_by_region(spark: SparkSession) -> DataFrame:
    """Revenue, orders and average order value per region (Spark SQL)."""
    return spark.sql(REGION_SALES_SQL)


def monthly_sales(spark: SparkSession) -> DataFrame:
    """Revenue and orders per calendar month (Spark SQL)."""
    return spark.sql(MONTHLY_SALES_SQL)


def overall_summary(spark: SparkSession) -> dict:
    """Overall KPIs (Spark SQL). Returns a plain dict — a single aggregated row."""
    row = spark.sql(OVERALL_SUMMARY_SQL).first()
    return row.asDict() if row else {}


def build_aggregations(spark: SparkSession, df: DataFrame) -> AggregationResults:
    """Compute every analytical summary from the transformed dataset."""
    register_sales_view(df)
    results = AggregationResults(
        sales_by_category=sales_by_category(df),
        sales_by_region=sales_by_region(spark),
        monthly_sales=monthly_sales(spark),
        top_products=top_products(df),
        overall_summary=overall_summary(spark),
    )
    logger.info("Aggregations completed: category, region, monthly, top_products, overall_summary")
    return results
