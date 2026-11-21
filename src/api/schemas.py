"""Pydantic response models for the Sales Analytics API."""

from __future__ import annotations

import datetime as dt

from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
    detail: str


class AppInfo(BaseModel):
    name: str
    version: str
    description: str
    docs_url: str
    endpoints: list[str]


class HealthResponse(BaseModel):
    status: str = Field(description="'ok' when all processed data is available, otherwise 'degraded'")
    timestamp: dt.datetime
    data_available: dict[str, bool]


class OverallSummary(BaseModel):
    total_revenue: float
    total_gross_sales: float
    total_discount: float
    total_quantity: int
    total_orders: int
    total_customers: int
    total_products: int
    average_order_value: float
    first_order_date: dt.date | None = None
    last_order_date: dt.date | None = None
    generated_at: dt.datetime | None = None


class CategorySales(BaseModel):
    category: str
    total_revenue: float
    total_quantity: int
    total_orders: int
    average_order_value: float


class RegionSales(BaseModel):
    region: str
    total_revenue: float
    total_orders: int
    average_order_value: float


class MonthlySales(BaseModel):
    year: int
    month: int = Field(ge=1, le=12)
    total_revenue: float
    total_orders: int


class TopProduct(BaseModel):
    product_id: str
    product_name: str
    category: str
    quantity_sold: int
    total_revenue: float


class Transaction(BaseModel):
    order_id: str
    order_date: dt.date
    customer_id: str
    product_id: str
    product_name: str
    category: str
    region: str
    quantity: int
    unit_price: float
    discount: float
    payment_method: str | None = None
    gross_amount: float
    discount_amount: float
    net_sales: float
    order_year: int
    order_month: int
    order_quarter: int


class PaginatedTransactions(BaseModel):
    total: int = Field(description="Number of transactions matching the filters")
    limit: int
    offset: int
    count: int = Field(description="Number of transactions in this page")
    items: list[Transaction]
