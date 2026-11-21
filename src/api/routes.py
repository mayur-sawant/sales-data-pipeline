"""HTTP routes. Routes only translate HTTP <-> repository calls; no business logic."""

from __future__ import annotations

import datetime as dt

from fastapi import APIRouter, Depends, Query, Request, status

from src import __version__
from src.api.dependencies import get_repository
from src.api.schemas import (
    AppInfo,
    CategorySales,
    ErrorResponse,
    HealthResponse,
    MonthlySales,
    OverallSummary,
    PaginatedTransactions,
    RegionSales,
    TopProduct,
)
from src.api.services import SalesDataRepository

DATA_ERRORS = {status.HTTP_503_SERVICE_UNAVAILABLE: {"model": ErrorResponse, "description": "Processed data not available"}}

router = APIRouter()
sales_router = APIRouter(prefix="/sales", tags=["Sales analytics"], responses=DATA_ERRORS)


@router.get("/", response_model=AppInfo, tags=["Info"])
def root(request: Request) -> AppInfo:
    """Welcome message and application information."""
    return AppInfo(
        name="Sales Data Processing Pipeline API",
        version=__version__,
        description="Sales analytics served from PySpark-processed data.",
        docs_url=str(request.url_for("swagger_ui_html")),
        endpoints=sorted(path for path in request.app.openapi()["paths"] if path != "/"),
    )


@router.get("/health", response_model=HealthResponse, tags=["Info"])
def health(repository: SalesDataRepository = Depends(get_repository)) -> HealthResponse:
    """API health. Reports 'degraded' when processed outputs are missing."""
    available = repository.available_files()
    return HealthResponse(
        status="ok" if all(available.values()) else "degraded",
        timestamp=dt.datetime.now(dt.timezone.utc),
        data_available=available,
    )


@sales_router.get("/summary", response_model=OverallSummary)
def sales_summary(repository: SalesDataRepository = Depends(get_repository)) -> dict:
    """Overall sales KPIs."""
    return repository.overall_summary()


@sales_router.get("/category", response_model=list[CategorySales])
def sales_by_category(repository: SalesDataRepository = Depends(get_repository)) -> list[dict]:
    """Revenue, quantity, orders and average order value per product category."""
    return repository.sales_by_category()


@sales_router.get("/region", response_model=list[RegionSales])
def sales_by_region(repository: SalesDataRepository = Depends(get_repository)) -> list[dict]:
    """Revenue, orders and average order value per region."""
    return repository.sales_by_region()


@sales_router.get("/monthly", response_model=list[MonthlySales])
def monthly_sales(repository: SalesDataRepository = Depends(get_repository)) -> list[dict]:
    """Revenue and orders per calendar month."""
    return repository.monthly_sales()


@sales_router.get("/top-products", response_model=list[TopProduct])
def top_products(repository: SalesDataRepository = Depends(get_repository)) -> list[dict]:
    """Top 10 products by revenue."""
    return repository.top_products()


@sales_router.get("/transactions", response_model=PaginatedTransactions)
def transactions(
    limit: int = Query(50, ge=1, le=500, description="Page size (1-500)"),
    offset: int = Query(0, ge=0, description="Number of records to skip"),
    category: str | None = Query(None, description="Filter by category (case-insensitive)"),
    region: str | None = Query(None, description="Filter by region (case-insensitive)"),
    repository: SalesDataRepository = Depends(get_repository),
) -> PaginatedTransactions:
    """Cleaned transactions with pagination and optional category / region filters."""
    total, items = repository.transactions(limit=limit, offset=offset, category=category, region=region)
    return PaginatedTransactions(total=total, limit=limit, offset=offset, count=len(items), items=items)


router.include_router(sales_router)
