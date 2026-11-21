"""API tests using FastAPI's TestClient against small fixture files (no Spark needed)."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from src import config
from src.api.dependencies import get_repository
from src.api.main import app
from src.api.services import SalesDataRepository

CLEANED_CSV = """order_id,order_date,customer_id,product_id,product_name,category,region,quantity,unit_price,discount,payment_method,gross_amount,discount_amount,net_sales,order_year,order_month,order_quarter
O1,2024-01-05,C1,P1,Mouse,Electronics,North,2,10.0,0.0,UPI,20.0,0.0,20.0,2024,1,1
O1,2024-01-05,C1,P2,Chair,Furniture,North,1,100.0,0.1,UPI,100.0,10.0,90.0,2024,1,1
O2,2024-02-10,C2,P1,Mouse,Electronics,South,3,10.0,0.0,Cash,30.0,0.0,30.0,2024,2,1
O3,2024-02-20,GUEST,P3,Shirt,Clothing,South,4,25.0,0.2,,100.0,20.0,80.0,2024,2,1
"""
FILES = {
    config.CLEANED_SALES_FILE: CLEANED_CSV,
    config.SALES_BY_CATEGORY_FILE: (
        "category,total_revenue,total_quantity,total_orders,average_order_value\n"
        "Furniture,90.0,1,1,90.0\nClothing,80.0,4,1,80.0\nElectronics,50.0,5,2,25.0\n"
    ),
    config.SALES_BY_REGION_FILE: (
        "region,total_revenue,total_orders,average_order_value\nNorth,110.0,1,110.0\nSouth,110.0,2,55.0\n"
    ),
    config.MONTHLY_SALES_FILE: "year,month,total_revenue,total_orders\n2024,1,110.0,1\n2024,2,110.0,2\n",
    config.TOP_PRODUCTS_FILE: (
        "product_id,product_name,category,quantity_sold,total_revenue\n"
        "P2,Chair,Furniture,1,90.0\nP3,Shirt,Clothing,4,80.0\nP1,Mouse,Electronics,5,50.0\n"
    ),
}
SUMMARY = {
    "total_revenue": 220.0, "total_gross_sales": 250.0, "total_discount": 30.0, "total_quantity": 10,
    "total_orders": 3, "total_customers": 2, "total_products": 3, "average_order_value": 73.33,
    "first_order_date": "2024-01-05", "last_order_date": "2024-02-20",
    "generated_at": "2025-01-01T00:00:00+00:00",
}


@pytest.fixture
def processed_dir(tmp_path):
    for name, content in FILES.items():
        (tmp_path / name).write_text(content, encoding="utf-8")
    (tmp_path / config.OVERALL_SUMMARY_FILE).write_text(json.dumps(SUMMARY), encoding="utf-8")
    return tmp_path


def _client_for(directory) -> TestClient:
    app.dependency_overrides[get_repository] = lambda: SalesDataRepository(directory)
    return TestClient(app)


@pytest.fixture
def client(processed_dir):
    yield _client_for(processed_dir)
    app.dependency_overrides.clear()


@pytest.fixture
def empty_client(tmp_path):
    yield _client_for(tmp_path / "nothing_here")
    app.dependency_overrides.clear()


def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "Sales Data Processing Pipeline API"
    assert "/sales/summary" in body["endpoints"]
    assert body["docs_url"].endswith("/docs")


def test_health_ok(client):
    body = client.get("/health").json()
    assert body["status"] == "ok"
    assert all(body["data_available"].values())


def test_health_degraded_without_data(empty_client):
    response = empty_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "degraded"


def test_summary(client):
    body = client.get("/sales/summary").json()
    assert body["total_revenue"] == 220.0
    assert body["total_customers"] == 2


def test_category(client):
    body = client.get("/sales/category").json()
    assert [row["category"] for row in body] == ["Furniture", "Clothing", "Electronics"]
    assert body[2]["average_order_value"] == 25.0


def test_region(client):
    body = client.get("/sales/region").json()
    assert {row["region"]: row["total_orders"] for row in body} == {"North": 1, "South": 2}


def test_monthly(client):
    body = client.get("/sales/monthly").json()
    assert [(r["year"], r["month"]) for r in body] == [(2024, 1), (2024, 2)]


def test_top_products(client):
    body = client.get("/sales/top-products").json()
    assert body[0] == {"product_id": "P2", "product_name": "Chair", "category": "Furniture",
                       "quantity_sold": 1, "total_revenue": 90.0}


def test_transactions_pagination(client):
    body = client.get("/sales/transactions", params={"limit": 2, "offset": 1}).json()
    assert (body["total"], body["count"], body["limit"], body["offset"]) == (4, 2, 2, 1)
    assert [(i["order_id"], i["product_id"]) for i in body["items"]] == [("O1", "P2"), ("O2", "P1")]


def test_transactions_filters_are_case_insensitive(client):
    body = client.get("/sales/transactions", params={"category": "electronics", "region": "SOUTH"}).json()
    assert body["total"] == 1
    assert body["items"][0]["order_id"] == "O2"


def test_transactions_handles_null_payment_method(client):
    body = client.get("/sales/transactions", params={"region": "south"}).json()
    o3 = next(item for item in body["items"] if item["order_id"] == "O3")
    assert o3["payment_method"] is None


def test_transactions_invalid_params(client):
    assert client.get("/sales/transactions", params={"limit": 0}).status_code == 422
    assert client.get("/sales/transactions", params={"limit": 501}).status_code == 422
    assert client.get("/sales/transactions", params={"offset": -1}).status_code == 422


@pytest.mark.parametrize(
    "endpoint",
    ["/sales/summary", "/sales/category", "/sales/region", "/sales/monthly", "/sales/top-products", "/sales/transactions"],
)
def test_missing_data_returns_503(empty_client, endpoint):
    response = empty_client.get(endpoint)
    assert response.status_code == 503
    assert "run_pipeline.py" in response.json()["detail"]


def test_openapi_docs_available(client):
    assert client.get("/docs").status_code == 200
    assert "/sales/transactions" in client.get("/openapi.json").json()["paths"]
