"""Generate a reproducible raw sales dataset with intentional data-quality issues.

Usage:
    python scripts/generate_sample_data.py
    python scripts/generate_sample_data.py --orders 800 --seed 7 --output data/raw/sales_data.csv
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import random
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.config import get_settings  # noqa: E402

COLUMNS = [
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
]

# product_id: (product_name, category, base_price)
PRODUCT_CATALOG = {
    "P1001": ("Wireless Mouse", "Electronics", 24.99),
    "P1002": ("Mechanical Keyboard", "Electronics", 89.99),
    "P1003": ("27-inch Monitor", "Electronics", 249.00),
    "P1004": ("Noise Cancelling Headphones", "Electronics", 199.50),
    "P1005": ("USB-C Hub", "Electronics", 39.95),
    "P1006": ("Bluetooth Speaker", "Electronics", 59.00),
    "P1007": ("Smartphone Stand", "Electronics", 15.49),
    "P2001": ("Office Chair", "Furniture", 179.00),
    "P2002": ("Standing Desk", "Furniture", 429.00),
    "P2003": ("Bookshelf", "Furniture", 119.99),
    "P2004": ("Filing Cabinet", "Furniture", 149.00),
    "P2005": ("Coffee Table", "Furniture", 99.00),
    "P3001": ("Cotton T-Shirt", "Clothing", 14.99),
    "P3002": ("Denim Jeans", "Clothing", 49.99),
    "P3003": ("Winter Jacket", "Clothing", 129.00),
    "P3004": ("Running Shoes", "Clothing", 84.50),
    "P3005": ("Hoodie", "Clothing", 39.99),
    "P3006": ("Formal Shirt", "Clothing", 34.99),
    "P4001": ("Leather Wallet", "Accessories", 29.99),
    "P4002": ("Backpack", "Accessories", 54.99),
    "P4003": ("Wrist Watch", "Accessories", 149.99),
    "P4004": ("Sunglasses", "Accessories", 79.00),
    "P4005": ("Laptop Sleeve", "Accessories", 22.49),
    "P4006": ("Water Bottle", "Accessories", 12.99),
}
REGIONS = ["North", "South", "East", "West"]
REGION_WEIGHTS = [0.3, 0.25, 0.25, 0.2]
PAYMENT_METHODS = ["Credit Card", "Debit Card", "UPI", "Net Banking", "Cash", "PayPal"]
DISCOUNTS = [0.0, 0.0, 0.0, 0.05, 0.1, 0.15, 0.2, 0.25]
START_DATE = dt.date(2024, 1, 1)
END_DATE = dt.date(2025, 6, 30)


def _random_date(rng: random.Random) -> dt.date:
    return START_DATE + dt.timedelta(days=rng.randint(0, (END_DATE - START_DATE).days))


def _format_date(rng: random.Random, value: dt.date) -> str:
    """Mostly ISO dates, with some alternative (but valid) formats."""
    roll = rng.random()
    if roll < 0.04:
        return value.strftime("%Y/%m/%d")
    if roll < 0.07:
        return value.strftime("%d-%m-%Y")
    return value.isoformat()


def generate_clean_rows(rng: random.Random, orders: int, customers: int) -> list[dict]:
    """Generate valid order lines; each order contains 1-3 distinct products."""
    rows = []
    product_ids = list(PRODUCT_CATALOG)
    for index in range(1, orders + 1):
        order_id = f"ORD-{index:06d}"
        order_date = _random_date(rng)
        customer_id = f"CUST-{rng.randint(1, customers):04d}"
        region = rng.choices(REGIONS, REGION_WEIGHTS)[0]
        payment = rng.choice(PAYMENT_METHODS)
        line_count = rng.choices([1, 2, 3], [0.6, 0.3, 0.1])[0]
        for product_id in rng.sample(product_ids, line_count):
            name, category, base_price = PRODUCT_CATALOG[product_id]
            price = round(base_price * rng.uniform(0.95, 1.05), 2)
            rows.append(
                {
                    "order_id": order_id,
                    "order_date": _format_date(rng, order_date),
                    "customer_id": customer_id,
                    "product_id": product_id,
                    "product_name": name,
                    "category": category,
                    "region": region,
                    "quantity": str(rng.choices([1, 2, 3, 4, 5, 8, 10], [35, 25, 15, 10, 7, 5, 3])[0]),
                    "unit_price": f"{price:.2f}",
                    "discount": str(rng.choice(DISCOUNTS)),
                    "payment_method": payment,
                }
            )
    return rows


def _mess_casing(rng: random.Random, value: str) -> str:
    return rng.choice([value.lower(), value.upper(), f"  {value} ", value.swapcase()])


def inject_quality_issues(rng: random.Random, rows: list[dict]) -> dict[str, int]:
    """Mutate rows in place to introduce realistic data-quality problems."""
    issues = {key: 0 for key in (
        "missing_customer_id", "missing_product_name", "missing_category", "invalid_quantity",
        "invalid_price", "inconsistent_casing", "invalid_date", "invalid_discount",
    )}
    for row in rows:
        if rng.random() < 0.04:
            row["customer_id"] = rng.choice(["", "NULL", "N/A"])
            issues["missing_customer_id"] += 1
        if rng.random() < 0.03:
            row["product_name"] = ""
            issues["missing_product_name"] += 1
        if rng.random() < 0.03:
            row["category"] = rng.choice(["", "null"])
            issues["missing_category"] += 1
        if rng.random() < 0.025:
            row["quantity"] = rng.choice(["-1", "-3", "0", "two"])
            issues["invalid_quantity"] += 1
        if rng.random() < 0.025:
            row["unit_price"] = rng.choice(["-15.00", "0", "", "abc"])
            issues["invalid_price"] += 1
        if rng.random() < 0.12:
            if row["category"]:
                row["category"] = _mess_casing(rng, row["category"])
            row["region"] = _mess_casing(rng, row["region"])
            issues["inconsistent_casing"] += 1
        if rng.random() < 0.02:
            row["order_date"] = rng.choice(["2024-13-45", "2025-02-30", "not_a_date", "", "31/31/2024"])
            issues["invalid_date"] += 1
        if rng.random() < 0.02:
            row["discount"] = rng.choice(["15", "-0.1", "1.5", ""])
            issues["invalid_discount"] += 1
    return issues


def add_duplicates(rng: random.Random, rows: list[dict]) -> tuple[list[dict], int, int]:
    """Append exact duplicates and business-key duplicates.

    Business-key duplicates repeat an existing (order_id, product_id) line but with
    different casing and a missing customer ID, as a re-sent / partial record would.
    """
    exact = [dict(row) for row in rng.sample(rows, max(1, len(rows) // 40))]
    near = []
    for row in rng.sample(rows, max(1, len(rows) // 100)):
        copy = dict(row)
        copy["region"] = copy["region"].upper()
        copy["customer_id"] = ""
        near.append(copy)
    combined = rows + exact + near
    rng.shuffle(combined)
    return combined, len(exact), len(near)


def generate_dataset(output: Path, orders: int = 700, customers: int = 250, seed: int = 42) -> dict:
    """Generate the dataset and write it to ``output``. Returns generation statistics."""
    rng = random.Random(seed)
    rows = generate_clean_rows(rng, orders, customers)
    issues = inject_quality_issues(rng, rows)
    rows, exact_dupes, near_dupes = add_duplicates(rng, rows)

    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    # A couple of structurally malformed lines (wrong column count / broken values).
    with output.open("a", newline="", encoding="utf-8") as handle:
        handle.write("ORD-999998,2024-05-05,CUST-0001,P1001,Wireless Mouse,Electronics\n")
        handle.write("ORD-999999,2024-05-06,CUST-0002,P1002,Mechanical Keyboard,Electronics,North,"
                     "three,ninety,0.1,UPI,EXTRA\n")

    return {"rows": len(rows) + 2, "exact_duplicates": exact_dupes,
            "business_key_duplicates": near_dupes, "malformed_lines": 2, **issues}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate sample raw sales data")
    parser.add_argument("--output", type=Path, default=None, help="Output CSV (default: RAW_DATA_PATH)")
    parser.add_argument("--orders", type=int, default=700, help="Number of orders (each has 1-3 lines)")
    parser.add_argument("--customers", type=int, default=250, help="Number of distinct customers")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output = args.output or get_settings().raw_data_path
    if output.suffix.lower() != ".csv":
        output = output / "sales_data.csv"
    stats = generate_dataset(output, args.orders, args.customers, args.seed)
    print(f"Sample data written to {output}")
    for key, value in stats.items():
        print(f"  {key:<26} {value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
