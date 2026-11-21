"""Data-access layer for the API.

The API never starts Spark. It reads the small processed outputs produced by the
pipeline and caches them in memory. The cache is keyed on the file modification
time, so re-running the pipeline is picked up without restarting the server.
"""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pandas as pd

from src import config

ID_COLUMNS = {"order_id": str, "customer_id": str, "product_id": str}


class DataNotAvailableError(Exception):
    """Raised when a processed output file has not been generated yet."""

    def __init__(self, path: Path):
        self.path = path
        super().__init__(
            f"Processed data file '{path.name}' was not found. "
            "Run the ETL pipeline first: python scripts/run_pipeline.py"
        )


class SalesDataRepository:
    """Read-only access to processed sales outputs with mtime-based caching."""

    def __init__(self, processed_dir: Path):
        self.processed_dir = Path(processed_dir)
        self._cache: dict[str, tuple[float, Any]] = {}
        self._lock = threading.Lock()

    # -- low-level loading -------------------------------------------------
    def _path(self, file_name: str) -> Path:
        return self.processed_dir / file_name

    def _load(self, file_name: str, loader) -> Any:
        path = self._path(file_name)
        try:
            mtime = path.stat().st_mtime
        except FileNotFoundError as exc:
            raise DataNotAvailableError(path) from exc
        with self._lock:
            cached = self._cache.get(file_name)
            if cached and cached[0] == mtime:
                return cached[1]
            data = loader(path)
            self._cache[file_name] = (mtime, data)
            return data

    @staticmethod
    def _read_csv(path: Path) -> pd.DataFrame:
        return pd.read_csv(path, dtype=ID_COLUMNS, keep_default_na=True)

    @staticmethod
    def _read_json(path: Path) -> dict:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    @staticmethod
    def _records(df: pd.DataFrame) -> list[dict]:
        return df.astype(object).where(pd.notna(df), None).to_dict(orient="records")

    def _table(self, file_name: str) -> list[dict]:
        return self._load(file_name, lambda p: self._records(self._read_csv(p)))

    # -- public API --------------------------------------------------------
    def available_files(self) -> dict[str, bool]:
        names = [
            config.CLEANED_SALES_FILE,
            config.SALES_BY_CATEGORY_FILE,
            config.SALES_BY_REGION_FILE,
            config.MONTHLY_SALES_FILE,
            config.TOP_PRODUCTS_FILE,
            config.OVERALL_SUMMARY_FILE,
        ]
        return {name: self._path(name).is_file() for name in names}

    def overall_summary(self) -> dict:
        return self._load(config.OVERALL_SUMMARY_FILE, self._read_json)

    def sales_by_category(self) -> list[dict]:
        return self._table(config.SALES_BY_CATEGORY_FILE)

    def sales_by_region(self) -> list[dict]:
        return self._table(config.SALES_BY_REGION_FILE)

    def monthly_sales(self) -> list[dict]:
        return self._table(config.MONTHLY_SALES_FILE)

    def top_products(self) -> list[dict]:
        return self._table(config.TOP_PRODUCTS_FILE)

    def _transactions_frame(self) -> pd.DataFrame:
        def loader(path: Path) -> pd.DataFrame:
            df = self._read_csv(path)
            df["_category_key"] = df["category"].str.lower()
            df["_region_key"] = df["region"].str.lower()
            return df.sort_values(["order_date", "order_id", "product_id"], kind="stable").reset_index(drop=True)

        return self._load(config.CLEANED_SALES_FILE, loader)

    def transactions(
        self,
        limit: int,
        offset: int,
        category: str | None = None,
        region: str | None = None,
    ) -> tuple[int, list[dict]]:
        """Return (total matching rows, one page of rows) with optional filters."""
        df = self._transactions_frame()
        mask = pd.Series(True, index=df.index)
        if category:
            mask &= df["_category_key"] == category.strip().lower()
        if region:
            mask &= df["_region_key"] == region.strip().lower()
        filtered = df[mask]
        page = filtered.iloc[offset : offset + limit].drop(columns=["_category_key", "_region_key"])
        return len(filtered), self._records(page)
