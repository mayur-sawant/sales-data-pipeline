"""FastAPI dependency providers (overridable in tests)."""

from __future__ import annotations

from functools import lru_cache

from src.api.services import SalesDataRepository
from src.config import get_settings


@lru_cache(maxsize=1)
def get_repository() -> SalesDataRepository:
    """Return a process-wide repository bound to PROCESSED_DATA_PATH."""
    return SalesDataRepository(get_settings().processed_data_path)
