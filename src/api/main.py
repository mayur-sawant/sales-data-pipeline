"""FastAPI application factory.

Run with:  uvicorn src.api.main:app --reload
Docs at:   http://127.0.0.1:8000/docs
"""

from __future__ import annotations

import logging

from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from src import __version__
from src.api.routes import router
from src.api.services import DataNotAvailableError
from src.config import get_settings
from src.logging_config import setup_logging

logger = logging.getLogger(__name__)


def create_app() -> FastAPI:
    settings = get_settings()
    setup_logging(settings.log_level, settings.log_dir / "api.log")

    app = FastAPI(
        title="Sales Data Processing Pipeline API",
        description=(
            "REST API exposing sales analytics produced by a PySpark ETL pipeline. "
            "Run `python scripts/run_pipeline.py` to (re)generate the data."
        ),
        version=__version__,
    )

    @app.exception_handler(DataNotAvailableError)
    async def data_not_available_handler(request: Request, exc: DataNotAvailableError) -> JSONResponse:
        logger.warning("Data not available path=%s endpoint=%s", exc.path, request.url.path)
        return JSONResponse(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, content={"detail": str(exc)})

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("Unhandled API error endpoint=%s", request.url.path)
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": "Internal server error while reading processed data."},
        )

    app.include_router(router)
    return app


app = create_app()
