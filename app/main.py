import logging
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
from yoyo import get_backend, read_migrations

from api import ingest, jobs, documents, health
from core.config import settings
from core.logging import setup_logging
from core.middleware import CorrelationIdMiddleware, AuthMiddleware
from core import metrics
from db.session import db_manager

# Setup logging first
setup_logging()
logger = logging.getLogger(__name__)


def run_migrations():
    """Applies pending database migrations."""
    logger.info("Attempting to apply database migrations...")
    try:
        backend = get_backend(settings.POSTGRES_URL)
        migrations_path = "migrations/versions"
        migrations = read_migrations(migrations_path)
        with backend.lock():
            backend.apply_migrations(backend.to_apply(migrations))
        logger.info("Database migrations applied successfully.")
    except Exception as e:
        logger.exception("Database migration failed.")
        raise RuntimeError("Database migration failed") from e


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Application startup...")
    # 1. Initialize DB Pool
    await db_manager.initialize(settings)
    # 2. Run Migrations (blocking startup on failure)
    run_migrations()
    logger.info("Application startup complete.")
    yield
    # Shutdown
    logger.info("Application shutdown...")
    await db_manager.close()
    logger.info("Application shutdown complete.")


app = FastAPI(
    title="ChunkSmith API",
    description="""
Service for ingesting and chunking documents.

**Authentication:** Most endpoints require Bearer token authentication.
Include the token in the `Authorization` header: `Authorization: Bearer YOUR_TOKEN`.

The following paths are public and do not require authentication:
- `/health`
- `/metrics`
- `/v1/metrics`
- `/openapi.json`
- `/docs`
- `/redoc`
""",
    version="0.1.0",
    lifespan=lifespan,
)

# Add Middleware (order matters: outer first)
app.add_middleware(CorrelationIdMiddleware)
app.add_middleware(AuthMiddleware, required_token=settings.AUTH_TOKEN)

# Instrument FastAPI for Prometheus metrics
Instrumentator(registry=metrics.REGISTRY).instrument(app).expose(
    app, endpoint="/metrics", include_in_schema=False
)

app.include_router(health.router, tags=["Health"])
app.include_router(ingest.router, prefix="/v1", tags=["Ingestion"])
app.include_router(jobs.router, prefix="/v1", tags=["Jobs"])
app.include_router(documents.router, prefix="/v1", tags=["Documents"])

if __name__ == "__main__":
    logger.info("Starting Uvicorn server for local development...")
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        log_level=settings.LOG_LEVEL.lower(),
    )
