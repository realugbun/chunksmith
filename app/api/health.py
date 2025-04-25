import logging
import time
from fastapi import APIRouter, Response
from prometheus_client import generate_latest

from api import models
from core import metrics
from db.session import db_manager

logger = logging.getLogger(__name__)
router = APIRouter()

# Store startup time
START_TIME = time.time()


@router.get("/health", response_model=models.HealthResponse)
async def get_health():
    """Returns the current health status and uptime of the service."""
    uptime_seconds = int(time.time() - START_TIME)
    logger.debug("Health check requested")
    db_status = "ok"
    overall_status = "ok"
    try:
        if db_manager.pool is None:
            raise RuntimeError("Database pool not initialized")
        async with db_manager.pool.connection() as conn:
            await conn.execute("SELECT 1")
        logger.debug("Database health check successful.")
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        db_status = "error"
        overall_status = "unhealthy"

    return models.HealthResponse(
        status=overall_status, uptime=f"{uptime_seconds}s", db_status=db_status
    )


@router.get("/v1/metrics", include_in_schema=False)
async def get_metrics():
    """Returns service metrics in Prometheus format."""
    logger.debug("Metrics requested")
    return Response(
        content=generate_latest(metrics.REGISTRY),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )
