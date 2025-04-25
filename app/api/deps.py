import logging
from typing import AsyncGenerator

from fastapi import Depends
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from core.logging import correlation_id_cv
from db.session import db_manager

logger = logging.getLogger(__name__)


async def get_db_pool() -> AsyncConnectionPool:
    """Dependency to get the initialized DB connection pool."""
    return db_manager.pool


async def get_db_connection(
    pool: AsyncConnectionPool = Depends(get_db_pool),
) -> AsyncGenerator[AsyncConnection, None]:
    """
    Dependency that provides a single database connection from the pool.
    Ensures the connection is returned to the pool.
    """
    async with pool.connection() as conn:
        try:
            yield conn
        except Exception:
            logger.exception("Error during database operation in request")
            raise


def get_correlation_id() -> str | None:
    """Dependency to retrieve the current correlation ID from context."""
    return correlation_id_cv.get()
