import asyncio
import logging
from typing import Optional

from psycopg_pool import AsyncConnectionPool

from core.config import Settings

logger = logging.getLogger(__name__)


class DatabaseConnectionManager:
    _instance: Optional["DatabaseConnectionManager"] = None
    _lock = asyncio.Lock()

    def __new__(cls):
        # Basic Singleton implementation (thread-safe due to GIL, async safety via lock later)
        if cls._instance is None:
            cls._instance = super(DatabaseConnectionManager, cls).__new__(cls)
            cls._instance._pool: Optional[AsyncConnectionPool] = None
        return cls._instance

    async def initialize(self, settings: Settings):
        async with self._lock:
            if self._pool is not None:
                logger.warning("Database pool already initialized.")
                return

            try:
                self._pool = AsyncConnectionPool(
                    conninfo=settings.POSTGRES_URL,
                    min_size=5,
                    max_size=20,
                    open=True,
                )
                async with self._pool.connection() as conn:
                    await conn.execute("SELECT 1")
                logger.info("Database connection pool initialized successfully.")
            except Exception:
                logger.exception("Failed to initialize database connection pool")
                self._pool = None
                raise

    async def close(self):
        async with self._lock:
            if self._pool:
                logger.info("Closing database connection pool.")
                await self._pool.close()
                self._pool = None
                logger.info("Database connection pool closed.")
            else:
                logger.warning(
                    "Attempted to close an already closed or uninitialized pool."
                )

    @property
    def pool(self) -> AsyncConnectionPool:
        if self._pool is None:
            logger.error("Database pool accessed before initialization.")
            raise RuntimeError("Database pool has not been initialized.")
        return self._pool


# Create a single instance of the manager for the application to use
db_manager = DatabaseConnectionManager()
