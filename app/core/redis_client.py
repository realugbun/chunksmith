import redis.asyncio as redis
from redis.asyncio.connection import ConnectionPool

from core.config import settings

# Global variable to hold the connection pool
_redis_pool: ConnectionPool | None = None


async def get_redis_connection() -> redis.Redis:
    """
    Get an async Redis connection from the connection pool.
    Initializes the pool on first call.
    """
    global _redis_pool
    if _redis_pool is None:
        _redis_pool = ConnectionPool.from_url(
            settings.REDIS_URL, decode_responses=False
        )

    # Create a connection from the pool
    return redis.Redis(connection_pool=_redis_pool)


async def close_redis_connection():
    """Closes the Redis connection pool during application shutdown."""
    global _redis_pool
    if _redis_pool:
        await _redis_pool.disconnect()
        _redis_pool = None
