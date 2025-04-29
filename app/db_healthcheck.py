import asyncio
import logging
import sys
import os

from db.session import db_manager
from core.config import settings

# Set up logging for the script
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(), # Default to INFO
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("db_healthcheck")

async def check_db_connection():
    """Checks the database connection by executing a simple query."""
    try:
        logger.debug("Initializing database manager for healthcheck...")
        await db_manager.initialize(settings)
        logger.debug("Database manager initialized (or was already initialized).")

        logger.info("Attempting to acquire database connection and run healthcheck query...")
        async with asyncio.timeout(delay=4): # Slightly less than Docker timeout
            async with db_manager.pool.connection() as conn:
                await conn.execute("SELECT 1")
        logger.info("Healthcheck query successful.")
        return True
    except asyncio.TimeoutError:
        logger.error("Database healthcheck failed: Timeout acquiring connection or executing query.")
        return False
    except Exception as e:
        logger.error(f"Database healthcheck failed: {type(e).__name__}: {e}", exc_info=False)
        logger.debug("Full exception info:", exc_info=True)
        return False
    finally:
        # Important: Do NOT close the pool here (db_manager.close()).
        # The main worker process manages the pool lifecycle.
        # Closing it here would break the worker.
        logger.debug("Healthcheck finished, pool remains open for worker.")

async def main():
    """Main async function to run the check and exit with appropriate code."""
    logger.info("Running DB healthcheck...")
    is_healthy = await check_db_connection()

    if is_healthy:
        logger.info("Database connection healthy.")
        sys.exit(0) 
    else:
        logger.error("Database connection unhealthy.")
        sys.exit(1) 

if __name__ == "__main__":
    asyncio.run(main()) 