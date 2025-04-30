import logging
import uuid
from datetime import datetime
from typing import Any, Dict, Optional

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

# Placeholder type for job status enum (matches the one in migration)
JobStatus = str  # Could use Literal['queued', 'processing', 'completed', 'failed', 'completed_with_errors']


async def create_job(pool: AsyncConnectionPool) -> uuid.UUID:
    """Creates a new job record with status 'queued' and returns its UUID."""
    sql = """
        INSERT INTO jobs (status)
        VALUES ('queued')
        RETURNING job_id;
    """
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql)
            result = await cur.fetchone()
            if result:
                logger.info("Created job.", extra={"job_id": result[0]})
                return result[0]
            else:
                # This case should ideally not happen with RETURNING
                logger.error("Failed to create job or retrieve job_id")
                raise RuntimeError("Failed to create job")


async def get_job_by_id(job_id: uuid.UUID, pool: AsyncConnectionPool) -> Optional[Dict[str, Any]]:
    """Retrieves a job record by its UUID."""
    sql = "SELECT * FROM jobs WHERE job_id = %s AND deleted_at IS NULL;"
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, (job_id,))
            result = await cur.fetchone()
            return result


async def update_job_status(
    pool: AsyncConnectionPool,
    job_id: uuid.UUID,
    status: str,
    started_at: Optional[datetime] = None,
    completed_at: Optional[datetime] = None,
    processing_seconds: Optional[float] = None,
    duration_seconds: Optional[float] = None,
    error_message: Optional[str] = None,
    doc_id: Optional[uuid.UUID] = None,
) -> bool:
    """Updates the status and associated details of a job."""
    fields_to_update = {
        "status": status,
        "started_at": started_at,
        "completed_at": completed_at,
        "processing_seconds": processing_seconds,
        "duration_seconds": duration_seconds,
        "error_message": error_message,
        "doc_id": doc_id,
    }
    # Filter out None values unless it's started_at or completed_at (which can be set to NULL)
    updates = {
        k: v
        for k, v in fields_to_update.items()
        if v is not None
        or k
        in [
            "started_at",
            "completed_at",
            "processing_seconds",
            "duration_seconds",
            "error_message",
            "doc_id",
        ]
    }

    if not updates:
        logger.warning(f"No fields provided to update for job {job_id}")
        return False

    set_clauses = ", ".join([f"{k} = %s" for k in updates.keys()])
    sql = f"""
        UPDATE jobs
        SET {set_clauses}
        WHERE job_id = %s;
    """
    values = list(updates.values()) + [job_id]

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, tuple(values), prepare=False)
            if cur.rowcount > 0:
                logger.debug(
                    "Updated job status.", extra={"job_id": job_id, "status": status}
                )
                return True
            else:
                logger.warning(
                    "Job not found or no changes made during status update.",
                    extra={"job_id": job_id},
                )
                return False


async def soft_delete_job(job_id: uuid.UUID, pool: AsyncConnectionPool) -> bool:
    """Marks a job as deleted by setting the deleted_at timestamp."""
    sql = """
        UPDATE jobs
        SET deleted_at = CURRENT_TIMESTAMP
        WHERE job_id = %s AND deleted_at IS NULL;
    """
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (job_id,))
            if cur.rowcount > 0:
                logger.info("Soft deleted job.", extra={"job_id": job_id})
                return True
            else:
                logger.warning(
                    "Job not found or already deleted.", extra={"job_id": job_id}
                )
                return False
