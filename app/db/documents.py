import logging
import uuid
from typing import Any, Dict, Optional

from psycopg.rows import dict_row

from db.session import db_manager

logger = logging.getLogger(__name__)


async def create_document(
    job_id: uuid.UUID,
    filename: Optional[str],
    content_type: Optional[str],
    file_path: Optional[str],
    full_text: Optional[str],
    page_count: Optional[int],
) -> uuid.UUID:
    """Creates a new document record associated with a job and returns its UUID."""
    insert_sql = """
        INSERT INTO documents (job_id, filename, content_type, file_path, full_text, page_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING doc_id;
    """
    values = (job_id, filename, content_type, file_path, full_text, page_count)
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(insert_sql, values, prepare=False)
            result = await cur.fetchone()
            if result:
                doc_id = result[0]
                update_job_sql = "UPDATE jobs SET doc_id = %s WHERE job_id = %s"
                await cur.execute(update_job_sql, (doc_id, job_id), prepare=False)
                logger.info(
                    "Created document.", extra={"doc_id": doc_id, "job_id": job_id}
                )
                return doc_id
            else:
                logger.error("Failed to create document.", extra={"job_id": job_id})
                raise RuntimeError("Failed to create document")


async def get_document_by_id(doc_id: uuid.UUID) -> Optional[Dict[str, Any]]:
    """Retrieves a document record by its UUID."""
    sql = "SELECT * FROM documents WHERE doc_id = %s AND deleted_at IS NULL;"
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, (doc_id,))
            result = await cur.fetchone()
            return result


async def get_document_text_and_created_at(
    doc_id: uuid.UUID,
) -> Optional[Dict[str, Any]]:
    """Retrieves the full_text and created_at fields for a given document ID."""
    sql = "SELECT full_text, created_at FROM documents WHERE doc_id = %s AND deleted_at IS NULL;"
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, (doc_id,))
            result = await cur.fetchone()
            return result


async def soft_delete_document(doc_id: uuid.UUID) -> bool:
    """Marks a document and its associated chunks as deleted."""
    # Note: Chunks are deleted via CASCADE constraint defined in migration
    sql = """
        UPDATE documents
        SET deleted_at = CURRENT_TIMESTAMP
        WHERE doc_id = %s AND deleted_at IS NULL;
    """
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (doc_id,))
            if cur.rowcount > 0:
                logger.info(
                    "Soft deleted document (and cascaded to chunks).",
                    extra={"doc_id": doc_id},
                )
                return True
            else:
                logger.warning(
                    "Document not found or already deleted.", extra={"doc_id": doc_id}
                )
                return False
