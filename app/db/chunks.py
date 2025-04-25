import logging
import uuid
import json
from typing import Any, Dict, List, Optional

from psycopg.rows import dict_row

from db.session import db_manager

logger = logging.getLogger(__name__)


async def create_chunk(
    doc_id: uuid.UUID,
    page: Optional[int],
    offset: Optional[int],
    byte_length: Optional[int],
    text: str,
    sequence: Optional[int],
    metadata: Optional[Dict[str, Any]],
) -> uuid.UUID:
    """Creates a new chunk record associated with a document and returns its UUID."""
    sql = """
        INSERT INTO chunks (doc_id, page, offset, byte_length, text, sequence, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING chunk_id;
    """
    values = (doc_id, page, offset, byte_length, text, sequence, metadata)
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, values)
            result = await cur.fetchone()
            if result:
                return result[0]
            else:
                logger.error(
                    "Failed to create chunk for document.", extra={"doc_id": doc_id}
                )
                raise RuntimeError("Failed to create chunk")


async def create_chunks_batch(chunks_data: List[Dict[str, Any]]) -> int:
    """Creates multiple chunks by executing individual INSERTs in a loop.
    Returns number of chunks inserted.
    """
    if not chunks_data:
        return 0

    sql = """
        INSERT INTO chunks (doc_id, page, "offset", byte_length, text, sequence, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    inserted_count = 0
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.transaction():
            async with conn.cursor() as cur:
                for chunk_dict in chunks_data:
                    values = (
                        chunk_dict.get("doc_id"),
                        chunk_dict.get("page"),
                        chunk_dict.get("offset"),
                        chunk_dict.get("byte_length"),
                        chunk_dict.get("text"),
                        chunk_dict.get("sequence"),
                        json.dumps(chunk_dict.get("metadata"))
                        if chunk_dict.get("metadata") is not None
                        else None,
                    )
                    try:
                        await cur.execute(sql, values, prepare=False)
                        inserted_count += 1
                    except Exception as e:
                        logger.error(
                            "Failed to insert chunk.",
                            extra={
                                "sequence": chunk_dict.get("sequence"),
                                "doc_id": chunk_dict.get("doc_id"),
                                "error": str(e),
                            },
                        )
                        raise

            logger.info(
                "Loop finished inserting chunks.", extra={"count": inserted_count}
            )
            return inserted_count


async def get_chunks_by_doc_id(
    doc_id: uuid.UUID, limit: int = 50, offset: int = 0
) -> List[Dict[str, Any]]:
    """Retrieves a paginated list of chunks for a given document ID, ordered by sequence."""
    sql = """
        SELECT *
        FROM chunks
        WHERE doc_id = %s AND deleted_at IS NULL
        ORDER BY sequence, page, "offset"
        LIMIT %s OFFSET %s;
    """
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(sql, (doc_id, limit, offset))
            results = await cur.fetchall()
            return results


async def count_chunks_by_doc_id(doc_id: uuid.UUID) -> int:
    """Counts the total number of non-deleted chunks for a given document ID."""
    sql = "SELECT COUNT(*) FROM chunks WHERE doc_id = %s AND deleted_at IS NULL;"
    pool = db_manager.pool
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(sql, (doc_id,))
            result = await cur.fetchone()
            return result[0] if result else 0
