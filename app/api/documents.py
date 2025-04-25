import logging
import uuid
import asyncio
from fastapi import APIRouter, Depends, HTTPException, Query

from api import models
from core.config import settings
from db.documents import (
    get_document_by_id,
    get_document_text_and_created_at,
)
from db.chunks import (
    get_chunks_by_doc_id,
    count_chunks_by_doc_id,
)
from api.deps import get_correlation_id

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/documents/{doc_id}/fulltext", response_model=models.FullTextResponse)
async def get_document_full_text(
    doc_id: uuid.UUID, correlation_id: str | None = Depends(get_correlation_id)
):
    """
    **Step 3a: Retrieve Full Text**

    Retrieves the complete extracted text content of a successfully processed document.

    **Workflow:**
    1. Obtain the `doc_id` from the `/v1/jobs/{job_id}` endpoint (Step 2) once the job status is `"completed"`.
    2. Call this endpoint using the `doc_id` to get the full text.
    """
    logger.info(
        f"Request for full text: {doc_id}",
        extra={"correlation_id": correlation_id, "doc_id": doc_id},
    )

    doc_data = await get_document_text_and_created_at(doc_id)

    if not doc_data:
        logger.warning(
            "Document not found for full text request.",
            extra={"correlation_id": correlation_id, "doc_id": doc_id},
        )
        raise HTTPException(status_code=404, detail="Document not found")

    full_text = doc_data.get("full_text")
    created_at = doc_data.get("created_at")

    if full_text is None:
        logger.warning(
            f"Full text not available for document: {doc_id}",
            extra={"correlation_id": correlation_id, "doc_id": doc_id},
        )
        raise HTTPException(
            status_code=404, detail="Full text not available for this document"
        )

    if created_at is None:
        logger.error(
            f"Document record for {doc_id} is missing created_at timestamp!",
            extra={"correlation_id": correlation_id, "doc_id": doc_id},
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error: Missing document creation timestamp",
        )

    return models.FullTextResponse(doc_id=doc_id, text=full_text, created_at=created_at)


@router.get("/documents/{doc_id}/chunks", response_model=models.ChunkListResponse)
async def get_document_chunks(
    doc_id: uuid.UUID,
    limit: int = Query(
        settings.CHUNK_DEFAULT_LIMIT,
        ge=1,
        le=500,
        description="Number of chunks per page",
    ),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    correlation_id: str | None = Depends(get_correlation_id),
):
    """
    **Step 3b: Retrieve Document Chunks**

    Retrieves a paginated list of processed text chunks for a successfully processed document.

    **Workflow:**
    1. Obtain the `doc_id` from the `/v1/jobs/{job_id}` endpoint (Step 2) once the job status is `"completed"`.
    2. Call this endpoint using the `doc_id` to get the chunks. Use `limit` and `offset` for pagination if needed.
    """
    logger.info(
        "Request for chunks.",
        extra={
            "correlation_id": correlation_id,
            "doc_id": doc_id,
            "limit": limit,
            "offset": offset,
        },
    )

    # Fetch chunks and total count concurrently
    chunks_data, total_count = await asyncio.gather(
        get_chunks_by_doc_id(doc_id, limit, offset), count_chunks_by_doc_id(doc_id)
    )

    # Check if document exists if count is 0
    if total_count == 0:
        doc = await get_document_by_id(doc_id)
        if not doc:
            logger.warning(
                "Document not found.",
                extra={"correlation_id": correlation_id, "doc_id": doc_id},
            )
            raise HTTPException(status_code=404, detail="Document not found")
        else:
            logger.info(
                "Document exists but has 0 chunks.",
                extra={"correlation_id": correlation_id, "doc_id": doc_id},
            )

    # Calculate next_offset
    next_offset = offset + limit if (offset + limit) < total_count else None
    chunks = [models.Chunk(**chunk_row) for chunk_row in chunks_data]

    return models.ChunkListResponse(
        chunks=chunks,
        total_chunks=total_count,
        limit=limit,
        offset=offset,
        next_offset=next_offset,
    )
