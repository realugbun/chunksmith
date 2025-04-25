import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, AnyHttpUrl


# Base model for shared fields if needed
class BaseAPIModel(BaseModel):
    pass


# ======== Ingestion ========#


class IngestTextPayload(BaseAPIModel):
    text: str = Field(
        ...,
        description="Raw text content to ingest.",
        examples=["This is the document content."],
    )
    filename: Optional[str] = Field(
        None,
        description="Optional filename for context/metadata.",
        examples=["my_document.txt"],
    )
    callback_url: Optional[AnyHttpUrl] = Field(
        None,
        description="Optional callback URL for webhook notification upon job completion/failure.",
        examples=["https://yourapp.com/webhook-handler"],
    )
    callback_secret: Optional[str] = Field(
        None,
        description="Secret for HMAC signature verification (required if callback_url is provided). Must be at least 32 bytes long.",
        examples=["your-super-secret-string-here"],
    )


class IngestResponse(BaseAPIModel):
    job_id: uuid.UUID
    status: Literal["queued"]
    submitted_at: datetime


# ======== Jobs ========#

JobStatusEnum = Literal[
    "queued", "processing", "completed", "failed", "completed_with_errors"
]


class JobStatusResponse(BaseAPIModel):
    job_id: uuid.UUID
    status: JobStatusEnum
    queued_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    queue_seconds: Optional[float] = None
    processing_seconds: Optional[float] = None
    duration_seconds: Optional[float] = None
    error_message: Optional[str] = None
    doc_id: Optional[uuid.UUID] = None


# ======== Documents & Chunks ========#


class FullTextResponse(BaseAPIModel):
    doc_id: uuid.UUID
    text: str
    created_at: datetime


class Chunk(BaseAPIModel):
    chunk_id: uuid.UUID
    doc_id: uuid.UUID
    page: Optional[int] = None
    offset: Optional[int] = None
    byte_length: Optional[int] = None
    text: str
    sequence: Optional[int] = None
    metadata: Optional[Dict[str, Any]] = {}
    created_at: datetime


class ChunkListResponse(BaseAPIModel):
    chunks: List[Chunk]
    total_chunks: Optional[int] = None
    limit: int
    offset: int
    next_offset: Optional[int] = None


# ======== Health ========#


class HealthResponse(BaseAPIModel):
    status: Literal["ok", "unhealthy"]
    uptime: str
    db_status: Optional[Literal["ok", "error"]] = None
