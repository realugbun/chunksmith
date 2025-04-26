import uuid
from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, AnyHttpUrl, field_validator, model_validator
from core.config import settings

MIN_CALLBACK_SECRET_BYTES = settings.MIN_CALLBACK_SECRET_BYTES


# Base model for shared fields if needed
class BaseAPIModel(BaseModel):
    pass


# ======== Callback Mixin ========#


class CallbackMixin(BaseModel):
    """Mixin for shared callback URL and secret fields."""

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

    @field_validator("callback_secret", mode="before")
    def check_secret_length(cls, v):
        """Validate secret length if provided."""
        if v and len(v.encode("utf-8")) < MIN_CALLBACK_SECRET_BYTES:
            raise ValueError(
                f"callback_secret must be at least {MIN_CALLBACK_SECRET_BYTES} bytes long."
            )
        return v  # Return original value for subsequent validation/assignment

    @model_validator(mode="after")
    def check_secret_present_if_url(self) -> "CallbackMixin":
        """Ensure secret is present if URL is provided."""
        if self.callback_url and not self.callback_secret:
            raise ValueError(
                "callback_secret is required when callback_url is provided."
            )
        return self


# ======== Ingestion ========#


class IngestTextPayload(BaseAPIModel, CallbackMixin):
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


class IngestUrlPayload(BaseAPIModel, CallbackMixin):
    """Payload for ingesting content from a URL."""

    url: AnyHttpUrl = Field(..., description="URL of the document to ingest.")


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
