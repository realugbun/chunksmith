import logging
from datetime import datetime, timezone
import uuid
import json
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Body, Form
import redis as sync_redis
from rq import Queue
from rq.job import Callback
from typing import Optional, List, Any, Tuple

from api import models
from core.config import settings
from core.redis_client import get_redis_connection, redis
from db.jobs import create_job
from api.deps import get_correlation_id
from workers.hooks import run_dispatch_webhook_sync

logger = logging.getLogger(__name__)
router = APIRouter()

# Minimum length for callback secrets
MIN_CALLBACK_SECRET_BYTES = 32

sync_redis_conn = sync_redis.Redis.from_url(settings.REDIS_URL)
q = Queue("chunksmith", connection=sync_redis_conn)


async def _enqueue_ingestion_task(
    correlation_id: Optional[str],
    task_name: str,
    worker_args_list: List[Any],
    log_msg: str,
    callback_url: Optional[str] = None,
    callback_secret: Optional[str] = None,
) -> models.IngestResponse:
    """
    Creates a job record, stores callback details in Redis (if provided),
    enqueues the specified task with optional hooks, and returns the response.
    Handles exceptions during DB interaction, Redis, or queueing.
    """
    job_uuid: uuid.UUID
    job_id_str: str
    callback_stored = False
    log_extra_base = {"correlation_id": correlation_id}

    # 1. Create initial job record in DB
    try:
        job_uuid = await create_job()
        job_id_str = str(job_uuid)
        log_extra = {**log_extra_base, "job_id": job_id_str}
        logger.info("Created job record", extra=log_extra)
    except Exception:
        logger.exception("Failed to create job record", extra=log_extra_base)
        raise HTTPException(status_code=500, detail="Failed to initiate ingestion job.")

    # Update log_extra now that job_id is available
    log_extra = {**log_extra_base, "job_id": job_id_str}

    # 2. Store callback details in Redis if provided
    redis_conn: redis.Redis | None = None
    if callback_url and callback_secret:
        try:
            redis_conn = await get_redis_connection()
            callback_key = f"callback:{job_id_str}"
            callback_data = json.dumps({"url": callback_url, "secret": callback_secret})
            await redis_conn.set(
                callback_key, callback_data, ex=settings.REDIS_QUEUE_TIMEOUT_SECONDS
            )
            callback_stored = True
            logger.info("Stored callback details in Redis", extra=log_extra)
        except json.JSONDecodeError:
            logger.exception("Failed to serialize callback details", extra=log_extra)
            raise HTTPException(
                status_code=500, detail="Failed to process callback details."
            )
        except Exception:
            logger.exception(
                "Failed to store callback details in Redis", extra=log_extra
            )
            raise HTTPException(
                status_code=500, detail="Failed to store callback details."
            )

    # 3. Prepare and Enqueue Task
    worker_args_list.insert(0, job_uuid)
    worker_args_tuple: Tuple[Any, ...] = tuple(worker_args_list)

    logger.info(log_msg, extra=log_extra)
    try:
        rq_job_options = {
            "job_id": job_id_str,
            "on_success": None,
            "on_failure": None,
        }
        # Add callbacks ONLY if details were successfully stored
        if callback_stored:
            rq_job_options["on_success"] = Callback(run_dispatch_webhook_sync)
            rq_job_options["on_failure"] = Callback(run_dispatch_webhook_sync)
            logger.debug(
                "Adding success/failure sync callback wrapper to job", extra=log_extra
            )

        q.enqueue_call(
            func=task_name,
            args=worker_args_tuple,
            kwargs={},
            timeout=settings.REDIS_QUEUE_TIMEOUT_SECONDS,
            **rq_job_options,
        )
        logger.debug("Successfully enqueued job.", extra=log_extra)
    except Exception:
        logger.exception("Failed to enqueue job.", extra=log_extra)
        raise HTTPException(status_code=500, detail="Failed to enqueue background job.")

    # 4. Return Accepted response
    return models.IngestResponse(
        job_id=job_uuid, status="queued", submitted_at=datetime.now(timezone.utc)
    )


# URL Validation Helper
def _validate_callback_url(url: str):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in ["http", "https"]:
            return False
        if not parsed.netloc:
            return False
        return True
    except ValueError:
        return False


# Shared Callback Validation Logic (for Form data)
def _validate_callback_params(
    callback_url: Optional[str] = None, callback_secret: Optional[str] = None
) -> Tuple[Optional[str], Optional[str]]:
    """Validates callback URL and secret, primarily for Form inputs."""
    validated_url: Optional[str] = None
    validated_secret: Optional[str] = None
    log_extra = {"callback_url_provided": bool(callback_url)}

    if callback_url:
        # Secret must be present if URL is provided (essential for Form)
        if not callback_secret:
            logger.warning("Callback URL provided without secret.", extra=log_extra)
            raise HTTPException(
                status_code=422,
                detail="callback_secret is required when callback_url is provided.",
            )

        # Validate URL format
        if not _validate_callback_url(callback_url):
            logger.warning("Invalid callback URL format provided.", extra=log_extra)
            raise HTTPException(
                status_code=422,
                detail="Invalid callback_url format or scheme. Must be HTTP/HTTPS.",
            )

        # Validate secret length
        if len(callback_secret.encode("utf-8")) < MIN_CALLBACK_SECRET_BYTES:
            logger.warning("Callback secret too short.", extra=log_extra)
            raise HTTPException(
                status_code=422,
                detail=f"callback_secret must be at least {MIN_CALLBACK_SECRET_BYTES} bytes long.",
            )

        validated_url = callback_url
        validated_secret = callback_secret
        logger.debug("Callback URL and secret validated successfully.", extra=log_extra)
    elif callback_secret:
        # Secret provided without URL - might indicate user error
        logger.warning(
            "Callback secret provided without a callback URL.", extra=log_extra
        )
        raise HTTPException(
            status_code=422, detail="callback_secret provided without callback_url."
        )

    return validated_url, validated_secret


@router.post("/ingest/text", response_model=models.IngestResponse, status_code=202)
async def ingest_text(
    payload: models.IngestTextPayload = Body(...),
    correlation_id: Optional[str] = Depends(get_correlation_id),
):
    """
    **Step 1: Ingest Text Data**

    Ingests raw text data provided in a JSON body for asynchronous chunking and processing.

    - Accepts `application/json` with `text` and optional `filename` fields.
    - Creates a background job and returns a `job_id` immediately.
    - Optionally accepts `callback_url` and `callback_secret` in the JSON body to enable webhook notifications upon job completion or failure.
      The webhook POST request body will conform to the `JobStatusResponse` schema.

    **Workflow:**
    1. Call this endpoint with your text data (and optionally callback details).
    2. Use the returned `job_id` to poll the `/v1/jobs/{job_id}` endpoint (Step 2) to check processing status (unless using webhooks).

    **Webhook Parameters (Optional):**
    - `callback_url`: An HTTP/HTTPS URL to POST job status updates to.
    - `callback_secret`: A secret string used to sign the webhook payload (required if `callback_url` is provided).
    """
    log_extra = {"correlation_id": correlation_id, "source_filename": payload.filename}
    logger.info("Text ingest request received.", extra=log_extra)

    validated_callback_url: Optional[str] = (
        str(payload.callback_url) if payload.callback_url else None
    )
    validated_callback_secret: Optional[str] = payload.callback_secret

    if validated_callback_url:
        logger.debug("Callback URL and secret provided via payload.", extra=log_extra)

    task_name = "workers.worker.process_document_text"
    worker_args = [correlation_id, payload.text, payload.filename]
    log_msg = "Enqueuing job for text payload"

    return await _enqueue_ingestion_task(
        correlation_id=correlation_id,
        task_name=task_name,
        worker_args_list=worker_args,
        log_msg=log_msg,
        callback_url=validated_callback_url,
        callback_secret=validated_callback_secret,
    )


@router.post("/ingest/file", response_model=models.IngestResponse, status_code=202)
async def ingest_file(
    file: UploadFile = File(
        ..., description="Document file to be chunked and processed."
    ),
    callback_url: Optional[str] = Form(
        None,
        description=(
            "Optional callback URL for webhook notification upon job completion/failure. "
            "Must be HTTP/HTTPS. The webhook POST request body will conform to the JobStatusResponse schema."
        ),
        examples=["https://yourapp.com/webhook-handler"],
    ),
    callback_secret: Optional[str] = Form(
        None,
        description=f"Secret for HMAC signature verification (required if callback_url is provided, min {MIN_CALLBACK_SECRET_BYTES} bytes).",
        examples=["your-super-secret-string-here"],
    ),
    correlation_id: Optional[str] = Depends(get_correlation_id),
):
    """
    **Step 1: Ingest File Data**

    Ingests a document file uploaded via `multipart/form-data` for asynchronous chunking and processing.

    - Accepts `multipart/form-data` with a `file` field.
    - Creates a background job and returns a `job_id` immediately.
    - Optionally accepts `callback_url` and `callback_secret` as form fields to enable webhook notifications upon job completion or failure.
      The webhook POST request body will conform to the `JobStatusResponse` schema.

    **Supported Formats & Processing:**
    The service accepts various file types. Optical Character Recognition (OCR) is performed on PDFs and images. All supported formats are converted to Markdown internally before chunking.
    - **PDF**: OCR is applied.
    - **DOCX, XLSX, PPTX**: Standard MS Office formats.
    - **Markdown**: `.md`
    - **AsciiDoc**: `.adoc`, `.asciidoc`
    - **HTML, XHTML**: `.html`, `.htm`
    - **CSV**: `.csv`
    - **Images**: `PNG`, `JPEG`, `TIFF`, `BMP` (OCR is applied).

    **Workflow:**
    1. Call this endpoint with your file data (and optionally callback details as form fields).
    2. Use the returned `job_id` to poll the `/v1/jobs/{job_id}` endpoint (Step 2) to check processing status (unless using webhooks).

    **Webhook Parameters (Optional):**
    - `callback_url`: An HTTP/HTTPS URL (form field) to POST job status updates to.
    - `callback_secret`: A secret string (form field) used to sign the webhook payload (required if `callback_url` is provided).
    """
    filename = file.filename
    content_type = file.content_type
    log_extra_base = {
        "correlation_id": correlation_id,
        "source_filename": filename,
        "content_type": content_type,
    }

    logger.info("File ingest request received.", extra=log_extra_base)

    # Use the shared validation function for Form parameters
    try:
        validated_callback_url, validated_callback_secret = _validate_callback_params(
            callback_url, callback_secret
        )
        if validated_callback_url:
            logger.debug(
                "Callback URL and secret provided via form and validated.",
                extra=log_extra_base,
            )
    except HTTPException as e:
        # Re-raise validation errors from the helper
        raise e
    except Exception:
        # Catch unexpected errors during validation
        logger.exception(
            "Unexpected error during callback parameter validation.",
            extra=log_extra_base,
        )
        raise HTTPException(
            status_code=500, detail="Error validating callback parameters."
        )

    file_content: bytes
    try:
        file_content = await file.read()
        if not file_content:
            logger.error("Uploaded file is empty.", extra=log_extra_base)
            raise HTTPException(status_code=400, detail="Uploaded file is empty.")
        logger.info(
            f"Read {len(file_content)} bytes from uploaded file", extra=log_extra_base
        )
    except Exception:
        logger.exception("Failed to read uploaded file", extra=log_extra_base)
        raise HTTPException(
            status_code=500, detail="Failed to read uploaded file content."
        )
    finally:
        await file.close()

    task_name = "workers.worker.process_document_file"
    worker_args = [correlation_id, file_content, filename, content_type]
    log_msg = f"Enqueuing job for file upload: {filename}"

    return await _enqueue_ingestion_task(
        correlation_id=correlation_id,
        task_name=task_name,
        worker_args_list=worker_args,
        log_msg=log_msg,
        callback_url=validated_callback_url,
        callback_secret=validated_callback_secret,
    )


# Add New API Endpoint for URL
@router.post("/ingest/url", response_model=models.IngestResponse, status_code=202)
async def ingest_url(
    payload: models.IngestUrlPayload = Body(...),
    correlation_id: Optional[str] = Depends(get_correlation_id),
):
    """
    **Step 1: Ingest Data from URL**

    Ingests document content directly from a provided URL for asynchronous chunking and processing.

    - Accepts `application/json` with a `url` field.
    - Creates a background job and returns a `job_id` immediately.
    - Optionally accepts `callback_url` and `callback_secret` in the JSON body to enable webhook notifications upon job completion or failure, conforming to the `JobStatusResponse` schema.

    **Workflow:**
    1. Call this endpoint with the URL (and optionally callback details).
    2. Use the returned `job_id` to poll the `/v1/jobs/{job_id}` endpoint (Step 2) to check processing status (unless using webhooks).

    **Webhook Parameters (Optional):**
    - `callback_url`: An HTTP/HTTPS URL to POST job status updates to.
    - `callback_secret`: A secret string used to sign the webhook payload (required if `callback_url` is provided).
    """
    log_extra = {"correlation_id": correlation_id, "source_url": str(payload.url)}
    logger.info("URL ingest request received.", extra=log_extra)

    # Pydantic model (CallbackMixin) handles validation (required secret, format)
    validated_callback_url: Optional[str] = (
        str(payload.callback_url) if payload.callback_url else None
    )
    validated_callback_secret: Optional[str] = payload.callback_secret

    if validated_callback_url:
        logger.debug("Callback URL and secret provided via payload.", extra=log_extra)

    task_name = "workers.worker.process_document_url"
    worker_args = [correlation_id, str(payload.url)]
    log_msg = f"Enqueuing job for URL: {payload.url}"

    return await _enqueue_ingestion_task(
        correlation_id=correlation_id,
        task_name=task_name,
        worker_args_list=worker_args,
        log_msg=log_msg,
        callback_url=validated_callback_url,
        callback_secret=validated_callback_secret,
    )
