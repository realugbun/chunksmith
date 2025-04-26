import asyncio
import uuid
import logging
import sys
import time
import functools
from typing import Optional, Callable, Any
from datetime import datetime, timezone

from redis import Redis
from rq import Queue, Worker

from core.config import settings
from core.logging import setup_logging, correlation_id_cv
from db.session import db_manager
from db.jobs import update_job_status
from db.documents import create_document
from db.chunks import create_chunks_batch
from services.ingestion import ingestion_service, IngestionResult
from core.metrics import (
    DOCS_PROCESSED_COUNTER,
    CHUNKS_CREATED_COUNTER,
    JOBS_FAILED_COUNTER,
    JOB_PROCESSING_DURATION_SECONDS,
)

## NOTE: The service init and worker tasks need to be in the same file to prevent
##       issues with the RQ worker importing dependencies on each task.

setup_logging()

logger = logging.getLogger(__name__)

# Define the queues this worker will listen to
QUEUES_TO_LISTEN = ["chunksmith"]


def setup_task_logging(correlation_id: Optional[str]):
    """Sets up correlation ID for logging within the task."""
    correlation_id_cv.set(correlation_id)


# --- Worker Decorator --- #


def job_processor(func: Callable[..., Any]) -> Callable[..., None]:
    """Decorator for RQ worker functions to handle common logic:
    - Logging setup/teardown
    - Timing
    - Exception handling
    - Job status updates (processing, completed, failed)
    - Metrics recording
    """

    @functools.wraps(func)
    async def wrapper(
        job_uuid: uuid.UUID, correlation_id: Optional[str], *args, **kwargs
    ) -> None:
        setup_task_logging(correlation_id)
        job_id_str = str(job_uuid)
        start_time = time.time()
        final_status: str = "failed"
        error_message: Optional[str] = None
        doc_id: Optional[uuid.UUID] = None
        ingestion_result: Optional[IngestionResult] = None
        total_chunks: int = 0
        failure_reason = "unknown"

        # Determine source representation for logging
        source_repr = "unknown_source"
        if args:
            # Heuristic: first arg after correlation_id might be the main input
            # For URL: args[0] is the URL string
            # For Text: args[0] is the text string
            # For File: args[0] is bytes, args[1] is filename
            if func.__name__ == "process_document_url":
                source_repr = f"url: {args[0]}"
            elif func.__name__ == "process_document_text":
                filename = args[1] if len(args) > 1 else None
                source_repr = f"text_payload ({filename or 'no filename'})"
            elif func.__name__ == "process_document_file":
                filename = args[1] if len(args) > 1 else "unknown_file"
                source_repr = f"file: {filename}"
            # Add more specific checks if needed based on function signature

        log_extra = {
            "job_id": job_id_str,
            "correlation_id": correlation_id,
            "source": source_repr,
            "worker_function": func.__name__,
        }

        logger.info(f"Starting job processing ({func.__name__}).", extra=log_extra)

        try:
            # 1. Update Status to Processing
            await update_job_status(
                job_id=job_uuid,
                status="processing",
                started_at=datetime.now(timezone.utc),
            )
            logger.debug("Set job status to processing", extra=log_extra)

            # 2. Execute the wrapped worker function's core logic
            ingestion_result = await func(job_uuid, correlation_id, *args, **kwargs)
            # Ensure the wrapped function returns IngestionResult or raises Exception
            if not isinstance(ingestion_result, IngestionResult):
                logger.error(
                    f"Worker function {func.__name__} did not return IngestionResult",
                    extra=log_extra,
                )
                raise TypeError(
                    f"Worker function {func.__name__} must return IngestionResult"
                )

            # 3. Process successful result
            logger.info("Worker function completed successfully.", extra=log_extra)
            full_text = ingestion_result.full_text
            chunks = ingestion_result.chunks
            page_count = ingestion_result.page_count
            total_chunks = len(chunks)
            source_filename = (
                ingestion_result.source_filename
            )  # Get filename from result
            content_type = (
                ingestion_result.detected_content_type
            )  # Get content type from result
            source_url = ingestion_result.source_url
            fetched_at = ingestion_result.fetched_at

            log_extra["detected_content_type"] = content_type
            log_extra["page_count"] = page_count
            log_extra["chunk_count"] = total_chunks

            if not full_text and not chunks:
                logger.warning(
                    "Ingestion service returned no text or chunks", extra=log_extra
                )
                final_status = "failed"
                error_message = "Document processing yielded no text content."
            else:
                # 4. Save Results to DB (only if content was produced)
                logger.info("Saving results to database...", extra=log_extra)
                doc_id = await create_document(
                    job_id=job_uuid,
                    filename=source_filename,
                    content_type=content_type,
                    file_path=None,
                    full_text=full_text,
                    page_count=page_count,
                    source_url=source_url,
                    fetched_at=fetched_at,
                )
                log_extra["doc_id"] = str(doc_id)
                logger.info("Created document record", extra=log_extra)

                if chunks:
                    # Add doc_id to each chunk before batch insert
                    for chunk in chunks:
                        chunk["doc_id"] = doc_id
                    await create_chunks_batch(chunks)
                    logger.info("Saved chunks to database", extra=log_extra)
                else:
                    # This case might be unlikely if full_text exists, but good to log
                    logger.warning(
                        "No chunks were generated, although full text might exist.",
                        extra=log_extra,
                    )

                final_status = "completed"
                logger.info("Job completed successfully.", extra=log_extra)

        except Exception as e:
            logger.exception(
                f"Processing failed in {func.__name__}", exc_info=e, extra=log_extra
            )
            error_message = f"{type(e).__name__}: {str(e)[:500]}"
            failure_reason = type(e).__name__
            final_status = "failed"

        finally:
            # 5. Update Final Job Status & Metrics
            end_time = time.time()
            processing_duration = end_time - start_time
            log_extra["final_status"] = final_status
            log_extra["processing_seconds"] = round(processing_duration, 3)
            if error_message:
                log_extra["error_message"] = error_message

            logger.info(
                "Updating final job status and recording metrics.", extra=log_extra
            )
            try:
                await update_job_status(
                    job_id=job_uuid,
                    status=final_status,
                    completed_at=datetime.now(timezone.utc),
                    processing_seconds=processing_duration,
                    duration_seconds=processing_duration,
                    error_message=error_message,
                    doc_id=doc_id,
                )

                # Record metrics after successful status update
                JOB_PROCESSING_DURATION_SECONDS.labels(
                    worker=func.__name__, status=final_status
                ).observe(processing_duration)
                if final_status == "completed":
                    DOCS_PROCESSED_COUNTER.labels(worker=func.__name__).inc()
                    CHUNKS_CREATED_COUNTER.labels(worker=func.__name__).inc(
                        total_chunks
                    )
                elif final_status == "failed":
                    # Use the captured failure reason
                    JOBS_FAILED_COUNTER.labels(
                        worker=func.__name__, reason=failure_reason
                    ).inc()

                logger.debug(
                    "Job final status updated and metrics recorded.", extra=log_extra
                )

            except Exception as update_err:
                # Log critical failure if status update/metrics fail
                log_extra["update_error"] = str(update_err)
                logger.exception(
                    "CRITICAL: Failed to update final job status or metrics!",
                    exc_info=update_err,
                    extra=log_extra,
                )

            setup_task_logging(None)

    return wrapper


# --- Task Entry Points (Decorated) --- #


@job_processor
async def process_document_file(
    job_uuid: uuid.UUID,
    correlation_id: Optional[str],
    file_content: bytes,
    filename: Optional[str],
    content_type: Optional[str],
) -> IngestionResult:
    """Processes uploaded file content. Wrapped by job_processor."""
    # Core logic: call ingestion service
    # The decorator handles logging start/end, status updates, errors, metrics
    return await ingestion_service.process(
        content_source=file_content, filename=filename, content_type=content_type
    )


@job_processor
async def process_document_text(
    job_uuid: uuid.UUID,
    correlation_id: Optional[str],
    text_content: str,
    filename: Optional[str],
) -> IngestionResult:
    """Processes raw text payload. Wrapped by job_processor."""
    # Core logic: call ingestion service
    # The decorator handles logging start/end, status updates, errors, metrics
    return await ingestion_service.process(
        content_source=text_content,
        filename=filename,
        content_type="text/plain",
    )


# New worker function for URL
@job_processor
async def process_document_url(
    job_uuid: uuid.UUID,
    correlation_id: Optional[str],
    source_url: str,
) -> IngestionResult:
    """Processes content from a URL. Wrapped by job_processor."""
    # Core logic: call ingestion service with the URL
    # The decorator handles logging start/end, status updates, errors, metrics
    return await ingestion_service.process(
        content_source=source_url, filename=None, content_type=None
    )


async def startup():
    """Initialize resources needed by the worker before starting."""
    logger.info("Worker starting up...")
    await db_manager.initialize(settings)
    logger.info("Worker DB pool initialized.")


async def shutdown():
    """Clean up resources when the worker shuts down."""
    logger.info("Worker shutting down...")
    await db_manager.close()
    logger.info("Worker DB pool closed.")


if __name__ == "__main__":
    logger.info("Initializing Redis connection")
    redis_conn = Redis.from_url(settings.REDIS_URL)

    # Create Queue instances with the explicit connection
    queues = [
        Queue(
            name,
            connection=redis_conn,
            default_timeout=settings.REDIS_QUEUE_TIMEOUT_SECONDS,
            is_async=True,
        )
        for name in QUEUES_TO_LISTEN
    ]

    logger.info(
        "Creating standard Worker listening on queues",
        extra={"queues": QUEUES_TO_LISTEN, "is_async": True},
    )

    worker = Worker(queues, connection=redis_conn, log_job_description=False)

    logger.info("Running worker startup...")
    try:
        asyncio.run(startup())
    except Exception as start_err:
        logger.exception("Worker startup failed! Exiting.", exc_info=start_err)
        sys.exit(1)

    logger.info("Starting RQ Worker...")
    try:
        worker.work(with_scheduler=False)
    except Exception:
        logger.exception("RQ Worker failed unexpectedly.")
    finally:
        logger.info("Running worker shutdown...")
        try:
            asyncio.run(shutdown())
        except Exception as shut_err:
            logger.exception("Worker shutdown failed!", exc_info=shut_err)
        logger.info("RQ Worker stopped.")
