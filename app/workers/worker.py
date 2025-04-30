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
from psycopg_pool import AsyncConnectionPool

## NOTE: The service init and worker tasks need to be in the same file to prevent
##       issues with the RQ worker importing dependencies on each task.

setup_logging()

logger = logging.getLogger(__name__)

# Define the queues this worker will listen to
QUEUES_TO_LISTEN = ["chunksmith"]


def setup_task_logging(correlation_id: Optional[str]):
    """Sets up correlation ID for logging within the task."""
    correlation_id_cv.set(correlation_id)


# --- Job Execution Helper --- #
async def _run_job_with_pool(
    job_uuid: uuid.UUID,
    correlation_id: Optional[str],
    worker_func_name: str, # Name of the original worker function for logging/metrics
    source_repr: str,      # Pre-determined source representation for logging
    core_task_awaitable: asyncio.Future, # The actual awaitable task (e.g., ingestion_service.process)
) -> None:
    """Handles per-job pool creation, status updates, execution, and metrics."""

    setup_task_logging(correlation_id)
    job_id_str = str(job_uuid)
    start_time = time.time()
    final_status: str = "failed"
    error_message: Optional[str] = None
    doc_id: Optional[uuid.UUID] = None
    ingestion_result: Optional[IngestionResult] = None
    total_chunks: int = 0
    failure_reason = "unknown"

    log_extra = {
        "job_id": job_id_str,
        "correlation_id": correlation_id,
        "source": source_repr,
        "worker_function": worker_func_name,
    }

    logger.info(f"Starting job execution via helper ({worker_func_name}).", extra=log_extra)

    # Create the main pool for this job
    conninfo_base = settings.POSTGRES_URL
    separator = "&" if "?" in conninfo_base else "?"
    conninfo_enhanced = f"{conninfo_base}{separator}keepalives_idle=60&keepalives_interval=10&keepalives_count=5"
    logger.debug("Creating main per-job connection pool.", extra=log_extra)

    local_pool = None # Define outside try/except for final block access
    try:
        async with AsyncConnectionPool(
            conninfo=conninfo_enhanced,
            min_size=1, max_size=5, open=True,
            max_idle=60, max_lifetime=300,
            timeout=60.0,
            check=None,
            name=f"job-{job_id_str[:8]}"
        ) as local_pool:
            logger.debug("Main per-job connection pool created.", extra=log_extra)

            # Initial connection check
            try:
                async with local_pool.connection() as conn:
                    await conn.execute("SELECT 1")
                logger.debug("Main pool initial connection successful.", extra=log_extra)
            except Exception as pool_init_err:
                logger.exception("Failed initial connection for main pool!", exc_info=pool_init_err, extra=log_extra)
                raise pool_init_err # Fail job early

            # 1. Update Status to Processing
            await update_job_status(
                pool=local_pool,
                job_id=job_uuid,
                status="processing",
                started_at=datetime.now(timezone.utc),
            )
            logger.debug("Set job status to processing", extra=log_extra)

            # 2. Execute the core task logic
            ingestion_result = await core_task_awaitable # Await the passed future/coroutine
            if not isinstance(ingestion_result, IngestionResult):
                logger.error("Core task did not return IngestionResult", extra=log_extra)
                raise TypeError("Core task must return IngestionResult")

            # 3. Process successful result
            full_text = ingestion_result.full_text
            chunks = ingestion_result.chunks
            page_count = ingestion_result.page_count
            total_chunks = len(chunks)
            source_filename = ingestion_result.source_filename
            content_type = ingestion_result.detected_content_type
            source_url = ingestion_result.source_url
            fetched_at = ingestion_result.fetched_at

            log_extra["detected_content_type"] = content_type
            log_extra["page_count"] = page_count
            log_extra["chunk_count"] = total_chunks

            if not full_text and not chunks:
                logger.warning("Ingestion service returned no text or chunks", extra=log_extra)
                final_status = "failed"
                error_message = "Document processing yielded no text content."
            else:
                # 4. Save Results to DB using local_pool
                logger.info("Saving results to database...", extra=log_extra)
                doc_id = await create_document(
                    pool=local_pool,
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
                    for chunk in chunks:
                        chunk["doc_id"] = doc_id
                    await create_chunks_batch(chunks, pool=local_pool)
                    logger.info("Saved chunks to database", extra=log_extra)
                else:
                    logger.warning("No chunks were generated...", extra=log_extra)

                final_status = "completed"
                logger.info("Job processing completed successfully.", extra=log_extra)

    except Exception as e:
        logger.exception(f"Processing failed in {worker_func_name}", exc_info=e, extra=log_extra)
        error_message = f"{type(e).__name__}: {str(e)[:500]}"
        failure_reason = type(e).__name__
        final_status = "failed"

    finally:
        # 5. Update Final Job Status & Metrics (using a separate, short-lived pool)
        end_time = time.time()
        processing_duration = end_time - start_time
        log_extra["final_status"] = final_status
        log_extra["processing_seconds"] = round(processing_duration, 3)
        if error_message:
            log_extra["error_message"] = error_message

        logger.info("Updating final job status and recording metrics.", extra=log_extra)
        final_update_pool = None
        try:
            async with AsyncConnectionPool(
                conninfo=conninfo_enhanced, # Reuse conn string logic
                min_size=1, max_size=2, timeout=10.0, name=f"final-update-{job_id_str[:8]}"
            ) as final_update_pool:
                await update_job_status(
                    pool=final_update_pool,
                    job_id=job_uuid,
                    status=final_status,
                    completed_at=datetime.now(timezone.utc),
                    processing_seconds=processing_duration,
                    duration_seconds=processing_duration,
                    error_message=error_message,
                    doc_id=doc_id,
                )

            # Metrics recording (outside pool context)
            JOB_PROCESSING_DURATION_SECONDS.labels(worker=worker_func_name, status=final_status).observe(processing_duration)
            if final_status == "completed":
                DOCS_PROCESSED_COUNTER.labels(worker=worker_func_name).inc()
                CHUNKS_CREATED_COUNTER.labels(worker=worker_func_name).inc(total_chunks)
            elif final_status == "failed":
                JOBS_FAILED_COUNTER.labels(worker=worker_func_name, reason=failure_reason).inc()

            logger.debug("Job final status updated and metrics recorded.", extra=log_extra)

        except Exception as update_err:
            log_extra["update_error"] = str(update_err)
            logger.exception("CRITICAL: Failed to update final job status or metrics!", exc_info=update_err, extra=log_extra)

        setup_task_logging(None)


# --- Task Entry Points --- #


# @job_processor # Removed decorator
async def process_document_file(
    job_uuid: uuid.UUID,
    correlation_id: Optional[str],
    file_content: bytes,
    filename: Optional[str],
    content_type: Optional[str],
) -> None: # Helper function handles return/exceptions
    """Processes uploaded file content by calling the job helper."""
    source_repr = f"file: {filename or 'unknown_file'}"
    await _run_job_with_pool(
        job_uuid=job_uuid,
        correlation_id=correlation_id,
        worker_func_name="process_document_file",
        source_repr=source_repr,
        core_task_awaitable=ingestion_service.process(
            content_source=file_content, filename=filename, content_type=content_type
        )
    )


# @job_processor # Removed decorator
async def process_document_text(
    job_uuid: uuid.UUID,
    correlation_id: Optional[str],
    text_content: str,
    filename: Optional[str],
) -> None: # Helper function handles return/exceptions
    """Processes raw text payload by calling the job helper."""
    source_repr = f"text_payload ({filename or 'no filename'})"
    await _run_job_with_pool(
        job_uuid=job_uuid,
        correlation_id=correlation_id,
        worker_func_name="process_document_text",
        source_repr=source_repr,
        core_task_awaitable=ingestion_service.process(
            content_source=text_content,
            filename=filename,
            content_type="text/plain",
        )
    )


# New worker function for URL
# @job_processor # Removed decorator
async def process_document_url(
    job_uuid: uuid.UUID,
    correlation_id: Optional[str],
    source_url: str,
) -> None: # Helper function handles return/exceptions
    """Processes content from a URL by calling the job helper."""
    source_repr = f"url: {source_url}"
    await _run_job_with_pool(
        job_uuid=job_uuid,
        correlation_id=correlation_id,
        worker_func_name="process_document_url",
        source_repr=source_repr,
        core_task_awaitable=ingestion_service.process(
            content_source=source_url, filename=None, content_type=None
        )
    )


async def startup():
    """Initialize resources needed by the worker before starting."""
    logger.info("Worker starting up...")
    # No need to initialize global db_manager pool if worker isn't using it.
    # await db_manager.initialize(settings)
    logger.info("Worker DB pool initialized.")


async def shutdown():
    """Clean up resources when the worker shuts down."""
    logger.info("Worker shutting down...")
    # No need to close global db_manager pool if worker isn't using it.
    # await db_manager.close()
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
