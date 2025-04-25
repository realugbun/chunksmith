import asyncio  # Import asyncio at the top
import uuid
import logging
import sys
import time
from typing import Optional, Union
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


async def _process_document_common(
    job_id: uuid.UUID,
    correlation_id: Optional[str],
    source_description: str,
    content_source: Union[str, bytes],
    filename: Optional[str] = None,
    content_type: Optional[str] = None,
) -> None:
    """Common logic for processing a document using IngestionService."""
    setup_task_logging(correlation_id)
    logger.info(
        "Starting processing for job",
        extra={
            "job_id": job_id,
            "source_description": source_description,
            "content_type": content_type,
        },
    )

    # Log entry into the common function
    common_func_entry_time = time.time()
    logger.debug("Entered _process_document_common for job", extra={"job_id": job_id})

    start_time = time.time()
    doc_id = None
    final_status = "failed"
    error_message = None
    total_chunks = 0
    ingestion_result: Optional[IngestionResult] = None

    try:
        # 1. Update Job Status to Processing
        await update_job_status(
            job_id=job_id, status="processing", started_at=datetime.now(timezone.utc)
        )
        logger.debug(
            "Finished awaiting update_job_status (processing)",
            extra={
                "job_id": job_id,
                "time_since_entry": time.time() - common_func_entry_time,
            },
        )

        # 2. Perform Ingestion using the centralized service
        logger.info("Running ingestion service", extra={"job_id": job_id})
        ingestion_result = await ingestion_service.process(
            content_source=content_source, content_type=content_type, filename=filename
        )

        full_text = ingestion_result.full_text
        chunks = ingestion_result.chunks
        page_count = ingestion_result.page_count
        total_chunks = len(chunks)
        logger.info(
            "Ingestion service finished",
            extra={"job_id": job_id, "chunks": total_chunks, "pages": page_count},
        )

        # Check if ingestion produced text
        if not full_text and not chunks:
            logger.warning(
                "Ingestion service returned no text or chunks", extra={"job_id": job_id}
            )
            # Decide if this is completed_with_errors or failed
            final_status = "failed"
            error_message = "Document processing yielded no text content."
            # Skip DB saving if no content
            raise ValueError(error_message)  # Go directly to finally block

        # 4. Save Results to DB
        logger.info("Saving results", extra={"job_id": job_id})
        doc_id = await create_document(
            job_id=job_id,
            filename=filename,
            content_type=content_type,
            file_path=None,
            full_text=full_text,
            page_count=page_count,
        )
        logger.info(
            "Created document record", extra={"job_id": job_id, "doc_id": doc_id}
        )

        if chunks:
            # Add doc_id to each chunk before batch insert
            for chunk in chunks:
                chunk["doc_id"] = doc_id
            await create_chunks_batch(chunks)
            logger.info(
                "Saved chunks",
                extra={
                    "job_id": job_id,
                    "total_chunks": total_chunks,
                    "doc_id": doc_id,
                },
            )
        else:
            logger.error(
                "No chunks were generated", extra={"job_id": job_id, "doc_id": doc_id}
            )

        # 5. Determine Final Status
        # Skipped pages logic removed as Docling handles internal errors
        final_status = "completed"
        logger.info("Job completed successfully", extra={"job_id": job_id})

    except Exception as e:
        logger.exception("Processing failed", exc_info=e, extra={"job_id": job_id})
        error_message = (
            f"Processing failed: {str(e)[:500]}"  # Limit error message length
        )
        final_status = "failed"

    finally:
        # 6. Update Job Status with final result
        end_time = time.time()
        processing_duration = end_time - start_time

        logger.info(
            "Updating final status",
            extra={"job_id": job_id, "final_status": final_status},
        )

        try:
            # Update DB first
            await update_job_status(
                job_id=job_id,
                status=final_status,
                completed_at=datetime.now(timezone.utc),
                processing_seconds=processing_duration,
                duration_seconds=processing_duration,
                error_message=error_message,
                doc_id=doc_id,
            )

            # Increment counters only after successful DB update
            if final_status == "completed":
                DOCS_PROCESSED_COUNTER.inc()
                CHUNKS_CREATED_COUNTER.inc(total_chunks)
            elif final_status == "failed":
                JOBS_FAILED_COUNTER.inc()

            logger.debug(
                "Job final status updated and metrics recorded",
                extra={"job_id": job_id},
            )

        except Exception as update_err:
            logger.exception(
                "Failed to update final status or metrics",
                exc_info=update_err,
                extra={"job_id": job_id},
            )

        correlation_id_cv.set(None)  # Clear context var at the end of the task


# --- Task Entry Points --- #


async def process_document_file(
    job_uuid_arg: uuid.UUID,
    correlation_id_arg: Optional[str],
    file_content_arg: bytes,
    filename_arg: Optional[str],
    content_type_arg: Optional[str],
):
    logger.debug(
        "process_document_file: About to await _process_document_common for job",
        extra={"job_id": job_uuid_arg},
    )  # Log before await
    await _process_document_common(
        job_id=job_uuid_arg,
        correlation_id=correlation_id_arg,
        source_description=f"file content: {filename_arg}",
        content_source=file_content_arg,
        filename=filename_arg,
        content_type=content_type_arg,
    )


async def process_document_text(
    job_uuid_arg: uuid.UUID,
    correlation_id_arg: Optional[str],
    text_content_arg: str,
    filename_arg: Optional[str],
):
    """RQ task entry point for processing text payload."""

    logger.debug(
        "process_document_text: About to await _process_document_common for job",
        extra={"job_id": job_uuid_arg},
    )  # Log before await
    await _process_document_common(
        job_id=job_uuid_arg,
        correlation_id=correlation_id_arg,
        source_description="text payload",
        content_source=text_content_arg,
        filename=filename_arg,
        content_type="text/plain",
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
        )
        for name in QUEUES_TO_LISTEN
    ]

    logger.info(
        "Creating standard Worker listening on queues",
        extra={"queues": QUEUES_TO_LISTEN},
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
