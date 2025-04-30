import logging
import uuid
from fastapi import APIRouter, Depends, HTTPException
from psycopg_pool import AsyncConnectionPool

from api import models
from db.jobs import get_job_by_id, soft_delete_job
from api.deps import get_correlation_id, get_db_pool

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/jobs/{job_id}", response_model=models.JobStatusResponse)
async def get_job_status(
    job_id: uuid.UUID,
    correlation_id: str | None = Depends(get_correlation_id),
    pool: AsyncConnectionPool = Depends(get_db_pool)
):
    """
    **Step 2: Check Job Status**

    Retrieves the status and details of a specific background processing job initiated by an ingest request.

    - Poll this endpoint periodically after submitting data in Step 1.
    - Check the `status` field in the response.

    **Workflow:**
    1. Poll this endpoint using the `job_id` from Step 1.
    2. Continue polling if `status` is `"queued"` or `"processing"`.
    3. If `status` is `"completed"` or `"completed_with_errors"`, retrieve the `doc_id` from the response. This `doc_id` is needed for Step 3.
    4. If `status` is `"failed"`, check the `error_message` field.
    """
    logger.info(
        "Request for job status.",
        extra={"correlation_id": correlation_id, "job_id": job_id},
    )
    job = await get_job_by_id(job_id, pool=pool)
    if not job:
        logger.warning(
            "Job not found.", extra={"correlation_id": correlation_id, "job_id": job_id}
        )
        raise HTTPException(status_code=404, detail="Job not found")

    # Pydantic will automatically handle the mapping if field names match
    return models.JobStatusResponse(**job)


@router.delete("/jobs/{job_id}", status_code=200)
async def delete_job(
    job_id: uuid.UUID,
    correlation_id: str | None = Depends(get_correlation_id),
    pool: AsyncConnectionPool = Depends(get_db_pool)
):
    logger.info(
        "Request to delete job.",
        extra={"correlation_id": correlation_id, "job_id": job_id},
    )
    deleted = await soft_delete_job(job_id, pool=pool)
    if not deleted:
        logger.warning(
            "Job not found for deletion.",
            extra={"correlation_id": correlation_id, "job_id": job_id},
        )
        raise HTTPException(status_code=404, detail="Job not found or already deleted")

    logger.info(
        "Successfully marked job for deletion.",
        extra={"correlation_id": correlation_id, "job_id": job_id},
    )
    return {"message": "Job marked for deletion successfully"}
