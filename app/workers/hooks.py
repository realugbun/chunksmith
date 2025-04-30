import logging
import json
import hmac
import hashlib
import uuid
import httpx
import asyncio
import time
from typing import Any
from datetime import datetime, timezone

from rq.job import Job
from redis import Redis as SyncRedis

from core.config import settings
from core.config import Settings # Import Settings for pool config
from core.redis_client import get_redis_connection
from psycopg_pool import AsyncConnectionPool # Import pool class
from db.jobs import get_job_by_id
from api.models import JobStatusResponse
from core.logging import correlation_id_cv
from core.metrics import WEBHOOK_DELIVERIES_TOTAL, WEBHOOK_DELIVERY_LATENCY

logger = logging.getLogger(__name__)


def run_dispatch_webhook_sync(
    job: Job,
    connection: SyncRedis,
    result_or_type: Any,
    value: Any | None = None,
    traceback: Any | None = None,
) -> None:
    """Synchronous wrapper to run the async webhook dispatch hook."""
    try:
        asyncio.run(dispatch_webhook(job, connection, result_or_type, value, traceback))
    except Exception:
        job_id = job.id if job else "unknown"
        logger.exception(f"Error executing async webhook dispatch for job {job_id}")


async def dispatch_webhook(
    job: Job,
    connection: SyncRedis,
    result_or_type: Any,
    value: Any | None = None,
    traceback: Any | None = None,
    **kwargs: Any,
) -> None:
    """RQ Job Hook: Dispatches webhook notification after job completion/failure."""

    job_id_str = job.id
    final_status = job.get_status()

    correlation_id = job.meta.get("correlation_id")
    token = None
    log_extra_base = {"job_id": job_id_str, "correlation_id": correlation_id}

    if correlation_id:
        token = correlation_id_cv.set(correlation_id)

    log_extra = {**log_extra_base, "job_status": final_status}
    logger.info(
        f"Webhook hook triggered for job {job_id_str} with status {final_status}",
        extra=log_extra,
    )

    redis_conn = None
    callback_key = f"callback:{job_id_str}"

    try:
        # 1. Fetch callback details from Redis using our async client
        redis_conn = await get_redis_connection()
        callback_data_bytes = await redis_conn.get(callback_key)

        if not callback_data_bytes:
            logger.debug(
                f"No callback details found in Redis for job {job_id_str}. Skipping webhook.",
                extra=log_extra,
            )
            return

        try:
            # Decode bytes from redis before JSON parsing
            callback_data = json.loads(callback_data_bytes.decode("utf-8"))
            url = callback_data["url"]
            secret = callback_data["secret"]
            logger.debug(f"Retrieved callback details for URL: {url}", extra=log_extra)
        except (json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError) as e:
            logger.error(
                f"Failed to parse callback details from Redis: {e}", extra=log_extra
            )
            return

        # 2. Fetch final job status from DB
        job_details_dict = None
        # <<< FIX A: Create dedicated pool for webhook DB access >>>
        try:
            conninfo_base = settings.POSTGRES_URL
            separator = "&" if "?" in conninfo_base else "?"
            conninfo_enhanced = f"{conninfo_base}{separator}keepalives_idle=60&keepalives_interval=10&keepalives_count=5"
            async with AsyncConnectionPool(
                conninfo=conninfo_enhanced,
                min_size=1, max_size=2, timeout=10.0, name=f"webhook-pool-{job_id_str[:8]}"
            ) as webhook_pool:
                 job_details_dict = await get_job_by_id(uuid.UUID(job_id_str), pool=webhook_pool)

        except Exception as db_err:
            logger.exception("Webhook hook failed to query DB for job details", exc_info=db_err, extra=log_extra)
            # Can't proceed without job details
            return
        # <<< END FIX >>>

        if not job_details_dict:
            logger.error(
                f"Job {job_id_str} not found in DB for webhook payload.",
                extra=log_extra,
            )
            return

        # 3. Prepare payload (using the API model for structure)
        try:
            job_status_response = JobStatusResponse(**job_details_dict)
            payload_json = job_status_response.model_dump_json(exclude_none=True)
        except Exception as e:
            logger.exception(
                "Failed to create JobStatusResponse payload",
                exc_info=e,
                extra=log_extra,
            )
            return

        # 4. Compute HMAC signature
        try:
            timestamp = datetime.now(timezone.utc).isoformat()

            # Construct the message to sign: timestamp + "." + body
            message_to_sign = f"{timestamp}.{payload_json}".encode("utf-8")

            signature = hmac.new(
                secret.encode("utf-8"), message_to_sign, hashlib.sha256
            ).hexdigest()
            headers = {
                "Content-Type": "application/json",
                "X-ChunkSmith-Signature": f"sha256={signature}",
                "X-ChunkSmith-Timestamp": timestamp,
            }
            logger.debug(
                f"Generated signature using timestamp: {timestamp}", extra=log_extra
            )
        except Exception as e:
            logger.exception(
                "Failed to compute HMAC signature", exc_info=e, extra=log_extra
            )
            return

        # 5. Make POST request
        logger.info(f"Dispatching webhook to {url}", extra=log_extra)
        response_status = -1
        try:
            start_time = time.perf_counter()
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    headers=headers,
                    content=payload_json,
                    timeout=settings.WEBHOOK_TIMEOUT_SECONDS,
                )
            response_status = response.status_code
            duration = time.perf_counter() - start_time
            WEBHOOK_DELIVERY_LATENCY.observe(duration)

            log_extra_resp = {**log_extra, "http_status": response_status}
            if 200 <= response_status < 300:
                logger.info(
                    f"Webhook delivered successfully to {url}", extra=log_extra_resp
                )
                WEBHOOK_DELIVERIES_TOTAL.labels(status="success").inc()
            else:
                logger.warning(
                    f"Webhook delivery to {url} failed with status {response_status}. Response: {response.text[:200]}",  # Log part of response body
                    extra=log_extra_resp,
                )
                WEBHOOK_DELIVERIES_TOTAL.labels(status="failure").inc()

        except httpx.TimeoutException:
            logger.warning(
                f"Webhook delivery to {url} timed out after {settings.WEBHOOK_TIMEOUT_SECONDS}s",
                extra=log_extra,
            )
            WEBHOOK_DELIVERIES_TOTAL.labels(status="failure").inc()
            WEBHOOK_DELIVERY_LATENCY.observe(settings.WEBHOOK_TIMEOUT_SECONDS)
        except httpx.RequestError as e:
            logger.error(
                f"Webhook delivery to {url} failed due to network/request error: {e}",
                extra=log_extra,
            )
            WEBHOOK_DELIVERIES_TOTAL.labels(status="failure").inc()
        except Exception as e:
            logger.exception(
                f"Unexpected error during webhook dispatch to {url}",
                exc_info=e,
                extra=log_extra,
            )
            WEBHOOK_DELIVERIES_TOTAL.labels(status="failure").inc()

    except Exception as e:
        logger.exception(
            "Failed during webhook hook setup (Redis fetch, DB query)",
            exc_info=e,
            extra=log_extra_base,
        )

    finally:
        # 6. ALWAYS attempt to delete the key from Redis
        if redis_conn:
            try:
                deleted_count = await redis_conn.delete(callback_key)
                if deleted_count > 0:
                    logger.debug(
                        f"Deleted callback details from Redis key {callback_key}",
                        extra=log_extra_base,
                    )
                else:
                    # This isn't necessarily an error - TTL might have expired
                    logger.debug(
                        f"Callback key {callback_key} not found for deletion (likely expired or already deleted).",
                        extra=log_extra_base,
                    )
            except Exception as del_err:
                # Log error but don't raise, as the main hook work might be done
                logger.error(
                    f"Failed to delete callback key {callback_key} from Redis: {del_err}",
                    extra=log_extra_base,
                )

        if token:
            correlation_id_cv.reset(token)
