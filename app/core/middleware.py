import logging
import uuid
import time
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from typing import Set

from core.logging import correlation_id_cv

logger = logging.getLogger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Middleware to handle X-Correlation-Id header and logging context."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # 1. Extract or Generate Correlation ID
        header_value = request.headers.get("X-Correlation-Id")
        correlation_id: str

        if header_value:
            try:
                uuid.UUID(header_value)
                correlation_id = header_value
                logger.debug(f"Using existing correlation ID: {correlation_id}")
            except ValueError:
                logger.warning(
                    f"Invalid X-Correlation-Id header received: {header_value}. Generating new one."
                )
                correlation_id = str(uuid.uuid4())
        else:
            correlation_id = str(uuid.uuid4())
            logger.debug(f"Generated new correlation ID: {correlation_id}")

        # 2. Set Correlation ID in ContextVar for logging
        correlation_id_cv.set(correlation_id)

        # 3. Process Request and Measure Time
        start_time = time.perf_counter()
        response = await call_next(request)
        process_time = time.perf_counter() - start_time

        # 4. Add headers to Response
        response.headers["X-Correlation-Id"] = correlation_id
        response.headers["X-Process-Time"] = str(process_time)

        return response


class AuthMiddleware(BaseHTTPMiddleware):
    """Middleware to enforce Bearer token authentication."""

    def __init__(self, app: ASGIApp, required_token: str):
        super().__init__(app)
        self.required_token = required_token
        # Define public paths that bypass authentication
        self.public_paths: Set[str] = {
            "/health",
            "/metrics",
            "/v1/metrics",
            "/openapi.json",
            "/docs",
            "/redoc",
        }

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Allow unauthenticated access to defined public paths
        if request.url.path in self.public_paths:
            logger.debug(
                f"Allowing unauthenticated access to public path: {request.url.path}"
            )
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        error_response = Response(
            content='{"detail": "Unauthorized"}',
            status_code=401,
            media_type="application/json",
            headers={"WWW-Authenticate": "Bearer"},
        )

        if not auth_header:
            logger.warning("Missing Authorization header")
            return error_response

        try:
            scheme, token = auth_header.split()
            if scheme.lower() != "bearer":
                logger.warning(f"Unsupported authentication scheme: {scheme}")
                return error_response

            if token != self.required_token:
                logger.warning("Invalid authentication token provided")
                return error_response

        except ValueError:
            logger.warning(f"Malformed Authorization header: {auth_header}")
            return error_response

        # If token is valid, proceed with the request
        logger.debug("Authentication successful")
        return await call_next(request)
