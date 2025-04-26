from prometheus_client import Counter, CollectorRegistry, Histogram

# Create a custom registry to avoid conflicts with default registry if other libs use it
REGISTRY = CollectorRegistry(auto_describe=True)

# Define custom metrics
DOCS_PROCESSED_COUNTER = Counter(
    "document_service_docs_processed_total",
    "Total number of documents successfully processed",
    registry=REGISTRY,
)

CHUNKS_CREATED_COUNTER = Counter(
    "document_service_chunks_created_total",
    "Total number of chunks created from documents",
    registry=REGISTRY,
)

JOBS_FAILED_COUNTER = Counter(
    "document_service_jobs_failed_total",
    "Total number of ingestion jobs that failed",
    ["worker", "reason"],
    registry=REGISTRY,
)

PROCESSING_TIME_BUCKETS = (
    0.1,
    0.5,
    1.0,
    2.5,
    5.0,
    10.0,
    30.0,
    60.0,
    120.0,
    300.0,
    600.0,
    float("inf"),
)

JOB_PROCESSING_DURATION_SECONDS = Histogram(
    "chunksmith_job_processing_duration_seconds",
    "Histogram of job processing durations in seconds",
    ["worker", "status"],  # Labels for worker function name and final status
    buckets=PROCESSING_TIME_BUCKETS,
    registry=REGISTRY,
)

# === Webhook Metrics ===
WEBHOOK_DELIVERIES_TOTAL = Counter(
    "chunksmith_webhook_deliveries_total",
    "Total number of webhook delivery attempts",
    ["status"],
    registry=REGISTRY,
)

WEBHOOK_LATENCY_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, float("inf"))

WEBHOOK_DELIVERY_LATENCY = Histogram(
    "chunksmith_webhook_delivery_latency_seconds",
    "Latency of webhook delivery attempts in seconds",
    buckets=WEBHOOK_LATENCY_BUCKETS,
    registry=REGISTRY,
)

# Example of an Info metric for static info (like service version)
# service_info = Info(
#     'document_service_info',
#     'Information about the document service',
#     registry=REGISTRY
# )
# service_info.info({'version': settings.APP_VERSION}) # Assuming APP_VERSION in settings

# Note: document_service_jobs_processing_current would typically be a Gauge metric,
# potentially managed within the worker or via tracking active job IDs.
# We'll omit it for now as it requires more complex state management.
