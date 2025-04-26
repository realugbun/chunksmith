# ChunkSmith

**ChunkSmith** is a robust backend service designed for efficiently ingesting various document types (including direct URL fetching, PDFs, DOCX, and images with OCR), processing their content, and splitting them into meaningful, context-aware chunks. It's built for asynchronous processing and scalability, making it suitable for integration into larger data pipelines or applications requiring document understanding.

[![Language](https://img.shields.io/badge/Language-Python%203.12-blue)](https://www.python.org/)
[![Framework](https://img.shields.io/badge/Framework-FastAPI-green)](https://fastapi.tiangolo.com/)
[![Database](https://img.shields.io/badge/Database-PostgreSQL-blue)](https://www.postgresql.org/)
[![Job Queue](https://img.shields.io/badge/Job%20Queue-Redis%20%26%20RQ-red)](https://python-rq.org/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)



## Table of Contents

- [Key Features](#key-features)
- [Technology Stack](#technology-stack)
- [Architecture Overview](#architecture-overview)
- [Quickstart](#quickstart)
- [Setup and Running](#setup-and-running)
- [API Usage](#api-usage)
  - [1a. Ingest Text](#1a-ingest-text)
  - [1b. Ingest File](#1b-ingest-file)
  - [1c. Ingest URL](#1c-ingest-url)
  - [2. Get Job Status](#2-get-job-status)
  - [3a. Get Document Full Text](#3a-get-document-full-text)
  - [3b. Get Document Chunks](#3b-get-document-chunks)
  - [Webhooks (Optional Callbacks)](#webhooks-optional-callbacks)
- [Scaling Workers](#scaling-workers)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Key Features

*   **Multi-Format Ingestion:** Accepts raw text, PDF, DOCX, XLSX, PPTX, Markdown, AsciiDoc, HTML, CSV, and common image formats (PNG, JPEG, TIFF, BMP).
*   **URL Ingestion:** Ingests documents directly from HTTP/HTTPS URLs.
*   **Optical Character Recognition (OCR):** Built-in OCR for extracting text from PDFs and images using `docling`.
*   **Asynchronous Processing:** Leverages a robust job queue (Redis & RQ) for handling time-consuming ingestion and processing tasks without blocking API requests.
*   **Scalable Workers:** Easily scale the number of processing workers to handle varying loads (see [Scaling Workers](#scaling-workers)).
*   **Intelligent Chunking:** Breaks documents into chunks using a configurable max byte length and overlap, respecting Markdown formatting boundaries (`langchain`).
*   **RESTful API:** Clean API for submitting documents, checking job status, and retrieving processed text or chunks.
*   **Webhook Support:** Optional webhook callbacks for real-time notification of job completion or failure, eliminating the need for polling.
*   **Authentication:** Secure API endpoints using Bearer token authentication.
*   **Database Storage:** Uses PostgreSQL to store job metadata, document information, and extracted chunks.
*   **Observability:**
    *   **Structured Logging:** Outputs logs in JSON format, ready for ingestion into logging systems.
    *   **Correlation IDs:** Uses `X-Correlation-ID` headers (generated if not provided) to trace requests across API and worker services within the logs.
    *   **Metrics:** Exposes Prometheus-compatible metrics for monitoring (`/metrics`).
*   **Containerized:** Fully containerized using Docker and Docker Compose for easy deployment and dependency management.

## Technology Stack

*   **Backend Framework:** FastAPI
*   **Programming Language:** Python 3.12+
*   **Document Processing:** `docling`, `langchain`
*   **Database:** PostgreSQL
*   **Database Migrations:** `yoyo-migrations`
*   **Job Queue:** Redis, RQ (Redis Queue)
*   **API Specification:** OpenAPI 3.1.0
*   **Containerization:** Docker, Docker Compose
*   **Web Server:** Uvicorn
*   **Dependency Management:** `uv` (via `pyproject.toml`)
*   **Testing:** Pytest
*   **Logging:** `python-json-logger`
*   **Monitoring:** `prometheus-client`, `prometheus-fastapi-instrumentator`

## Architecture Overview

ChunkSmith runs as a set of containerized services managed by Docker Compose:

1.  **`chunksmith-api`:** The main FastAPI application handling incoming API requests, authentication, request validation, job queuing, and serving results.
2.  **`chunksmith-worker`:** One or more worker processes that pull jobs from the Redis queue, perform document parsing, OCR (if needed), chunking using `docling`/`langchain`, and database updates.
3.  **`chunksmith-postgres`:** The PostgreSQL database storing application data (jobs, documents, chunks).
4.  **`chunksmith-redis`:** The Redis instance acting as the message broker for the RQ job queue.


## Quickstart

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/realugbun/chunksmith.git
    cd chunksmith
    ```
2.  **Configure Environment:**
    ```bash
    cp .env.example .env
    # Edit .env and set your AUTH_TOKEN (and other variables if needed)
    # For example, using nano: nano .env
    ```
3.  **Build and Run:**
    ```bash
    docker compose up --build -d
    ```
    The services will start, and database migrations will be applied automatically.
4.  **Ingest Sample Text:**
    *(Make sure your AUTH_TOKEN is set in your shell environment or replace `$AUTH_TOKEN`)*
    ```bash
    # Set the token in your environment (example for bash/zsh)
    # export AUTH_TOKEN=$(grep AUTH_TOKEN .env | cut -d '=' -f2)

    curl -X POST http://localhost:8000/v1/ingest/text \
      -H "Authorization: Bearer $AUTH_TOKEN" \
      -H "Content-Type: application/json" \
      -H "X-Correlation-ID: quickstart-$(uuidgen)" \
      -d '{"text":"Hello world from ChunkSmith! This is the first document.","filename":"hello.txt"}'
    ```
5.  **Check API Docs:** Explore more endpoints at `http://localhost:8000/docs`.

## Setup and Running

1.  **Prerequisites:** Ensure Docker and Docker Compose are installed.
2.  **Configuration (`.env` file):**
    *   Copy `.env.example` to `.env`.
    *   Edit `.env` and provide necessary values. Key variables include:
        *   `AUTH_TOKEN`: **Required.** A secret bearer token clients must use for API authentication. Generate a strong, unique value.
        *   `POSTGRES_USER`: **Required.** Username for the PostgreSQL database. Defaults match `docker-compose.yml`.
        *   `POSTGRES_PASSWORD`: **Required.** Password for the PostgreSQL database user. Defaults match `docker-compose.yml`.
        *   `POSTGRES_DB`: **Required.** Name of the PostgreSQL database. Defaults match `docker-compose.yml`.
        *   `POSTGRES_HOST`: **Required.** Hostname or IP address of the PostgreSQL server. Defaults match `docker-compose.yml`.
        *   `POSTGRES_PORT`: **Required.** Port number for the PostgreSQL server (default: 5432). Defaults match `docker-compose.yml`. (The full `POSTGRES_URL` is constructed from these components).
        *   `REDIS_URL`: **Required.** Connection string for the Redis instance used by the job queue. Defaults match `docker-compose.yml`.
        *   `DATA_ROOT`: Path *inside the containers* where temporary files might be stored during processing. Defaults to `/tmp/chunksmith-data`.
        *   `MAX_FILE_SIZE_MB`: Maximum allowed size for uploaded files (default: 100 MB).
        *   `MAX_PAGE_COUNT`: Maximum number of pages to process in a PDF (default: 500). Helps prevent excessive processing time for very large PDFs.
        *   `CHUNK_DEFAULT_LIMIT`: Default number of chunks returned by the `GET /chunks` endpoint if no `limit` is specified (default: 50).
        *   `CHUNK_SIZE_BYTES`: Target size for text chunks in bytes (default: 2048). The actual size may vary slightly.
        *   `CHUNK_OVERLAP_BYTES`: Number of bytes to overlap between consecutive chunks (default: 205). Helps maintain context.
        *   `LOG_LEVEL`: Logging level (e.g., `DEBUG`, `INFO`, `WARNING`, `ERROR`) (default: `DEBUG`).
        *   `SERVICE_NAME`: Name used in structured logs (default: `chunksmith`).
        *   `REDIS_QUEUE_TIMEOUT_SECONDS`: Maximum time a worker job is allowed to run before being considered failed (default: 600 seconds / 10 minutes).
        *   `WEBHOOK_TIMEOUT_SECONDS`: Timeout for sending webhook POST requests to client URLs (default: 5 seconds).
3.  **Build & Run:**
    ```bash
    docker compose up --build -d
    ```
    This command builds the Docker images (if they don't exist or have changed) and starts all services defined in `docker-compose.yml` in detached mode. Database migrations are automatically applied on startup by the API service.
4.  **Access:**
    *   API Base URL: `http://localhost:8000` (default)
    *   API Docs (Swagger UI): `http://localhost:8000/docs`
    *   Alternate API Docs (ReDoc): `http://localhost:8000/redoc`
    *   Health Check: `http://localhost:8000/health`
    *   Metrics: `http://localhost:8000/metrics`

## API Usage

Interact with the ChunkSmith API using HTTP requests. Most endpoints require an `Authorization: Bearer YOUR_TOKEN` header. Provide an optional `X-Correlation-Id` header (UUID recommended) to trace requests through the system logs; if omitted, one will be generated.

**Core Workflow:**

1.  **Ingest Data:** Submit text or a file for processing.
2.  **Check Job Status:** Poll the job status endpoint using the returned `job_id` (unless using webhooks).
3.  **Retrieve Results:** Once the job is complete, use the `doc_id` from the job status to fetch the full text or chunks.

---

### 1a. Ingest Text

*   **Endpoint:** `POST /v1/ingest/text`
*   **Description:** Submits raw text content for processing.
*   **Auth:** Bearer Token required.
*   **Request Body:** `application/json`

**Example Request:**

```http
POST /v1/ingest/text HTTP/1.1
Host: localhost:8000
Authorization: Bearer your_secret_token_here
Content-Type: application/json
X-Correlation-ID: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee

{
  "text": "This is the content of the document.\nIt has multiple lines.",
  "filename": "example.txt",
  "callback_url": "https://yourapp.com/webhook-handler",
  "callback_secret": "your-super-secret-string-here"
}
```

**Example Response (202 Accepted):**

```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "queued",
  "submitted_at": "2023-10-27T10:00:00.123456+00:00"
}
```

---

### 1b. Ingest File

*   **Endpoint:** `POST /v1/ingest/file`
*   **Description:** Uploads a document file for processing. Supports various formats (PDF, DOCX, images, etc.). OCR is applied automatically to PDFs and images.
*   **Auth:** Bearer Token required.
*   **Request Body:** `multipart/form-data`

**Example Request (`curl`):**

```bash
curl -X POST http://localhost:8000/v1/ingest/file \
  -H "Authorization: Bearer your_secret_token_here" \
  -H "X-Correlation-ID: aaaaaaaa-bbbb-cccc-dddd-ffffffffffff" \
  -F "file=@/path/to/your/document.pdf" \
  -F "callback_url=https://yourapp.com/webhook-handler" \
  -F "callback_secret=your-super-secret-string-here"
```

**Example Response (202 Accepted):**

```json
{
  "job_id": "abcdef01-2345-6789-abcd-ef0123456789",
  "status": "queued",
  "submitted_at": "2023-10-27T10:05:00.987654+00:00"
}
```

---

### 1c. Ingest URL

*   **Endpoint:** `POST /v1/ingest/url`
*   **Description:** Submits a URL for the service to fetch and process the content. Supports similar document types as file upload, handled by `docling`.
*   **Auth:** Bearer Token required.
*   **Request Body:** `application/json`

**Parameters:**
*   `url` (string, required): The HTTP/HTTPS URL of the document to fetch and process.
*   `callback_url` (string, optional): An HTTP/HTTPS URL to POST job status updates to.
*   `callback_secret` (string, optional): A secret string used to sign the webhook payload (required if `callback_url` is provided, minimum 32 bytes).

**Example Request:**

```http
POST /v1/ingest/url HTTP/1.1
Host: localhost:8000
Authorization: Bearer your_secret_token_here
Content-Type: application/json
X-Correlation-ID: dddddddd-eeee-ffff-gggg-hhhhhhhhhhhh

{
  "url": "https://gist.githubusercontent.com/realugbun/4a6c805b8617e66014c961f7134d933c/raw/e06c919b731b77868781abf674e814c07c17815f/sample_doc_for_chunksmith_test.txt",
  "callback_url": "https://yourapp.com/webhook-handler",
  "callback_secret": "your-secure-webhook-secret-minimum-32-bytes"
}
```

**Example Response (202 Accepted):**

```json
{
  "job_id": "456a7890-e12c-34d5-e678-901234567890",
  "status": "queued",
  "submitted_at": "2023-10-27T11:00:00.987654+00:00"
}
```

---

### 2. Get Job Status

*   **Endpoint:** `GET /v1/jobs/:job_id`
*   **Description:** Retrieves the current status and details of a specific processing 
job. Poll this endpoint if not using webhooks.
*   **Auth:** Bearer Token required.
*   **Path Parameter:** `job_id` (UUID)

**Example Request:**

```http
GET /v1/jobs/123e4567-e89b-12d3-a456-426614174000 HTTP/1.1
Host: localhost:8000
Authorization: Bearer your_secret_token_here
X-Correlation-ID: aaaaaaaa-bbbb-cccc-dddd-gggggggggggg
```

**Example Response (Job Completed):**

```json
{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "completed",
  "queued_at": "2023-10-27T10:00:00.123456+00:00",
  "started_at": "2023-10-27T10:00:05.500000+00:00",
  "completed_at": "2023-10-27T10:00:15.750000+00:00",
  "queue_seconds": 5.376544,
  "processing_seconds": 10.25,
  "duration_seconds": 15.626544,
  "error_message": null,
  "doc_id": "987e6543-e21b-12d3-a456-426655440000"
}
```

**Example Response (Job Failed):**

```json
{
  "job_id": "fedcba98-7654-3210-fedc-ba9876543210",
  "status": "failed",
  "queued_at": "2023-10-27T11:00:00.000000+00:00",
  "started_at": "2023-10-27T11:00:02.000000+00:00",
  "completed_at": "2023-10-27T11:00:03.500000+00:00",
  "queue_seconds": 2.0,
  "processing_seconds": 1.5,
  "duration_seconds": 3.5,
  "error_message": "Failed to process file: Unsupported format or file corrupted.",
  "doc_id": null
}
```

---

### 3a. Get Document Full Text

*   **Endpoint:** `GET /v1/documents/:doc_id/fulltext`
*   **Description:** Retrieves the complete extracted and cleaned text content of a successfully processed document. Requires the `doc_id` obtained from a completed job status.
*   **Auth:** Bearer Token required.
*   **Path Parameter:** `doc_id` (UUID)

**Example Request:**

```http
GET /v1/documents/987e6543-e21b-12d3-a456-426655440000/fulltext HTTP/1.1
Host: localhost:8000
Authorization: Bearer your_secret_token_here
X-Correlation-ID: aaaaaaaa-bbbb-cccc-dddd-hhhhhhhhhhhh
```

**Example Response (200 OK):**

```json
{
  "doc_id": "987e6543-e21b-12d3-a456-426655440000",
  "text": "This is the full extracted text content of the document.\nIt includes all processed lines combined into a single string.",
  "created_at": "2023-10-27T10:00:15.800000+00:00"
}
```

---

### 3b. Get Document Chunks

*   **Endpoint:** `GET /v1/documents/:doc_id/chunks`
*   **Description:** Retrieves a paginated list of processed text chunks for a document. Requires the `doc_id` from a completed job.
*   **Auth:** Bearer Token required.
*   **Path Parameter:** `doc_id` (UUID)
*   **Query Parameters:**
    *   `limit` (integer, optional, default: 50, max: 500): Number of chunks per page.
    *   `offset` (integer, optional, default: 0): Starting index for pagination.

**Example Request:**

```http
GET /v1/documents/987e6543-e21b-12d3-a456-426655440000/chunks?limit=2&offset=0 HTTP/1.1
Host: localhost:8000
Authorization: Bearer your_secret_token_here
X-Correlation-ID: aaaaaaaa-bbbb-cccc-dddd-iiiiiiiiiiii
```

**Example Response (200 OK):**

```json
{
  "chunks": [
    {
      "chunk_id": "chunkuuid-0001-aaaa",
      "doc_id": "987e6543-e21b-12d3-a456-426655440000",
      "page": null,
      "offset": 0,
      "byte_length": 150,
      "text": "This is the first chunk of the document content. It might end mid-sentence depending on the chunking strategy and byte limits.",
      "sequence": 0,
      "metadata": {
        "source_filename": "example.txt"
      },
      "created_at": "2023-10-27T10:00:15.900000+00:00"
    },
    {
      "chunk_id": "chunkuuid-0002-bbbb",
      "doc_id": "987e6543-e21b-12d3-a456-426655440000",
      "page": null,
      "offset": 120, // Offset accounts for overlap
      "byte_length": 160,
      "text": "depending on the chunking strategy and byte limits. This is the second chunk, continuing the text and potentially including overlap from the previous chunk.",
      "sequence": 1,
      "metadata": {
         "source_filename": "example.txt"
      },
      "created_at": "2023-10-27T10:00:15.950000+00:00"
    }
  ],
  "total_chunks": 5, // Example total
  "limit": 2,
  "offset": 0,
  "next_offset": 2
}
```

---

### Webhooks (Optional Callbacks)

If `callback_url` and `callback_secret` are provided during ingestion, ChunkSmith will send a POST request to your `callback_url` when the job reaches a final state (`completed`, `completed_with_errors`, `failed`).

**Important:** The `callback_secret` must be at least 32 bytes long for security purposes. The service will reject requests with shorter secrets if a `callback_url` is provided.

*   **Method:** `POST`
*   **Endpoint:** Your provided `callback_url`.
*   **Payload:** The JSON body is identical to the response from `GET /v1/jobs/:job_id` for the specific job.
*   **Headers:**
    *   `Content-Type: application/json`
    *   `X-ChunkSmith-Timestamp`: ISO 8601 UTC timestamp (e.g., `2023-10-27T10:00:15.750123+00:00`). **Verify this is recent.**
    *   `X-ChunkSmith-Signature`: HMAC-SHA256 signature (hex digest) prefixed with `sha256=`. Calculated over `timestamp + "." + raw_request_body`. **Verify this using your `callback_secret`.**

**Example Received Webhook Request:**

```http
POST /your-webhook-handler HTTP/1.1
Host: yourapp.com
Content-Type: application/json
User-Agent: ChunkSmith-Webhook-Client/0.1.0
X-ChunkSmith-Timestamp: 2023-10-27T10:00:15.750123+00:00
X-ChunkSmith-Signature: sha256=abcdef1234567890abcdef1234567890abcdef1234567890abcdef123456
X-Correlation-ID: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee # Original request correlation ID

{
  "job_id": "123e4567-e89b-12d3-a456-426614174000",
  "status": "completed",
  "queued_at": "2023-10-27T10:00:00.123456+00:00",
  "started_at": "2023-10-27T10:00:05.500000+00:00",
  "completed_at": "2023-10-27T10:00:15.750000+00:00",
  "queue_seconds": 5.376544,
  "processing_seconds": 10.25,
  "duration_seconds": 15.626544,
  "error_message": null,
  "doc_id": "987e6543-e21b-12d3-a456-426655440000"
}
```

**Client Responsibilities:**
1.  **Verify Timestamp:** Check `X-ChunkSmith-Timestamp` against the current time (allow a small tolerance, e.g., 5 minutes).
2.  **Verify Signature:** Calculate the expected HMAC-SHA256 signature using your `callback_secret` and the received timestamp + `.` + raw body. Compare securely (e.g., `hmac.compare_digest`) against the `X-ChunkSmith-Signature` header.
3.  **Process Payload:** If valid, use the job status details. Track `job_id`s to handle potential retries idempotently.
4.  **Respond Quickly:** Respond to the webhook POST request with a `2xx` status code promptly (e.g., `200 OK`, `202 Accepted`, `204 No Content`) to acknowledge receipt. Process the payload asynchronously if necessary.

## Scaling Workers

To increase processing throughput, you can run multiple `chunksmith-worker` instances.

**Method 1: Docker Compose Scale**

```bash
# Start 3 worker instances (adjust the number as needed)
docker compose up --build -d --scale chunksmith-worker=3
```

**Method 2: Edit `docker-compose.yml`**

Modify the `deploy.replicas` value under the `chunksmith-worker` service definition before running `docker compose up`.

```yaml
services:
  # ... other services
  chunksmith-worker:
    # ... other settings
    deploy:
      replicas: 3 # Changed from 1
```

## Development

*   **Dependencies:** Managed using `uv` via `pyproject.toml`. Install dev dependencies with `uv sync --dev`.
*   **Database Migrations:** Uses `yoyo-migrations`. Migrations are automatically applied by the `chunksmith-api` service on startup. To manually apply or manage migrations, you might run `yoyo` inside the container or connect locally if the database port is exposed.
*   **Running Tests:** Execute integration tests using `pytest`. Ensure the services are running.
    ```bash
    uv run pytest tests/test_ingestion_flow.py -v -s
    ```

## License

This project is distributed under the Apache 2.0 license.