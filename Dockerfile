# Stage 1: Base image with common setup
FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

WORKDIR /app

# Install uv separately first for better layer caching
RUN pip install uv

# Copy only dependency file first for caching
COPY pyproject.toml README.md /app/

# Install dependencies
RUN uv sync --no-dev

# Copy the rest of the application code
COPY ./app /app/
COPY ./migrations /app/migrations/


# Stage 2: API image
FROM base AS api

WORKDIR /app

# Expose port for API
EXPOSE 80

# Define environment variables for uvicorn (can be inherited, but explicit is clear)
ENV MODULE_NAME="main"
ENV VARIABLE_NAME="app"

# Default command to run the API server
CMD ["uv", "run", "python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]


# Stage 3: Worker image
FROM base AS worker

WORKDIR /app

# Download core set of docling models during the build
# Rebuild the image if the models are updated
# RUN uv add huggingface_hub && uv run huggingface-cli download ds4sd/docling-models

# Default command to run the worker
CMD ["uv", "run", "python", "-m", "workers.worker"] 