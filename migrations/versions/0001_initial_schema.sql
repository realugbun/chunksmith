-- migrations/versions/0001_initial_schema.sql
-- depends: 

-- Create ENUM type for job status if it doesn't exist
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'job_status') THEN
        CREATE TYPE job_status AS ENUM ('queued', 'processing', 'completed', 'failed', 'completed_with_errors');
    END IF;
END$$;

-- Create jobs table
CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    status job_status NOT NULL DEFAULT 'queued',
    queued_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    queue_seconds FLOAT,
    processing_seconds FLOAT,
    duration_seconds FLOAT,
    doc_id UUID, -- FK constraint added after documents table is created
    error_message TEXT,
    deleted_at TIMESTAMPTZ
);

-- Create documents table
CREATE TABLE IF NOT EXISTS documents (
    doc_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL, -- FK constraint added later
    filename TEXT,
    content_type TEXT,
    file_path TEXT,
    full_text TEXT,
    page_count INT,
    source_url TEXT,
    fetched_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ
);

-- Create chunks table
CREATE TABLE IF NOT EXISTS chunks (
    chunk_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doc_id UUID NOT NULL, -- FK constraint added later
    page INT,
    "offset" INT,
    byte_length INT,
    text TEXT NOT NULL,
    sequence INT, -- Order index within the document
    metadata JSONB, -- For Docling metadata
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at TIMESTAMPTZ
);

-- Add Foreign Key constraints
-- Ensure jobs.doc_id references documents.doc_id
ALTER TABLE jobs
ADD CONSTRAINT fk_jobs_doc_id
FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
ON DELETE SET NULL; -- Or cascade, depending on desired behavior

-- Ensure documents.job_id references jobs.job_id
ALTER TABLE documents
ADD CONSTRAINT fk_documents_job_id
FOREIGN KEY (job_id) REFERENCES jobs(job_id)
ON DELETE CASCADE; -- If a job is deleted, cascade to the document

-- Ensure chunks.doc_id references documents.doc_id
ALTER TABLE chunks
ADD CONSTRAINT fk_chunks_doc_id
FOREIGN KEY (doc_id) REFERENCES documents(doc_id)
ON DELETE CASCADE; -- If a document is deleted, cascade to its chunks

-- Add Indexes for common lookups
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_deleted_at ON jobs(deleted_at);
CREATE INDEX IF NOT EXISTS idx_documents_job_id ON documents(job_id);
CREATE INDEX IF NOT EXISTS idx_documents_deleted_at ON documents(deleted_at);
CREATE INDEX IF NOT EXISTS idx_chunks_doc_id ON chunks(doc_id);
CREATE INDEX IF NOT EXISTS idx_chunks_deleted_at ON chunks(deleted_at);
CREATE INDEX IF NOT EXISTS idx_chunks_doc_page_offset ON chunks(doc_id, page, "offset"); -- For ordered retrieval 