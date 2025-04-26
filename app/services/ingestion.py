import logging
from typing import Any, Optional, List, Dict, Union
import asyncio
import tempfile
import os
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime, timezone

from langchain_docling import DoclingLoader
from langchain.text_splitter import MarkdownTextSplitter
from langchain_core.documents import Document

from core.config import settings

logger = logging.getLogger(__name__)


class IngestionResult:
    """Holds the results of the ingestion process."""

    def __init__(self):
        self.full_text: Optional[str] = None
        self.chunks: List[Dict[str, Any]] = []
        self.page_count: int = 0
        self.source_filename: Optional[str] = None
        self.detected_content_type: Optional[str] = None
        self.source_url: Optional[str] = None
        self.fetched_at: Optional[str] = None


class IngestionService:
    """
    Orchestrates the document ingestion pipeline using DoclingLoader.
    """

    def __init__(self):
        pass

    async def process(
        self,
        content_source: Union[str, bytes],
        content_type: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> IngestionResult:
        """
        Processes document content (file path, URL, raw bytes, or text string)
        using DoclingLoader for extraction/OCR, then chunks the result.

        Args:
            content_source: File path (str), URL (str), raw bytes, or text string.
            content_type: MIME type (optional, useful for raw bytes or URL).
            filename: Original filename (optional, useful for text/bytes input).

        Returns:
            IngestionResult containing extracted text, chunks, and metadata.
        """
        result = IngestionResult()
        result.source_filename = filename
        result.detected_content_type = content_type

        processing_start_time = asyncio.get_event_loop().time()
        is_url_input = False
        source_url_value: Optional[str] = None
        fetched_at_iso: Optional[str] = None

        source_repr = filename or (
            f"url: {content_source[:100]}..."
            if isinstance(content_source, str)
            and content_source.startswith(("http://", "https://"))
            else f"{type(content_source).__name__} input ({len(content_source)} bytes)"
            if isinstance(content_source, bytes)
            else f"text input ({len(content_source)} chars)"
        )
        log_extra = {
            "source_repr": source_repr,
            "input_content_type": content_type,
            "input_filename": filename,
        }

        logger.info("Starting ingestion process.", extra=log_extra)

        loaded_docs: List[Document] = []
        input_target_for_docling = None
        temp_file_handle = None
        cleanup_temp_file = False

        try:
            if isinstance(content_source, str):
                try:
                    parsed = urlparse(content_source)
                    if parsed.scheme in ["http", "https"] and parsed.netloc:
                        is_url_input = True
                        source_url_value = content_source
                        input_target_for_docling = source_url_value
                        result.source_url = source_url_value
                        logger.debug("Processing URL input.", extra=log_extra)
                    else:
                        logger.debug(
                            "Processing raw text input (from non-URL string).",
                            extra=log_extra,
                        )
                        doc = Document(
                            page_content=content_source,
                            metadata={"source": filename or "text_payload", "page": 1},
                        )
                        loaded_docs = [doc]
                        result.page_count = 1
                        result.detected_content_type = "text/plain"
                except ValueError:
                    logger.debug(
                        "Input string parsing failed, processing as raw text.",
                        extra=log_extra,
                    )
                    doc = Document(
                        page_content=content_source,
                        metadata={"source": filename or "text_payload", "page": 1},
                    )
                    loaded_docs = [doc]
                    result.page_count = 1
                    result.detected_content_type = "text/plain"

            elif isinstance(content_source, bytes):
                is_plain_text = (content_type in ["text/plain", "text/markdown"]) or (
                    filename
                    and (
                        filename.lower().endswith(".txt")
                        or filename.lower().endswith(".md")
                    )
                )
                log_extra["is_plain_text_guess"] = is_plain_text
                logger.debug("Processing raw bytes input.", extra=log_extra)

                if is_plain_text:
                    try:
                        text_content = content_source.decode("utf-8")
                        doc = Document(
                            page_content=text_content,
                            metadata={"source": filename or "bytes_payload", "page": 1},
                        )
                        loaded_docs = [doc]
                        result.page_count = 1
                        result.detected_content_type = content_type or "text/plain"
                        logger.debug(
                            "Decoded bytes to text, skipping DoclingLoader.",
                            extra=log_extra,
                        )
                    except UnicodeDecodeError as e:
                        logger.error(
                            "Failed to decode supposed text file bytes as UTF-8.",
                            extra={**log_extra, "error": str(e)},
                        )
                        raise ValueError(
                            "Failed to decode file content as UTF-8 text."
                        ) from e
                else:
                    logger.debug(
                        "Saving non-plain-text bytes to temporary file for Docling.",
                        extra=log_extra,
                    )
                    suffix = Path(filename).suffix if filename else (".bin")
                    fd, temp_file_path = tempfile.mkstemp(suffix=suffix)
                    try:
                        with os.fdopen(fd, "wb") as temp_file_handle:
                            temp_file_handle.write(content_source)
                        input_target_for_docling = temp_file_path
                        cleanup_temp_file = True
                        log_extra["temp_file_path"] = input_target_for_docling
                        logger.info("Bytes saved to temporary file.", extra=log_extra)
                    except Exception as e:
                        os.close(fd)
                        if os.path.exists(temp_file_path):
                            os.remove(temp_file_path)
                        logger.exception(
                            "Failed to write to temporary file", extra=log_extra
                        )
                        raise IOError(
                            "Failed to save uploaded content for processing."
                        ) from e

            else:
                err_msg = f"Unsupported content_source type: {type(content_source)}"
                logger.error(err_msg, extra=log_extra)
                raise ValueError(err_msg)

            if input_target_for_docling:
                load_start_time = asyncio.get_event_loop().time()
                if is_url_input:
                    fetched_at_iso = datetime.now(timezone.utc).isoformat()
                    result.fetched_at = fetched_at_iso
                    log_extra["fetched_at"] = fetched_at_iso

                logger.info("Initializing DoclingLoader.", extra=log_extra)
                try:
                    loader = DoclingLoader(
                        file_path=input_target_for_docling,
                        export_type="markdown",
                    )
                    loaded_docs = loader.load()
                    load_duration = asyncio.get_event_loop().time() - load_start_time
                    log_extra["docling_load_duration_sec"] = round(load_duration, 2)

                    if not loaded_docs:
                        logger.warning(
                            "DoclingLoader returned no documents.", extra=log_extra
                        )
                        return result

                    logger.info(
                        "DoclingLoader finished.",
                        extra={**log_extra, "doc_count": len(loaded_docs)},
                    )

                    if is_url_input and loaded_docs:
                        logger.debug(
                            "Injecting URL metadata into loaded documents.",
                            extra=log_extra,
                        )
                        for doc in loaded_docs:
                            doc.metadata = doc.metadata or {}
                            doc.metadata["source_url"] = source_url_value
                            doc.metadata["fetched_at"] = fetched_at_iso
                            doc.metadata["source"] = source_url_value
                            if not result.source_filename and doc.metadata.get(
                                "filename"
                            ):
                                result.source_filename = doc.metadata["filename"]
                            if not result.detected_content_type and doc.metadata.get(
                                "content_type"
                            ):
                                result.detected_content_type = doc.metadata[
                                    "content_type"
                                ]
                        log_extra["detected_filename"] = result.source_filename
                        log_extra["detected_content_type"] = (
                            result.detected_content_type
                        )

                    if loaded_docs:
                        result.page_count = loaded_docs[-1].metadata.get(
                            "page", len(loaded_docs)
                        )
                        log_extra["page_count"] = result.page_count

                except Exception as docling_err:
                    logger.exception(
                        "DoclingLoader failed.", extra=log_extra, exc_info=docling_err
                    )
                    raise docling_err

            if not loaded_docs:
                logger.warning(
                    "No document content available for splitting.", extra=log_extra
                )
                result.full_text = ""
                return result

            logger.info(
                "Splitting content into chunks using MarkdownTextSplitter...",
                extra=log_extra,
            )
            markdown_splitter = MarkdownTextSplitter(
                chunk_size=settings.CHUNK_SIZE_BYTES,
                chunk_overlap=settings.CHUNK_OVERLAP_BYTES,
                length_function=lambda text: len(text.encode("utf-8")),
            )
            split_chunks_docs: List[Document] = markdown_splitter.split_documents(
                loaded_docs
            )

            result.full_text = "\n\n".join(
                [doc.page_content for doc in loaded_docs]
            ).strip()
            log_extra["full_text_length_chars"] = len(result.full_text)

            if not split_chunks_docs:
                logger.warning(
                    "MarkdownTextSplitter produced no chunks.", extra=log_extra
                )
                return result

            formatted_chunks: List[Dict[str, Any]] = []
            for i, chunk_doc in enumerate(split_chunks_docs):
                chunk_text = chunk_doc.page_content
                final_metadata = chunk_doc.metadata or {}

                if is_url_input:
                    final_metadata["source"] = source_url_value
                    final_metadata["source_url"] = source_url_value
                    if fetched_at_iso:
                        final_metadata["fetched_at"] = fetched_at_iso
                elif "source" not in final_metadata:
                    final_metadata["source"] = filename or "unknown_source"

                page = final_metadata.get("page")
                if page is None:
                    page = 1
                    final_metadata["page"] = 1

                byte_length = len(chunk_text.encode("utf-8"))

                chunk_data = {
                    "doc_id": None,
                    "page": page,
                    "offset": None,
                    "byte_length": byte_length,
                    "text": chunk_text,
                    "sequence": i,
                    "metadata": final_metadata,
                }
                formatted_chunks.append(chunk_data)

            result.chunks = formatted_chunks
            log_extra["chunk_count"] = len(result.chunks)
            logger.info("Generated chunks.", extra=log_extra)

        except FileNotFoundError as e:
            logger.error(
                "Input file error during ingestion.",
                extra={**log_extra, "error": str(e)},
            )
            raise
        except ImportError as e:
            logger.critical(
                "Docling/Langchain import error. Check dependencies.",
                extra={**log_extra, "error": str(e)},
                exc_info=True,
            )
            raise
        except Exception:
            logger.exception(
                "Ingestion pipeline failed unexpectedly.",
                extra=log_extra,
                exc_info=True,
            )
            raise
        finally:
            if cleanup_temp_file and input_target_for_docling:
                try:
                    if os.path.exists(input_target_for_docling):
                        os.remove(input_target_for_docling)
                        logger.debug(
                            "Removed temporary input file.",
                            extra={**log_extra, "path": input_target_for_docling},
                        )
                    else:
                        logger.debug(
                            "Temporary input file already removed or not found.",
                            extra={**log_extra, "path": input_target_for_docling},
                        )
                except OSError as e:
                    logger.error(
                        "Error removing temporary file.",
                        extra={
                            **log_extra,
                            "path": input_target_for_docling,
                            "error": str(e),
                        },
                    )

        processing_duration = asyncio.get_event_loop().time() - processing_start_time
        log_extra["duration_seconds"] = round(processing_duration, 2)
        logger.info("Ingestion finished.", extra=log_extra)
        return result


ingestion_service = IngestionService()
