import logging
from typing import Any, Optional, List, Dict, Union
import asyncio
import tempfile
import os
from pathlib import Path

from langchain_docling import DoclingLoader
from langchain.text_splitter import MarkdownTextSplitter
from langchain_core.documents import Document

from core.config import settings

logger = logging.getLogger(__name__)


class IngestionResult:
    """Holds the results of the ingestion process."""

    def __init__(self):
        self.full_text: Optional[str] = None
        self.chunks: list = []
        self.page_count: int = 0


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
        Processes document content using DoclingLoader for extraction, OCR,
        and then chunks the result using MarkdownTextSplitter.

        Args:
            content_source: File path (str), raw bytes, or text string.
            is_file_path: Boolean indicating if the content_source is a file path.
            content_type: MIME type (optional, useful for raw bytes).
            filename: Original filename (optional, useful for text/bytes input).

        Returns:
            IngestionResult containing extracted full text and chunks.
        """
        result = IngestionResult()
        processing_start_time = asyncio.get_event_loop().time()
        source_repr = filename or (
            f"{type(content_source).__name__} input ({len(content_source)} bytes)"
            if isinstance(content_source, bytes)
            else f"text input ({len(content_source)} chars)"
        )
        logger.info(
            "Starting ingestion process.",
            extra={"source": source_repr, "content_type": content_type},
        )

        loaded_docs: List[Document] = []
        input_path_for_docling = None
        temp_file_handle = None
        cleanup_temp_file = False

        try:
            # --- 1. Determine Input Type and Prepare for Processing ---
            if isinstance(content_source, str):
                # --- Raw Text Input (from JSON) ---
                logger.debug("Processing raw text input (from string).")
                doc = Document(
                    page_content=content_source,
                    metadata={"source": filename or "text_payload", "page": 1},
                )
                loaded_docs = [doc]
                result.page_count = 1

            elif isinstance(content_source, bytes):
                # --- Bytes Input (from uploaded file) ---
                is_plain_text = (content_type in ["text/plain", "text/markdown"]) or (
                    filename
                    and (
                        filename.lower().endswith(".txt")
                        or filename.lower().endswith(".md")
                    )
                )
                logger.debug(
                    "Processing raw bytes input.",
                    extra={"is_plain_text": is_plain_text},
                )

                if is_plain_text:
                    # Decode and treat as raw text
                    try:
                        text_content = content_source.decode("utf-8")
                        doc = Document(
                            page_content=text_content,
                            metadata={"source": filename or "bytes_payload", "page": 1},
                        )
                        loaded_docs = [doc]
                        result.page_count = 1
                        logger.debug("Decoded bytes to text, skipping DoclingLoader.")
                    except UnicodeDecodeError as e:
                        logger.error(
                            "Failed to decode supposed text file bytes as UTF-8.",
                            extra={"error": str(e)},
                        )
                        raise ValueError(
                            "Failed to decode file content as UTF-8 text."
                        ) from e
                else:
                    logger.debug(
                        "Saving non-plain-text bytes to temporary file for Docling."
                    )
                    suffix = (
                        Path(filename).suffix
                        if filename
                        else (
                            f".{content_type.split('/')[-1]}"
                            if content_type
                            else ".bin"
                        )
                    )
                    with tempfile.NamedTemporaryFile(
                        mode="wb", delete=False, suffix=suffix
                    ) as temp_file_handle:
                        temp_file_handle.write(content_source)
                        input_path_for_docling = temp_file_handle.name
                    logger.info(
                        "Bytes saved to temporary file.",
                        extra={"path": input_path_for_docling},
                    )
                    cleanup_temp_file = True

            else:
                raise ValueError(
                    f"Unsupported content_source type: {type(content_source)}"
                )

            # --- 2. Load documents using DoclingLoader (if needed) ---
            if input_path_for_docling:
                if not Path(input_path_for_docling).exists():
                    raise FileNotFoundError(
                        f"Temporary file for Docling not found at: {input_path_for_docling}"
                    )
                logger.info(
                    "Initializing DoclingLoader.",
                    extra={"path": input_path_for_docling},
                )
                loader = DoclingLoader(
                    file_path=input_path_for_docling, export_type="markdown"
                )
                loaded_docs = loader.load()
                if not loaded_docs:
                    logger.warning(
                        "DoclingLoader returned no documents.",
                        extra={"source": source_repr},
                    )
                    return result
                logger.info(
                    "DoclingLoader finished.", extra={"doc_count": len(loaded_docs)}
                )
                result.page_count = loaded_docs[-1].metadata.get(
                    "page", len(loaded_docs)
                )

            # --- 3. Ensure Documents are Available for Splitting ---
            if not loaded_docs:
                logger.warning(
                    "No document content available for splitting.",
                    extra={"source": source_repr},
                )
                return result

            # --- 4. Configure & Run Markdown Text Splitter ---
            logger.info("Splitting content into chunks using MarkdownTextSplitter...")
            markdown_splitter = MarkdownTextSplitter(
                chunk_size=settings.CHUNK_SIZE_BYTES,
                chunk_overlap=settings.CHUNK_OVERLAP_BYTES,
            )
            split_chunks_docs: List[Document] = markdown_splitter.split_documents(
                loaded_docs
            )
            if not split_chunks_docs:
                logger.warning(
                    "MarkdownTextSplitter produced no chunks.",
                    extra={"source": source_repr},
                )
                result.full_text = "\n\n".join(
                    [doc.page_content for doc in loaded_docs]
                ).strip()
                return result
            result.full_text = "\n\n".join(
                [doc.page_content for doc in loaded_docs]
            ).strip()

            # --- 5. Format Chunks for Database ---
            formatted_chunks: List[Dict[str, Any]] = []
            for i, chunk_doc in enumerate(split_chunks_docs):
                chunk_text = chunk_doc.page_content
                final_metadata = chunk_doc.metadata or {}
                page = final_metadata.get("page")
                was_file_input = isinstance(content_source, bytes)
                if page is None and not was_file_input:
                    page = 1
                    final_metadata["page"] = 1
                elif page is None:
                    logger.debug(
                        "Chunk missing 'page' metadata. Defaulting to 1.",
                        extra={"sequence": i},
                    )
                    page = 1
                    final_metadata["page"] = 1
                if "source" not in final_metadata and filename:
                    final_metadata["source"] = filename
                offset = None
                byte_length = len(chunk_text.encode("utf-8"))
                chunk_data = {
                    "page": page,
                    "offset": offset,
                    "byte_length": byte_length,
                    "text": chunk_text,
                    "sequence": i,
                    "metadata": final_metadata,
                }
                formatted_chunks.append(chunk_data)
            result.chunks = formatted_chunks
            logger.info("Generated chunks.", extra={"count": len(result.chunks)})

        except FileNotFoundError as e:
            logger.error(
                "Input file error during ingestion.",
                extra={"source": source_repr, "error": str(e)},
            )
            raise
        except ImportError as e:
            logger.critical(
                "Docling/Langchain import error. Ensure dependencies are installed.",
                extra={"error": str(e)},
                exc_info=True,
            )
            raise
        except Exception as e:
            logger.exception(
                "Ingestion pipeline failed.",
                extra={"source": source_repr, "error": str(e)},
            )
            raise
        finally:
            # --- Cleanup Temporary File (if created for bytes input) ---
            if cleanup_temp_file and input_path_for_docling:
                try:
                    if Path(input_path_for_docling).exists():
                        os.remove(input_path_for_docling)
                        logger.debug(
                            "Removed temporary input file.",
                            extra={"path": input_path_for_docling},
                        )
                    else:
                        logger.debug(
                            "Temporary input file already removed or never existed.",
                            extra={"path": input_path_for_docling},
                        )
                except OSError as e:
                    logger.error(
                        "Error removing temporary file.",
                        extra={"path": input_path_for_docling, "error": str(e)},
                    )

        processing_duration = asyncio.get_event_loop().time() - processing_start_time
        logger.info(
            "Ingestion finished.",
            extra={
                "source": source_repr,
                "duration_seconds": round(processing_duration, 2),
                "chunk_count": len(result.chunks),
                "page_count": result.page_count,
            },
        )
        return result


ingestion_service = IngestionService()
