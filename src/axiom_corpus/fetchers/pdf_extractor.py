"""PDF text extraction for IRS guidance documents.

Uses PyMuPDF (fitz) for fast, high-quality text extraction from PDF files.
Handles both native text PDFs and scanned documents.
"""

import re
from io import BytesIO
from typing import Union

import fitz  # PyMuPDF


class PDFTextExtractor:
    """Extract text content from PDF documents.

    Uses PyMuPDF for text extraction with options for cleaning
    and formatting the output.
    """

    def __init__(self, clean_whitespace: bool = True):
        """Initialize the extractor.

        Args:
            clean_whitespace: Whether to clean up excessive whitespace
        """
        self.clean_whitespace = clean_whitespace

    def extract_text(self, pdf_content: Union[bytes, BytesIO]) -> str:
        """Extract text from PDF bytes.

        Args:
            pdf_content: PDF file content as bytes or BytesIO

        Returns:
            Extracted text as a string

        Raises:
            ValueError: If the PDF cannot be read
        """
        if isinstance(pdf_content, BytesIO):
            pdf_content = pdf_content.read()

        try:
            with fitz.open(stream=pdf_content, filetype="pdf") as doc:
                pages = []
                for page in doc:
                    # Extract text with blocks option for better layout preservation
                    text = page.get_text("text")
                    pages.append(text)

                full_text = "\n\n".join(pages)

        except Exception as e:
            raise ValueError(f"Failed to read PDF: {e}") from e

        if self.clean_whitespace:
            full_text = self._clean_text(full_text)

        return full_text

    def extract_text_from_file(self, path: str) -> str:
        """Extract text from a PDF file path.

        Args:
            path: Path to the PDF file

        Returns:
            Extracted text as a string
        """
        with open(path, "rb") as f:
            return self.extract_text(f.read())

    def _clean_text(self, text: str) -> str:
        """Clean up extracted text.

        - Reduce excessive blank lines
        - Normalize spacing (but preserve intentional multiple spaces in tables)
        - Remove control characters

        Args:
            text: Raw extracted text

        Returns:
            Cleaned text
        """
        # Remove control characters except newlines and tabs
        text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", "", text)

        # Reduce more than 3 consecutive newlines to 2
        text = re.sub(r"\n{4,}", "\n\n\n", text)

        # Clean up lines that are just whitespace
        lines = text.split("\n")
        cleaned_lines = []
        for line in lines:
            # Strip trailing whitespace but preserve leading for indentation
            line = line.rstrip()
            cleaned_lines.append(line)

        text = "\n".join(cleaned_lines)

        # Remove trailing whitespace at end of document
        text = text.strip()

        return text

    def get_metadata(self, pdf_content: Union[bytes, BytesIO]) -> dict:
        """Extract metadata from a PDF document.

        Args:
            pdf_content: PDF file content as bytes or BytesIO

        Returns:
            Dictionary with metadata fields (title, author, subject, etc.)
        """
        if isinstance(pdf_content, BytesIO):
            pdf_content = pdf_content.read()

        with fitz.open(stream=pdf_content, filetype="pdf") as doc:
            metadata = doc.metadata
            return {
                "title": metadata.get("title", ""),
                "author": metadata.get("author", ""),
                "subject": metadata.get("subject", ""),
                "creator": metadata.get("creator", ""),
                "producer": metadata.get("producer", ""),
                "creation_date": metadata.get("creationDate", ""),
                "modification_date": metadata.get("modDate", ""),
                "page_count": len(doc),
            }
