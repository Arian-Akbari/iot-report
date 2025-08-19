import asyncio
import io
import json
from datetime import datetime
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd

# Additional imports for better extraction
import pdfplumber
import PyPDF2
import pytesseract
from pdfminer.high_level import extract_text as pdfminer_extract_text
from PIL import Image

import models


async def extract_text_from_pdf_robust(pdf_path):
    """Robust PDF text extraction with multiple methods and OCR fallback"""

    print(f"   🔍 Trying multiple extraction methods for {pdf_path.name}")

    # Method 1: PyMuPDF (current method - fast and usually good)
    try:
        doc = fitz.open(pdf_path)
        text_parts = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                text_parts.append(text)
        doc.close()

        full_text = "\n".join(text_parts)
        if len(full_text.strip()) > 100:  # Good extraction
            print(f"   ✅ PyMuPDF extraction successful: {len(full_text)} chars")
            return full_text
        else:
            print(f"   ⚠️  PyMuPDF extracted minimal text: {len(full_text)} chars")
    except Exception as e:
        print(f"   ❌ PyMuPDF failed: {e}")

    # Method 2: pdfplumber (better for complex layouts)
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text_parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text and text.strip():
                    text_parts.append(text)

        full_text = "\n".join(text_parts)
        if len(full_text.strip()) > 100:
            print(f"   ✅ pdfplumber extraction successful: {len(full_text)} chars")
            return full_text
        else:
            print(f"   ⚠️  pdfplumber extracted minimal text: {len(full_text)} chars")
    except Exception as e:
        print(f"   ❌ pdfplumber failed: {e}")

    # Method 3: PDFMiner (good for complex PDFs)
    try:
        full_text = pdfminer_extract_text(str(pdf_path))
        if len(full_text.strip()) > 100:
            print(f"   ✅ PDFMiner extraction successful: {len(full_text)} chars")
            return full_text
        else:
            print(f"   ⚠️  PDFMiner extracted minimal text: {len(full_text)} chars")
    except Exception as e:
        print(f"   ❌ PDFMiner failed: {e}")

    # Method 4: PyPDF2 (lightweight fallback)
    try:
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            text_parts = []
            for page in reader.pages:
                text = page.extract_text()
                if text and text.strip():
                    text_parts.append(text)

        full_text = "\n".join(text_parts)
        if len(full_text.strip()) > 100:
            print(f"   ✅ PyPDF2 extraction successful: {len(full_text)} chars")
            return full_text
        else:
            print(f"   ⚠️  PyPDF2 extracted minimal text: {len(full_text)} chars")
    except Exception as e:
        print(f"   ❌ PyPDF2 failed: {e}")

    # Method 5: OCR with Tesseract (for scanned PDFs)
    try:
        print(f"   🔍 Attempting OCR extraction (this may take longer)...")
        doc = fitz.open(pdf_path)
        text_parts = []

        for page_num in range(min(5, len(doc))):  # Limit to first 5 pages for OCR
            page = doc.load_page(page_num)
            pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))  # Higher resolution
            img_data = pix.tobytes("png")
            img = Image.open(io.BytesIO(img_data))

            # OCR the image
            ocr_text = pytesseract.image_to_string(img, lang="eng")
            if ocr_text and ocr_text.strip():
                text_parts.append(f"=== PAGE {page_num + 1} (OCR) ===\n{ocr_text}")

        doc.close()
        full_text = "\n".join(text_parts)

        if len(full_text.strip()) > 100:
            print(f"   ✅ OCR extraction successful: {len(full_text)} chars")
            return full_text
        else:
            print(f"   ⚠️  OCR extracted minimal text: {len(full_text)} chars")

    except Exception as e:
        print(f"   ❌ OCR failed: {e}")

    # If all methods fail
    print(f"   ❌ All extraction methods failed for {pdf_path.name}")
    raise Exception("All extraction methods failed")


async def extract_paper_info(text, filename, max_retries=3):
    """Extract structured information with retry logic"""

    json_schema = {
        "type": "object",
        "properties": {
            "title": {"type": "string", "description": "Paper title"},
            "abstract": {"type": "string", "description": "Paper abstract"},
            "method": {"type": "string", "description": "Proposed method"},
            "objectives": {"type": "string", "description": "Research objectives"},
            "categories": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Keywords from abstract",
            },
            "summary": {
                "type": "string",
                "description": "Comprehensive summary covering: problem addressed, main contributions, methodology, key findings, practical applications, significance, and future implications. Include technical details, algorithms, datasets, performance metrics (300-500 words).",
            },
        },
        "required": [
            "title",
            "abstract",
            "method",
            "objectives",
            "categories",
            "summary",
        ],
        "additionalProperties": False,
    }

    for attempt in range(max_retries):
        try:
            response = models.client.chat.completions.create(
                model="openai/gpt-4o-mini",
                messages=[
                    {
                        "role": "system",
                        "content": "You are a scientific paper analysis assistant. If information is not available, use empty string for text fields and empty array for categories.",
                    },
                    {
                        "role": "user",
                        "content": f"Extract paper information from: {text}",  # Increased limit
                    },
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": "paper_analysis",
                        "description": "Structured paper information extraction",
                        "schema": json_schema,
                        "strict": True,
                    },
                },
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"   ❌ Attempt {attempt + 1} failed for {filename}: {e}")
            if attempt == max_retries - 1:
                raise Exception(
                    f"All {max_retries} attempts failed for {filename}: {e}"
                )
            await asyncio.sleep(2**attempt)


async def save_to_excel(record, excel_path):
    """Save record to Excel file"""
    try:
        record["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if excel_path.exists():
            df = pd.read_excel(excel_path)
            df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
        else:
            df = pd.DataFrame([record])

        df.to_excel(excel_path, index=False)

    except Exception as e:
        raise Exception(f"Error saving to Excel: {e}")


async def process_single_pdf(pdf_path):
    """Process a single PDF and return the record with robust extraction"""
    try:
        # Extract text with robust methods
        text = await extract_text_from_pdf_robust(pdf_path)
        if not text.strip():
            raise Exception("No text extracted")

        # Process with OpenAI (with retry)
        info_json = await extract_paper_info(text, pdf_path.name)

        # Parse JSON
        record = json.loads(info_json)
        record["filename"] = pdf_path.name

        return record, None

    except Exception as e:
        # Return clean empty record instead of error messages
        clean_record = {
            "filename": pdf_path.name,
            "title": "",
            "abstract": "",
            "method": "",
            "objectives": "",
            "categories": [],
            "summary": "",
        }
        return clean_record, str(e)
