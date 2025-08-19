import asyncio
import json
from datetime import datetime
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd

import models


async def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using PyMuPDF"""
    try:
        doc = fitz.open(pdf_path)
        full_text = []
        for page in doc:
            text = page.get_text()
            if text.strip():
                full_text.append(text)
        doc.close()
        return "\n".join(full_text)
    except Exception as e:
        raise Exception(f"Error extracting text from {pdf_path}: {e}")


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
                        "content": f"Extract paper information from: {text[:15000]}",
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
                max_tokens=3000,
                temperature=0.1,
            )

            return response.choices[0].message.content

        except Exception as e:
            print(f"   ❌ Attempt {attempt + 1} failed for {filename}: {e}")
            if attempt == max_retries - 1:
                raise Exception(
                    f"All {max_retries} attempts failed for {filename}: {e}"
                )
            await asyncio.sleep(2**attempt)  # Exponential backoff: 1s, 2s, 4s


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
    """Process a single PDF and return the record with clean error handling"""
    try:
        # Extract text
        text = await extract_text_from_pdf(pdf_path)
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
