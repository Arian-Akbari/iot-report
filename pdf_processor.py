import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

import fitz  # PyMuPDF
import pandas as pd

# Import your models module
import models


async def extract_text_from_pdf(pdf_path):
    """Extract text from PDF using PyMuPDF"""
    try:
        doc = fitz.open(pdf_path)
        full_text = []
        for page in doc:
            text = page.get_text()
            if text.strip():  # Only add non-empty pages
                full_text.append(text)
        doc.close()
        return "\n".join(full_text)
    except Exception as e:
        print(f"Error extracting text from {pdf_path}: {e}")
        return ""


async def extract_paper_info(text, filename):
    """Extract structured information using OpenAI's JSON Schema validation"""

    # Define your JSON schema
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
                "description": "Comprehensive and detailed summary covering: the problem addressed, main contributions, methodology used, key findings/results, practical applications, significance to the field, and future implications. Include specific details about algorithms, techniques, datasets, performance metrics, and comparative analysis when mentioned. Make it rich in context and technical depth (aim for 300-500 words).",
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

    try:
        response = models.client.chat.completions.create(
            model="openai/gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": "You are a scientific paper analysis assistant.",
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
                    "strict": True,  # This enforces strict validation
                },
            },
        )

        return response.choices[0].message.content

    except Exception as e:
        print(f"Error processing {filename}: {e}")
        return None


async def save_to_excel(record, excel_path):
    """Save record to Excel file incrementally"""
    try:
        # Add timestamp and filename
        record["processed_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if excel_path.exists():
            df = pd.read_excel(excel_path)
            df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
        else:
            df = pd.DataFrame([record])

        df.to_excel(excel_path, index=False)
        print(f"✅ Saved record to {excel_path}")

    except Exception as e:
        print(f"Error saving to Excel: {e}")


async def process_single_pdf(pdf_path, excel_output="papers_summary.xlsx"):
    """Process a single PDF file and save results to Excel"""
    pdf_file = Path(pdf_path)

    if not pdf_file.exists():
        print(f"❌ PDF file '{pdf_path}' not found!")
        return False

    if not pdf_file.suffix.lower() == ".pdf":
        print(f"❌ File '{pdf_path}' is not a PDF!")
        return False

    print(f"📄 Processing: {pdf_file.name}")

    # Extract text
    print("   📝 Extracting text...")
    text = await extract_text_from_pdf(pdf_file)

    if not text.strip():
        print("   ⚠️  No text extracted, skipping...")
        return False

    print(f"   📊 Extracted {len(text)} characters")

    # Process with OpenAI
    print("   🤖 Processing with OpenAI...")
    info_json = await extract_paper_info(text, pdf_file.name)

    if info_json is None:
        print("   ❌ Failed to extract information from OpenAI")
        record = {
            "filename": pdf_file.name,
            "title": pdf_file.name,
            "abstract": "OpenAI processing failed",
            "method": "OpenAI processing failed",
            "objectives": "OpenAI processing failed",
            "categories": ["Error"],
            "summary": "Failed to process with OpenAI",
        }
    else:
        try:
            record = json.loads(info_json)
            record["filename"] = pdf_file.name
        except json.JSONDecodeError as e:
            print(f"   ❌ JSON parsing error: {e}")
            record = {
                "filename": pdf_file.name,
                "title": pdf_file.name,
                "abstract": "JSON parsing failed",
                "method": "JSON parsing failed",
                "objectives": "JSON parsing failed",
                "categories": ["Error"],
                "summary": "Failed to parse OpenAI response",
            }

    # Save to Excel
    excel_path = Path(excel_output)
    await save_to_excel(record, excel_path)
    print(f"✅ Results saved to {excel_output}")
    return True


async def process_papers(test_mode=True):
    """Main processing function for multiple papers"""
    papers_dir = Path("papers")
    excel_output = Path("papers_summary.xlsx")

    if not papers_dir.exists():
        print(f"❌ Papers directory '{papers_dir}' not found!")
        return

    pdf_files = list(papers_dir.glob("*.pdf"))

    if not pdf_files:
        print(f"❌ No PDF files found in '{papers_dir}'")
        return

    print(f"📁 Found {len(pdf_files)} PDF files")

    if test_mode:
        print("🧪 TEST MODE: Processing first file only")
        pdf_files = pdf_files[:1]

    for idx, pdf_file in enumerate(pdf_files, 1):
        print(f"\n📄 Processing {idx}/{len(pdf_files)}: {pdf_file.name}")

        # Extract text
        print("   📝 Extracting text...")
        text = await extract_text_from_pdf(pdf_file)

        if not text.strip():
            print("   ⚠️  No text extracted, skipping...")
            continue

        print(f"   📊 Extracted {len(text)} characters")

        # Process with OpenAI
        print("   🤖 Processing with OpenAI...")
        info_json = await extract_paper_info(text, pdf_file.name)

        if info_json is None:
            print("   ❌ Failed to extract information from OpenAI")
            record = {
                "filename": pdf_file.name,
                "title": pdf_file.name,
                "abstract": "OpenAI processing failed",
                "method": "OpenAI processing failed",
                "objectives": "OpenAI processing failed",
                "categories": ["Error"],
                "summary": "Failed to process with OpenAI",
            }
        else:
            try:
                record = json.loads(info_json)
                record["filename"] = pdf_file.name

                if test_mode:
                    print("\n📋 EXTRACTED INFORMATION:")
                    print(f"Title: {record.get('title', 'N/A')}")
                    print(f"Abstract: {record.get('abstract', 'N/A')[:200]}...")
                    print(f"Method: {record.get('method', 'N/A')[:200]}...")
                    print(f"Objectives: {record.get('objectives', 'N/A')[:200]}...")
                    print(f"Categories: {record.get('categories', 'N/A')}")
                    print(f"Summary: {record.get('summary', 'N/A')}")

                    user_input = input("\n✅ Does this look good? (y/n): ")
                    if user_input.lower() != "y":
                        print("❌ Test failed. Please check your setup.")
                        return
                    else:
                        print("✅ Test passed! You can now run the full processing.")
                        return

            except json.JSONDecodeError as e:
                print(f"   ❌ JSON parsing error: {e}")
                record = {
                    "filename": pdf_file.name,
                    "title": pdf_file.name,
                    "abstract": "JSON parsing failed",
                    "method": "JSON parsing failed",
                    "objectives": "JSON parsing failed",
                    "categories": ["Error"],
                    "summary": "Failed to parse OpenAI response",
                }

        # Save to Excel
        if not test_mode:
            await save_to_excel(record, excel_output)

    if not test_mode:
        print(f"\n🎉 Processing complete! Results saved to '{excel_output}'")


if __name__ == "__main__":
    print("🚀 Async PDF Paper Processor")
    print("=" * 50)

    # Example usage:
    # 1. Process a single PDF file:
    # asyncio.run(process_single_pdf("path/to/your/paper.pdf", "output.xlsx"))

    # 2. Process multiple papers from papers directory:
    # asyncio.run(process_papers(test_mode=False))

    # 3. Test mode (first file only):
    asyncio.run(process_papers(test_mode=True))
