import asyncio
import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd

# Import the pdf processor functions
from pdf_processor import (
    extract_paper_info,
    extract_text_from_pdf,
    process_single_pdf,
    save_to_excel,
)


async def process_pdf_batch(pdf_files, batch_num, excel_output):
    """Process a batch of PDF files with TRUE concurrency"""
    print(f"\n🔄 Processing Batch {batch_num} ({len(pdf_files)} files)")
    print("=" * 60)
    print(f"   🚀 Starting {len(pdf_files)} concurrent processes...")

    # Step 1: Extract text from all PDFs concurrently
    print("   📝 Extracting text from all PDFs simultaneously...")
    text_tasks = [extract_text_from_pdf(pdf_file) for pdf_file in pdf_files]
    texts = await asyncio.gather(*text_tasks, return_exceptions=True)

    # Step 2: Process valid texts with OpenAI concurrently
    print("   🤖 Processing with OpenAI simultaneously...")
    openai_tasks = []
    valid_indices = []

    for i, (pdf_file, text) in enumerate(zip(pdf_files, texts)):
        if isinstance(text, Exception):
            print(f"      ❌ Text extraction failed for {pdf_file.name}: {text}")
            continue
        elif not text.strip():
            print(f"      ⚠️  No text extracted from {pdf_file.name}")
            continue
        else:
            print(f"      ✅ {pdf_file.name}: {len(text)} chars extracted")
            openai_tasks.append(extract_paper_info(text, pdf_file.name))
            valid_indices.append(i)

    # Execute all OpenAI calls simultaneously
    if openai_tasks:
        print(f"   🔥 Sending {len(openai_tasks)} concurrent OpenAI requests...")
        openai_results = await asyncio.gather(*openai_tasks, return_exceptions=True)
    else:
        openai_results = []

    # Step 3: Save all results concurrently
    print("   💾 Saving results to Excel simultaneously...")
    save_tasks = []
    successful = 0
    failed = 0

    # Process files that had no text
    for i, (pdf_file, text) in enumerate(zip(pdf_files, texts)):
        if i not in valid_indices:
            record = {
                "filename": pdf_file.name,
                "title": pdf_file.name,
                "abstract": (
                    "No text extracted"
                    if not isinstance(text, Exception)
                    else "Text extraction error"
                ),
                "method": "Processing failed",
                "objectives": "Processing failed",
                "categories": ["Error"],
                "summary": "Failed to extract text from PDF",
            }
            save_tasks.append(save_to_excel(record, Path(excel_output)))
            failed += 1

    # Process OpenAI results
    for i, (valid_idx, openai_result) in enumerate(zip(valid_indices, openai_results)):
        pdf_file = pdf_files[valid_idx]

        if isinstance(openai_result, Exception):
            print(f"      ❌ OpenAI failed for {pdf_file.name}: {openai_result}")
            record = {
                "filename": pdf_file.name,
                "title": pdf_file.name,
                "abstract": "OpenAI processing failed",
                "method": "OpenAI processing failed",
                "objectives": "OpenAI processing failed",
                "categories": ["Error"],
                "summary": f"OpenAI error: {str(openai_result)}",
            }
            failed += 1
        elif openai_result is None:
            print(f"      ❌ OpenAI returned None for {pdf_file.name}")
            record = {
                "filename": pdf_file.name,
                "title": pdf_file.name,
                "abstract": "OpenAI processing failed",
                "method": "OpenAI processing failed",
                "objectives": "OpenAI processing failed",
                "categories": ["Error"],
                "summary": "OpenAI returned no response",
            }
            failed += 1
        else:
            try:
                record = json.loads(openai_result)
                record["filename"] = pdf_file.name
                print(f"      ✅ {pdf_file.name}: Successfully processed")
                successful += 1
            except json.JSONDecodeError as e:
                print(f"      ❌ JSON parsing failed for {pdf_file.name}: {e}")
                record = {
                    "filename": pdf_file.name,
                    "title": pdf_file.name,
                    "abstract": "JSON parsing failed",
                    "method": "JSON parsing failed",
                    "objectives": "JSON parsing failed",
                    "categories": ["Error"],
                    "summary": f"JSON parsing error: {str(e)}",
                }
                failed += 1

        save_tasks.append(save_to_excel(record, Path(excel_output)))

    # Execute all save operations concurrently
    if save_tasks:
        await asyncio.gather(*save_tasks, return_exceptions=True)

    print(f"\n📊 Batch {batch_num} Results: {successful} successful, {failed} failed")
    return successful, failed


async def batch_process_papers(
    batch_size=10, papers_dir="papers", excel_output="batch_papers_summary.xlsx"
):
    """Main function to process papers in batches with concurrent requests"""

    papers_path = Path(papers_dir)
    if not papers_path.exists():
        print(f"❌ Papers directory '{papers_dir}' not found!")
        return

    # Get all PDF files
    pdf_files = list(papers_path.glob("*.pdf"))

    if not pdf_files:
        print(f"❌ No PDF files found in '{papers_dir}'")
        return

    print(f"🚀 Starting Batch Processing")
    print(f"📁 Found {len(pdf_files)} PDF files")
    print(f"📦 Batch size: {batch_size}")
    print(f"💾 Output file: {excel_output}")
    print("=" * 60)

    # Process files in batches
    total_successful = 0
    total_failed = 0
    start_time = datetime.now()

    for i in range(0, len(pdf_files), batch_size):
        batch = pdf_files[i : i + batch_size]
        batch_num = (i // batch_size) + 1

        # Process batch
        successful, failed = await process_pdf_batch(batch, batch_num, excel_output)
        total_successful += successful
        total_failed += failed

        # Progress update
        processed = min(i + batch_size, len(pdf_files))
        progress = (processed / len(pdf_files)) * 100
        print(f"\n📈 Progress: {processed}/{len(pdf_files)} ({progress:.1f}%)")

        # Small delay between batches to avoid overwhelming the API
        if i + batch_size < len(pdf_files):
            print("⏳ Waiting 2 seconds before next batch...")
            await asyncio.sleep(2)

    # Final summary
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("🎉 BATCH PROCESSING COMPLETE!")
    print("=" * 60)
    print(f"📊 Total Results:")
    print(f"   ✅ Successful: {total_successful}")
    print(f"   ❌ Failed: {total_failed}")
    print(f"   📄 Total Files: {len(pdf_files)}")
    print(f"   ⏱️  Processing Time: {processing_time:.1f} seconds")
    print(f"   📁 Results saved to: {excel_output}")

    if total_failed > 0:
        print(
            f"\n⚠️  {total_failed} files failed processing. Check the logs above for details."
        )

    return total_successful, total_failed


if __name__ == "__main__":
    print("🚀 Batch PDF Processor")
    print("=" * 50)

    # Configuration
    BATCH_SIZE = 10  # Process 10 files concurrently
    PAPERS_DIR = "papers"  # Directory containing PDF files
    EXCEL_OUTPUT = "batch_papers_summary.xlsx"  # Output Excel file

    # Start batch processing
    asyncio.run(
        batch_process_papers(
            batch_size=BATCH_SIZE, papers_dir=PAPERS_DIR, excel_output=EXCEL_OUTPUT
        )
    )
