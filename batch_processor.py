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
    """Process a batch of PDF files concurrently"""
    print(f"\n🔄 Processing Batch {batch_num} ({len(pdf_files)} files)")
    print("=" * 60)

    # Create tasks for concurrent processing
    tasks = []
    for pdf_file in pdf_files:
        task = process_single_pdf_async(pdf_file, excel_output)
        tasks.append(task)

    # Execute all tasks concurrently
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Process results
    successful = 0
    failed = 0

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"   ❌ {pdf_files[i].name}: {result}")
            failed += 1
        elif result:
            print(f"   ✅ {pdf_files[i].name}: Success")
            successful += 1
        else:
            print(f"   ⚠️  {pdf_files[i].name}: Failed")
            failed += 1

    print(f"\n📊 Batch {batch_num} Results: {successful} successful, {failed} failed")
    return successful, failed


async def process_single_pdf_async(pdf_file, excel_output):
    """Async wrapper for processing a single PDF"""
    try:
        # Extract text
        text = await extract_text_from_pdf(pdf_file)

        if not text.strip():
            print(f"   ⚠️  No text extracted from {pdf_file.name}")
            return False

        # Process with OpenAI
        info_json = await extract_paper_info(text, pdf_file.name)

        if info_json is None:
            print(f"   ❌ OpenAI processing failed for {pdf_file.name}")
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
                print(f"   ❌ JSON parsing error for {pdf_file.name}: {e}")
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
        return True

    except Exception as e:
        print(f"   ❌ Error processing {pdf_file.name}: {e}")
        return False


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

    # Start batch processing
    asyncio.run(
        batch_process_papers(
            batch_size=BATCH_SIZE, papers_dir=PAPERS_DIR, excel_output=EXCEL_OUTPUT
        )
    )
