import asyncio
from pathlib import Path

from pdf_processor import process_single_pdf, save_to_excel


async def process_pdf_batch(pdf_files, batch_num, excel_output):
    """Process a batch of PDF files concurrently with clean error handling"""
    print(f"\n🔄 Batch {batch_num}: Processing {len(pdf_files)} files concurrently")

    # Process all PDFs in the batch concurrently
    tasks = [process_single_pdf(pdf_file) for pdf_file in pdf_files]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful = 0
    failed = 0

    # Save results as they come in
    for i, result in enumerate(results):
        pdf_file = pdf_files[i]

        if isinstance(result, Exception):
            print(f"   ❌ {pdf_file.name}: Unexpected error - {result}")
            # Clean empty record for unexpected errors
            record = {
                "filename": pdf_file.name,
                "title": "",
                "abstract": "",
                "method": "",
                "objectives": "",
                "categories": [],
                "summary": "",
            }
            failed += 1
        else:
            record, error = result
            if error:
                print(f"   ❌ {pdf_file.name}: {error}")
                failed += 1
            else:
                print(f"   ✅ {pdf_file.name}: Success")
                successful += 1

        # Save to Excel immediately
        try:
            await save_to_excel(record, excel_output)
        except Exception as e:
            print(f"   ⚠️  Failed to save {pdf_file.name} to Excel: {e}")

    print(f"   📊 Batch {batch_num} Complete: {successful} success, {failed} failed")
    return successful, failed


async def batch_process_papers(
    batch_size=10, papers_dir="papers", excel_output="batch_papers_summary.xlsx"
):
    """Main function to process papers in batches"""

    papers_path = Path(papers_dir)
    if not papers_path.exists():
        print(f"❌ Papers directory '{papers_dir}' not found!")
        return

    pdf_files = list(papers_path.glob("*.pdf"))
    if not pdf_files:
        print(f"❌ No PDF files found in '{papers_dir}'")
        return

    print(f"🚀 Starting Batch Processing")
    print(f"📁 Found {len(pdf_files)} PDF files")
    print(f"📦 Batch size: {batch_size}")
    print(f"💾 Output: {excel_output}")
    print("=" * 50)

    total_successful = 0
    total_failed = 0

    # Process in batches
    for i in range(0, len(pdf_files), batch_size):
        batch = pdf_files[i : i + batch_size]
        batch_num = (i // batch_size) + 1

        # Process batch
        successful, failed = await process_pdf_batch(
            batch, batch_num, Path(excel_output)
        )
        total_successful += successful
        total_failed += failed

        # Progress update
        processed = min(i + batch_size, len(pdf_files))
        progress = (processed / len(pdf_files)) * 100
        print(f"📈 Progress: {processed}/{len(pdf_files)} ({progress:.1f}%)")

        # Delay between batches
        if i + batch_size < len(pdf_files):
            await asyncio.sleep(2)

    # Final summary
    print("\n" + "=" * 50)
    print("🎉 PROCESSING COMPLETE!")
    print(f"✅ Successful: {total_successful}")
    print(f"❌ Failed: {total_failed}")
    print(f"📄 Total: {len(pdf_files)}")
    print(f"📁 Results: {excel_output}")

    return total_successful, total_failed


if __name__ == "__main__":
    # Configuration
    BATCH_SIZE = 10
    PAPERS_DIR = "papers"
    EXCEL_OUTPUT = "batch_papers_summary.xlsx"

    # Start processing
    asyncio.run(batch_process_papers(BATCH_SIZE, PAPERS_DIR, EXCEL_OUTPUT))
