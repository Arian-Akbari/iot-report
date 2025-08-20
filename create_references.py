import json
import os
from datetime import datetime
from pathlib import Path

import pandas as pd


def create_references_file(papers_dir="papers", output_file="paper_references.txt"):
    """Create a properly formatted references file from all papers"""
    
    papers_path = Path(papers_dir)
    if not papers_path.exists():
        print(f"❌ Papers directory '{papers_dir}' not found!")
        return
    
    # Get all PDF files
    pdf_files = list(papers_path.glob("*.pdf"))
    
    if not pdf_files:
        print(f"❌ No PDF files found in '{papers_dir}'")
        return
    
    print(f"📁 Found {len(pdf_files)} PDF files")
    print(f"📝 Creating references file: {output_file}")
    
    # Check if we have processed data from batch_processor
    excel_file = "batch_papers_summary.xlsx"
    processed_data = {}
    
    if Path(excel_file).exists():
        try:
            df = pd.read_excel(excel_file)
            print(f"✅ Found processed data for {len(df)} papers")
            
            # Create a mapping from filename to processed data
            for _, row in df.iterrows():
                if pd.notna(row['filename']):
                    processed_data[row['filename']] = {
                        'title': row['title'] if pd.notna(row['title']) else '',
                        'abstract': row['abstract'] if pd.notna(row['abstract']) else '',
                        'method': row['method'] if pd.notna(row['method']) else '',
                        'objectives': row['objectives'] if pd.notna(row['objectives']) else '',
                        'summary': row['summary'] if pd.notna(row['summary']) else '',
                        'categories': row['categories'] if pd.notna(row['categories']) else '',
                        'processed_at': row['processed_at'] if pd.notna(row['processed_at']) else ''
                    }
        except Exception as e:
            print(f"⚠️  Could not read processed data: {e}")
    
    # Create references file
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("PAPER REFERENCES FOR FINAL DOCUMENT\n")
        f.write("=" * 80 + "\n\n")
        f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total papers: {len(pdf_files)}\n\n")
        
        # Write references in different formats
        f.write("REFERENCE FORMATS:\n")
        f.write("-" * 40 + "\n\n")
        
        for i, pdf_file in enumerate(pdf_files, 1):
            filename = pdf_file.name
            
            # Try to get processed data
            if filename in processed_data:
                data = processed_data[filename]
                title = data['title'] if data['title'] and data['title'] != filename else "Title not extracted"
                abstract = data['abstract'] if data['abstract'] and 'error' not in str(data['abstract']).lower() else "Abstract not available"
                method = data['method'] if data['method'] and 'error' not in str(data['method']).lower() else "Method not available"
                categories = data['categories'] if data['categories'] else "Categories not available"
            else:
                title = "Title not extracted"
                abstract = "Abstract not available"
                method = "Method not available"
                categories = "Categories not available"
            
            f.write(f"REFERENCE {i:03d}: {filename}\n")
            f.write("-" * 60 + "\n")
            
            # Format 1: Simple filename reference
            f.write(f"Filename: {filename}\n")
            
            # Format 2: Title reference (if available)
            if title and title != filename and 'error' not in str(title).lower():
                f.write(f"Title: {title}\n")
            
            # Format 3: Categories (if available)
            if categories and categories != "Categories not available":
                f.write(f"Categories: {categories}\n")
            
            # Format 4: Abstract snippet (if available)
            if abstract and abstract != "Abstract not available" and 'error' not in str(abstract).lower():
                # Truncate abstract to reasonable length
                abstract_text = str(abstract)
                if len(abstract_text) > 200:
                    abstract_text = abstract_text[:200] + "..."
                f.write(f"Abstract: {abstract_text}\n")
            
            # Format 5: Method snippet (if available)
            if method and method != "Method not available" and 'error' not in str(method).lower():
                method_text = str(method)
                if len(method_text) > 150:
                    method_text = method_text[:150] + "..."
                f.write(f"Method: {method_text}\n")
            
            f.write("\n")
        
        # Add usage instructions
        f.write("\n" + "=" * 80 + "\n")
        f.write("USAGE INSTRUCTIONS FOR FINAL DOCUMENT\n")
        f.write("=" * 80 + "\n\n")
        
        f.write("1. CITATION FORMATS:\n")
        f.write("   - In-text: (Author et al., Year) or [1], [2], etc.\n")
        f.write("   - Bibliography: Use the filename as reference identifier\n\n")
        
        f.write("2. REFERENCE LIST:\n")
        f.write("   - Copy relevant references to your bibliography section\n")
        f.write("   - Format according to your required citation style (APA, IEEE, etc.)\n\n")
        
        f.write("3. IN-TEXT CITATIONS:\n")
        f.write("   - Use [REF001], [REF002], etc. for numbered citations\n")
        f.write("   - Or use (Author et al., Year) format if you extract author names\n\n")
        
        f.write("4. BIBLIOGRAPHY ENTRY EXAMPLE:\n")
        f.write("   [REF001] Filename.pdf - Title if available\n")
        f.write("   [REF002] Filename.pdf - Title if available\n\n")
        
        f.write("5. FOR FULL PAPER ANALYSIS:\n")
        f.write("   - Use the categorized summaries in 'categorized_papers/' folder\n")
        f.write("   - Each category has combined summaries for comprehensive analysis\n")
        f.write("   - Use the AI prompts in 'report_prompts/' folder for report generation\n")
    
    print(f"✅ References file created: {output_file}")
    print(f"📊 Total references: {len(pdf_files)}")
    
    # Also create a simple numbered list
    numbered_file = "numbered_references.txt"
    with open(numbered_file, 'w', encoding='utf-8') as f:
        f.write("NUMBERED REFERENCES FOR CITATIONS\n")
        f.write("=" * 50 + "\n\n")
        
        for i, pdf_file in enumerate(pdf_files, 1):
            filename = pdf_file.name
            if filename in processed_data:
                title = processed_data[filename]['title']
                if title and title != filename and 'error' not in str(title).lower():
                    f.write(f"[{i:03d}] {title}\n")
                else:
                    f.write(f"[{i:03d}] {filename}\n")
            else:
                f.write(f"[{i:03d}] {filename}\n")
    
    print(f"✅ Numbered references file created: {numbered_file}")
    
    return len(pdf_files)

if __name__ == "__main__":
    print("📚 CREATING PAPER REFERENCES FOR FINAL DOCUMENT")
    print("=" * 60)
    
    count = create_references_file()
    
    if count > 0:
        print(f"\n🎉 Successfully created references for {count} papers!")
        print(f"📁 Files created:")
        print(f"   • paper_references.txt - Detailed references with abstracts")
        print(f"   • numbered_references.txt - Simple numbered list for citations")
        print(f"\n💡 Use these files to:")
        print(f"   • Add citations in your final document")
        print(f"   • Create a bibliography section")
        print(f"   • Reference specific papers in your report")
    else:
        print("❌ No papers found to process")
    else:
        print("❌ No papers found to process")
