import json
from collections import Counter

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


def analyze_excel_data(excel_file="batch_papers_summary.xlsx"):
    """Analyze the Excel file to understand the data structure and content"""

    try:
        # Read the Excel file
        df = pd.read_excel(excel_file)

        print("🔍 EXCEL FILE ANALYSIS")
        print("=" * 50)

        # Basic info
        print(f"📊 Total Papers: {len(df)}")
        print(f"📋 Columns: {list(df.columns)}")
        print(f"🗂️  Data Shape: {df.shape}")

        print("\n📝 COLUMN ANALYSIS:")
        print("-" * 30)

        for col in df.columns:
            print(f"\n{col.upper()}:")
            print(f"  - Non-null values: {df[col].notna().sum()}")
            print(f"  - Null values: {df[col].isna().sum()}")

            if col == "categories":
                # Analyze categories
                all_categories = []
                for cats in df[col].dropna():
                    if isinstance(cats, str):
                        try:
                            cat_list = eval(cats) if cats.startswith("[") else [cats]
                            all_categories.extend(cat_list)
                        except:
                            all_categories.append(cats)

                category_counts = Counter(all_categories)
                print(f"  - Unique categories: {len(category_counts)}")
                print(f"  - Top 10 categories:")
                for cat, count in category_counts.most_common(10):
                    print(f"    • {cat}: {count}")

            elif col in ["title", "abstract", "method", "objectives", "summary"]:
                # Sample some text data
                sample_values = df[col].dropna().head(3).tolist()
                for i, sample in enumerate(sample_values):
                    sample_text = (
                        str(sample)[:100] + "..."
                        if len(str(sample)) > 100
                        else str(sample)
                    )
                    print(f"  - Sample {i+1}: {sample_text}")

        print("\n🎯 DATA QUALITY ASSESSMENT:")
        print("-" * 35)

        # Check for processing errors
        error_indicators = [
            "Error",
            "Failed",
            "processing failed",
            "JSON parsing failed",
        ]
        error_count = 0

        for indicator in error_indicators:
            error_count += (
                df["summary"].str.contains(indicator, case=False, na=False).sum()
            )

        print(f"📈 Successfully processed papers: {len(df) - error_count}")
        print(f"❌ Papers with errors: {error_count}")
        print(f"✅ Success rate: {((len(df) - error_count) / len(df)) * 100:.1f}%")

        # Save sample data for inspection
        sample_df = df.head(10)
        sample_df.to_csv("sample_papers.csv", index=False)
        print(f"\n💾 Sample data saved to: sample_papers.csv")

        return df

    except Exception as e:
        print(f"❌ Error analyzing Excel file: {e}")
        return None


if __name__ == "__main__":
    df = analyze_excel_data()
if __name__ == "__main__":
    df = analyze_excel_data()
