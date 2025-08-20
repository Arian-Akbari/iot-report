import json
import re
from collections import Counter

import pandas as pd


def analyze_categories(excel_file="batch_papers_summary.xlsx"):
    """Analyze all categories and suggest grouping strategy"""

    try:
        df = pd.read_excel(excel_file)

        print("🔍 COMPREHENSIVE CATEGORY ANALYSIS")
        print("=" * 60)

        # Extract all categories
        all_categories = []
        for cats in df["categories"].dropna():
            if isinstance(cats, str):
                try:
                    # Handle different formats
                    if cats.startswith("["):
                        cat_list = eval(cats)
                    else:
                        cat_list = [cats]
                    all_categories.extend(cat_list)
                except:
                    all_categories.append(cats)

        # Clean and normalize categories
        cleaned_categories = []
        for cat in all_categories:
            # Remove extra spaces, normalize case
            cleaned_cat = str(cat).strip().lower()
            cleaned_categories.append(cleaned_cat)

        category_counts = Counter(cleaned_categories)
        total_categories = len(category_counts)

        print(f"📊 Total unique categories: {total_categories}")
        print(f"📈 Total category mentions: {len(all_categories)}")
        print(f"📋 Average categories per paper: {len(all_categories)/len(df):.1f}")

        print(f"\n🏆 TOP 30 CATEGORIES:")
        print("-" * 40)
        for i, (cat, count) in enumerate(category_counts.most_common(30), 1):
            percentage = (count / len(all_categories)) * 100
            print(f"{i:2d}. {cat:<35} | {count:3d} ({percentage:4.1f}%)")

        # Analyze category patterns for grouping
        print(f"\n🎯 CATEGORY PATTERN ANALYSIS:")
        print("-" * 40)

        # Define main research areas based on keywords
        research_areas = {
            "Scheduling & Optimization": [
                "scheduling",
                "job shop",
                "flexible",
                "makespan",
                "optimization",
                "multi-objective",
                "flowshop",
                "batch",
                "production",
                "workflow",
            ],
            "Algorithms & Methods": [
                "algorithm",
                "genetic",
                "metaheuristic",
                "ant colony",
                "particle swarm",
                "tabu search",
                "simulated annealing",
                "heuristic",
                "evolutionary",
                "neural network",
                "machine learning",
                "artificial intelligence",
            ],
            "Manufacturing & Industry": [
                "manufacturing",
                "industry",
                "production",
                "factory",
                "automation",
                "industrial",
                "supply chain",
                "logistics",
                "resource allocation",
            ],
            "Deep Learning & AI": [
                "deep learning",
                "reinforcement learning",
                "neural",
                "ai",
                "artificial intelligence",
                "machine learning",
                "deep reinforcement",
                "convolutional",
                "lstm",
            ],
            "Mathematical Models": [
                "mathematical",
                "model",
                "programming",
                "linear",
                "integer",
                "constraint",
                "mathematical programming",
                "milp",
                "optimization model",
            ],
            "Performance & Evaluation": [
                "performance",
                "evaluation",
                "benchmark",
                "comparison",
                "analysis",
                "efficiency",
                "effectiveness",
                "quality",
                "metrics",
            ],
        }

        # Categorize each category into research areas
        area_counts = {area: 0 for area in research_areas}
        categorized_items = {area: [] for area in research_areas}
        uncategorized = []

        for cat, count in category_counts.items():
            categorized = False
            for area, keywords in research_areas.items():
                if any(keyword in cat for keyword in keywords):
                    area_counts[area] += count
                    categorized_items[area].append((cat, count))
                    categorized = True
                    break

            if not categorized:
                uncategorized.append((cat, count))

        print("\n📊 SUGGESTED RESEARCH AREA GROUPINGS:")
        print("=" * 50)

        sorted_areas = sorted(area_counts.items(), key=lambda x: x[1], reverse=True)

        for i, (area, total_count) in enumerate(sorted_areas, 1):
            percentage = (total_count / len(all_categories)) * 100
            print(f"\n{i}. {area.upper()}")
            print(f"   Total mentions: {total_count} ({percentage:.1f}%)")
            print(f"   Top categories:")

            # Show top categories in this area
            sorted_cats = sorted(
                categorized_items[area], key=lambda x: x[1], reverse=True
            )
            for cat, count in sorted_cats[:8]:  # Top 8 in each area
                cat_percentage = (count / len(all_categories)) * 100
                print(f"   • {cat:<30} | {count:3d} ({cat_percentage:4.1f}%)")

        # Handle uncategorized items
        if uncategorized:
            uncategorized_count = sum(count for _, count in uncategorized)
            percentage = (uncategorized_count / len(all_categories)) * 100
            print(f"\n7. UNCATEGORIZED/OTHER")
            print(f"   Total mentions: {uncategorized_count} ({percentage:.1f}%)")
            print(f"   Top categories:")

            sorted_uncat = sorted(uncategorized, key=lambda x: x[1], reverse=True)
            for cat, count in sorted_uncat[:10]:
                cat_percentage = (count / len(all_categories)) * 100
                print(f"   • {cat:<30} | {count:3d} ({cat_percentage:4.1f}%)")

        # Recommendation for report structure
        print(f"\n🎯 RECOMMENDATION FOR 40-PAGE REPORT:")
        print("=" * 50)

        significant_areas = [area for area, count in sorted_areas if count >= 20]

        if len(significant_areas) <= 6:
            recommended_structure = significant_areas
            if uncategorized and sum(count for _, count in uncategorized) >= 15:
                recommended_structure.append("Emerging Technologies & Others")
        else:
            # Take top 5-6 areas
            recommended_structure = [area for area, _ in sorted_areas[:6]]

        print(f"📝 Suggested {len(recommended_structure)} main sections:")
        pages_per_section = 35 // len(
            recommended_structure
        )  # Leave 5 pages for intro/conclusion

        for i, area in enumerate(recommended_structure, 1):
            area_count = area_counts.get(
                area,
                (
                    sum(count for _, count in uncategorized)
                    if area == "Emerging Technologies & Others"
                    else 0
                ),
            )
            area_percentage = (area_count / len(all_categories)) * 100
            print(f"{i}. {area}")
            print(f"   - Papers: ~{area_count} mentions ({area_percentage:.1f}%)")
            print(f"   - Suggested pages: {pages_per_section}-{pages_per_section+2}")

        print(f"\n📋 Report Structure Recommendation:")
        print(f"   • Introduction: 2-3 pages")
        print(
            f"   • Main sections: {len(recommended_structure)} × {pages_per_section} pages each"
        )
        print(f"   • Conclusion & Future Work: 2-3 pages")
        print(f"   • Total: ~40 pages")

        return {
            "total_categories": total_categories,
            "category_counts": category_counts,
            "research_areas": area_counts,
            "recommended_structure": recommended_structure,
            "categorized_items": categorized_items,
        }

    except Exception as e:
        print(f"❌ Error analyzing categories: {e}")
        return None


if __name__ == "__main__":
    result = analyze_categories()
if __name__ == "__main__":
    result = analyze_categories()
