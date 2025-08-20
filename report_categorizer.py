import json
import os
from pathlib import Path

import pandas as pd


def categorize_papers_for_report(excel_file="batch_papers_summary.xlsx"):
    """Categorize papers into main research areas and prepare for report generation"""

    df = pd.read_excel(excel_file)

    # Define the 4 main categories based on analysis
    categories = {
        "1_Scheduling_Optimization": {
            "name": "Scheduling & Optimization Systems",
            "keywords": [
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
                "resource allocation",
                "combinatorial",
            ],
            "papers": [],
            "description": "Research focused on scheduling algorithms, job shop optimization, and production planning systems",
        },
        "2_Algorithms_Methods": {
            "name": "Metaheuristic Algorithms & Computational Methods",
            "keywords": [
                "algorithm",
                "genetic",
                "metaheuristic",
                "ant colony",
                "particle swarm",
                "tabu search",
                "simulated annealing",
                "heuristic",
                "evolutionary",
                "differential evolution",
                "local search",
                "variable neighborhood",
            ],
            "papers": [],
            "description": "Studies on metaheuristic algorithms, optimization techniques, and computational methodologies",
        },
        "3_AI_MachineLearning": {
            "name": "Artificial Intelligence & Machine Learning",
            "keywords": [
                "deep learning",
                "reinforcement learning",
                "neural",
                "ai",
                "artificial intelligence",
                "machine learning",
                "deep reinforcement",
                "convolutional",
                "lstm",
                "neural networks",
            ],
            "papers": [],
            "description": "Research incorporating AI, machine learning, and deep learning approaches",
        },
        "4_Industry_Applications": {
            "name": "Industry Applications & Emerging Technologies",
            "keywords": [
                "manufacturing",
                "industry",
                "automation",
                "supply chain",
                "logistics",
                "sustainability",
                "energy",
                "maintenance",
                "quality",
                "performance",
                "benchmark",
                "evaluation",
                "mathematical programming",
                "constraint",
            ],
            "papers": [],
            "description": "Industrial applications, performance evaluation, and emerging technological approaches",
        },
    }

    print("🔍 CATEGORIZING PAPERS FOR 40-PAGE REPORT")
    print("=" * 60)

    # Categorize each paper
    uncategorized_papers = []

    for idx, row in df.iterrows():
        # Skip error papers
        if (
            pd.isna(row["summary"])
            or "error" in str(row["summary"]).lower()
            or "failed" in str(row["summary"]).lower()
        ):
            continue

        paper_categories = []
        if pd.notna(row["categories"]):
            try:
                if str(row["categories"]).startswith("["):
                    paper_categories = eval(row["categories"])
                else:
                    paper_categories = [row["categories"]]
            except:
                paper_categories = [str(row["categories"])]

        # Convert to lowercase for matching
        paper_cats_lower = [str(cat).lower() for cat in paper_categories]

        # Find best matching category
        best_match = None
        max_matches = 0

        for cat_key, cat_info in categories.items():
            matches = 0
            for keyword in cat_info["keywords"]:
                for paper_cat in paper_cats_lower:
                    if keyword in paper_cat:
                        matches += 1
                        break

            if matches > max_matches:
                max_matches = matches
                best_match = cat_key

        # If no good match found, try title/abstract
        if max_matches == 0:
            text_to_search = f"{str(row['title'])} {str(row['abstract'])} {str(row['method'])}".lower()

            for cat_key, cat_info in categories.items():
                matches = 0
                for keyword in cat_info["keywords"]:
                    if keyword in text_to_search:
                        matches += 1

                if matches > max_matches:
                    max_matches = matches
                    best_match = cat_key

        # Assign to category
        if best_match and max_matches > 0:
            categories[best_match]["papers"].append(
                {
                    "filename": row["filename"],
                    "title": row["title"],
                    "abstract": row["abstract"],
                    "method": row["method"],
                    "objectives": row["objectives"],
                    "summary": row["summary"],
                    "categories": paper_categories,
                    "processed_at": row["processed_at"],
                }
            )
        else:
            # Default to Industry Applications if no clear match
            categories["4_Industry_Applications"]["papers"].append(
                {
                    "filename": row["filename"],
                    "title": row["title"],
                    "abstract": row["abstract"],
                    "method": row["method"],
                    "objectives": row["objectives"],
                    "summary": row["summary"],
                    "categories": paper_categories,
                    "processed_at": row["processed_at"],
                }
            )

    # Print categorization results
    print(f"📊 CATEGORIZATION RESULTS:")
    print("-" * 40)
    total_papers = 0

    for cat_key, cat_info in categories.items():
        count = len(cat_info["papers"])
        total_papers += count
        percentage = (count / len(df)) * 100
        print(f"{cat_info['name']:<40} | {count:3d} papers ({percentage:4.1f}%)")

    print(f"\nTotal categorized papers: {total_papers}")

    # Save categorized papers to separate files
    output_dir = Path("categorized_papers")
    output_dir.mkdir(exist_ok=True)

    print(f"\n💾 SAVING CATEGORIZED PAPERS:")
    print("-" * 40)

    category_summaries = {}

    for cat_key, cat_info in categories.items():
        if cat_info["papers"]:
            # Save to JSON for processing
            json_file = output_dir / f"{cat_key}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "category_name": cat_info["name"],
                        "description": cat_info["description"],
                        "paper_count": len(cat_info["papers"]),
                        "papers": cat_info["papers"],
                    },
                    f,
                    indent=2,
                    ensure_ascii=False,
                )

            # Save to Excel for easy viewing
            excel_file = output_dir / f"{cat_key}.xlsx"
            cat_df = pd.DataFrame(cat_info["papers"])
            cat_df.to_excel(excel_file, index=False)

            # Extract all summaries for this category
            summaries = [
                paper["summary"] for paper in cat_info["papers"] if paper["summary"]
            ]
            combined_summary = "\n\n---PAPER SEPARATOR---\n\n".join(summaries)

            # Save combined summaries
            summary_file = output_dir / f"{cat_key}_summaries.txt"
            with open(summary_file, "w", encoding="utf-8") as f:
                f.write(f"CATEGORY: {cat_info['name']}\n")
                f.write(f"DESCRIPTION: {cat_info['description']}\n")
                f.write(f"TOTAL PAPERS: {len(cat_info['papers'])}\n")
                f.write("=" * 80 + "\n\n")
                f.write(combined_summary)

            category_summaries[cat_key] = {
                "name": cat_info["name"],
                "description": cat_info["description"],
                "paper_count": len(cat_info["papers"]),
                "summary_file": str(summary_file),
            }

            print(f"✅ {cat_info['name']}: {len(cat_info['papers'])} papers")
            print(
                f"   📁 Files: {json_file.name}, {excel_file.name}, {summary_file.name}"
            )

    # Generate base prompt for report writing
    generate_report_prompts(category_summaries)

    return categories


def generate_report_prompts(category_summaries):
    """Generate AI prompts for report writing"""

    prompts_dir = Path("report_prompts")
    prompts_dir.mkdir(exist_ok=True)

    print(f"\n🎯 GENERATING AI PROMPTS FOR REPORT WRITING:")
    print("-" * 50)

    # Base context prompt
    base_prompt = f"""
# RESEARCH PAPER ANALYSIS REPORT - BASE CONTEXT

You are tasked with writing a comprehensive 40-page research report analyzing {sum(cat['paper_count'] for cat in category_summaries.values())} scientific papers in the field of job shop scheduling, optimization, and related technologies.

## REPORT STRUCTURE OVERVIEW:

**Target Length:** 40 pages total
**Number of Main Sections:** 4

### Section Breakdown:
"""

    for i, (cat_key, cat_info) in enumerate(category_summaries.items(), 1):
        base_prompt += f"""
{i}. **{cat_info['name']}** ({cat_info['paper_count']} papers)
   - Target: 8-9 pages
   - Focus: {cat_info['description']}
"""

    base_prompt += """

## WRITING GUIDELINES:

1. **Academic Tone:** Formal, scholarly writing appropriate for a research publication
2. **Structure:** Clear sections with proper headings and subheadings
3. **Content Depth:** Balance between breadth and depth - cover main themes comprehensively
4. **Citations:** Reference specific papers when discussing particular approaches or findings
5. **Analysis:** Don't just summarize - analyze trends, compare approaches, identify gaps
6. **Visual Elements:** Suggest locations for figures, tables, and diagrams
7. **Page Distribution:** Aim for roughly 8-9 pages per main section

## ANALYSIS APPROACH:

- **Systematic Review:** Identify common themes, methodologies, and approaches
- **Comparative Analysis:** Compare different algorithms, techniques, and results
- **Trend Analysis:** Identify emerging trends and future directions
- **Gap Analysis:** Highlight areas needing further research
- **Practical Applications:** Discuss real-world implementations and case studies

---

**Instructions for each section:**
You will receive the summaries of all papers in each category. Analyze these summaries to write a comprehensive section that synthesizes the research, identifies key contributions, compares approaches, and provides insights into the current state and future directions of the field.
"""

    # Save base prompt
    base_file = prompts_dir / "00_base_context.txt"
    with open(base_file, "w", encoding="utf-8") as f:
        f.write(base_prompt)

    print(f"✅ Base context prompt: {base_file.name}")

    # Generate section-specific prompts
    for i, (cat_key, cat_info) in enumerate(category_summaries.items(), 1):
        section_prompt = f"""
# SECTION {i}: {cat_info['name'].upper()}

## TASK:
Write a comprehensive 8-9 page section analyzing {cat_info['paper_count']} research papers in the area of {cat_info['name'].lower()}.

## SECTION STRUCTURE:
1. **Introduction** (1 page)
   - Overview of the research area
   - Importance in the field
   - Scope of papers analyzed

2. **Literature Review & Analysis** (4-5 pages)
   - Key methodologies and approaches
   - Comparative analysis of different techniques
   - Major contributions and innovations
   - Performance comparisons where available

3. **Current Trends & Emerging Patterns** (1-2 pages)
   - Recent developments
   - Popular algorithms/methods
   - Integration with modern technologies

4. **Challenges & Future Directions** (1-2 pages)
   - Current limitations
   - Open research questions
   - Suggested future work
   - Potential improvements

## SPECIFIC FOCUS AREAS:
{cat_info['description']}

## ANALYSIS GUIDELINES:
- Synthesize rather than just summarize
- Identify patterns across multiple papers
- Compare and contrast different approaches
- Highlight significant findings and contributions
- Discuss practical implications
- Suggest areas for future research

## INPUT DATA:
You will receive the summaries of all {cat_info['paper_count']} papers in this category. Use these summaries to extract key information, identify themes, and build your comprehensive analysis.

---

**Remember:** This is Section {i} of a 4-section report. Ensure it flows well with the overall report structure while being self-contained and comprehensive.
"""

        section_file = (
            prompts_dir / f"{cat_key.split('_')[0]}_{cat_key.split('_')[1]}_prompt.txt"
        )
        with open(section_file, "w", encoding="utf-8") as f:
            f.write(section_prompt)

        print(f"✅ Section {i} prompt: {section_file.name}")

    # Generate final instructions
    final_instructions = """
# FINAL REPORT ASSEMBLY INSTRUCTIONS

## USAGE WORKFLOW:

1. **Start with Base Context:** Read the base context prompt to understand the overall report structure and guidelines.

2. **Process Each Section:** For each of the 4 main sections:
   - Read the section-specific prompt
   - Load the corresponding summaries file
   - Feed both the prompt and summaries to your AI assistant
   - Generate the 8-9 page section

3. **Add Introduction & Conclusion:**
   - Write a 2-3 page introduction covering the scope, methodology, and overview
   - Write a 2-3 page conclusion summarizing key findings and future directions

4. **Final Assembly:**
   - Combine all sections in order
   - Ensure consistent formatting and flow
   - Add table of contents
   - Review for coherence and completeness

## FILES STRUCTURE:
- `00_base_context.txt` - Overall context and guidelines
- `1_Scheduling_prompt.txt` - Prompt for Scheduling & Optimization section
- `2_Algorithms_prompt.txt` - Prompt for Algorithms & Methods section  
- `3_AI_prompt.txt` - Prompt for AI & Machine Learning section
- `4_Industry_prompt.txt` - Prompt for Industry Applications section
- `categorized_papers/` - Directory with all categorized papers and summaries

## ESTIMATED OUTPUT:
- Introduction: 2-3 pages
- Section 1: 8-9 pages
- Section 2: 8-9 pages  
- Section 3: 8-9 pages
- Section 4: 8-9 pages
- Conclusion: 2-3 pages
- **Total: ~40 pages**
"""

    final_file = prompts_dir / "99_final_instructions.txt"
    with open(final_file, "w", encoding="utf-8") as f:
        f.write(final_instructions)

    print(f"✅ Final instructions: {final_file.name}")
    print(f"\n🎉 All prompts generated in: {prompts_dir}/")


if __name__ == "__main__":
    categories = categorize_papers_for_report()
if __name__ == "__main__":
    categories = categorize_papers_for_report()
