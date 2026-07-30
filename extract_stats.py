import re
import os

html_file = r"D:\Desktop\13th August 2023\python-output\python-inputs\a-process-telegram-uploads\Spark-In-Action\spark_study_portal.html"
artifact_file = r"C:\Users\kingn\.gemini\antigravity\brain\f3e157f6-be89-4b1d-8ce8-7cfedce4b81d\concept_lengths.md"

with open(html_file, "r", encoding="utf-8") as f:
    content = f.read()

# Topics are enclosed in { id: ... } dictionaries in the CURRICULUM array.
# We can find all topics that have an overview:
topics = re.findall(r"\{\s*id\s*:\s*'[^']+',\s*title\s*:\s*'([^']+)'(.*?overview\s*:\s*'([^']+)')(.*?)(?=\s*\{(?:\s*id|\s*icon|\s*chapters)\s*:|\Z)", content, re.DOTALL)

results = []

for title, _, overview, rest_of_topic in topics:
    # Calculate counts
    char_count = len(overview)
    word_count = len(overview.split())
    
    # Check for examples by looking for lang:'...' or label:'...' in the rest of the topic string
    # For example: code:{lang:'bash',label:'Bash',...} or code1:{...}
    langs = re.findall(r"label\s*:\s*'([^']+)'", rest_of_topic)
    
    # Check if there are concepts/sub-items that are examples? Usually they are under code blocks.
    examples_provided = "None"
    if langs:
        examples_provided = "Yes (" + ", ".join(langs) + ")"
    elif re.search(r"\bcode\s*:", rest_of_topic) or re.search(r"codeSnippet\s*:", rest_of_topic):
        examples_provided = "Yes"
        
    results.append({
        'title': title,
        'chars': char_count,
        'words': word_count,
        'examples': examples_provided,
        'text': overview
    })

# Write to the artifact file
with open(artifact_file, "w", encoding="utf-8") as f:
    f.write("# Concept Explanation Lengths\n\n")
    f.write("This document lists the word and character counts for the `overview` (explanation) of each important concept in the Spark curriculum, along with the examples provided for each.\n\n")
    f.write("| Concept Title | Word Count | Character Count | Examples Provided |\n")
    f.write("| :--- | :--- | :--- | :--- |\n")
    
    total_words = 0
    total_chars = 0
    
    for r in results:
        f.write(f"| {r['title']} | {r['words']} | {r['chars']} | {r['examples']} |\n")
        total_words += r['words']
        total_chars += r['chars']
        
    f.write(f"| **TOTAL ({len(results)} concepts)** | **{total_words}** | **{total_chars}** | - |\n\n")
    
    f.write("## Detailed Descriptions\n\n")
    for r in results:
        f.write(f"### {r['title']}\n")
        f.write(f"- **Words:** {r['words']}\n")
        f.write(f"- **Characters:** {r['chars']}\n")
        f.write(f"- **Examples:** {r['examples']}\n\n")
        f.write(f"> {r['text']}\n\n")

print(f"Artifact updated successfully with {len(results)} concepts.")
