import json
import glob
import re
import os

OUTPUT_FILE = "fundamentals_data.js"
BRAIN_DIR = r"C:\Users\kingn\.gemini\antigravity\brain"

subagent_ids = [
    "0f88da17-fe9c-4728-9ed4-db0f1e66ed84",
    "93c511f7-cdf5-40ad-a099-45e84a6fb4df",
    "50c5af3f-1e05-4694-996d-6809ceaa3ffa",
    "54169f61-a1dc-4a5c-91ef-2da091410458",
    "9dd5a8a0-47f8-42ec-8cc0-0f0538565f60"
]

def main():
    if not os.path.exists(OUTPUT_FILE):
        print(f"Error: {OUTPUT_FILE} not found.")
        return

    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        content = f.read()
    
    match = re.search(r"const\s+FUNDAMENTALS_DATA\s*=\s*\{", content)
    if not match:
        print("Could not find FUNDAMENTALS_DATA object.")
        return
    
    start_idx = match.end()
    
    new_entries = []
    
    for agent_id in subagent_ids:
        scratch_dir = os.path.join(BRAIN_DIR, agent_id, "scratch")
        for filepath in glob.glob(os.path.join(scratch_dir, "prereq_output_*.json")):
            with open(filepath, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except:
                    continue
                
            if "topic_key" not in data:
                continue
                
            key = data["topic_key"]
            markdown = data["content"]
            
            markdown = markdown.replace('\\', '\\\\').replace('`', '\\`').replace('$', '\\$')
            
            entry = f'  "{key}": `\n{markdown}\n`'
            new_entries.append(entry)
            print(f"Found {key}")
            
    if not new_entries:
        print("No prerequisite files found.")
        return
        
    injections = ",\n".join(new_entries) + ",\n"
    
    new_content = content[:start_idx] + "\n" + injections + content[start_idx:]
    
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        f.write(new_content)
        
    print(f"Successfully injected {len(new_entries)} prerequisite topics into {OUTPUT_FILE}.")

if __name__ == "__main__":
    main()
