import re
import os

with open('spark_study_portal.html', encoding='utf-8') as f:
    data = f.read()

block = data[data.find('const DLQ_MAP = {'):data.find('};', data.find('const DLQ_MAP = {'))]
keys = re.findall(r"'([^']+)'\s*:", block)

# get done from file system
done = [f.replace('.md', '') for f in os.listdir('Curriculum_DeepDives') if f.endswith('.md')]
remaining = [k for k in keys if k not in done]

seen = set()
unique_remaining = []
for r in remaining:
    if r not in seen:
        unique_remaining.append(r)
        seen.add(r)
print("Remaining:", len(unique_remaining))
print(unique_remaining)
