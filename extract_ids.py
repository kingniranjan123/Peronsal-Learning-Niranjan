import re

with open('spark_study_portal.html', encoding='utf-8') as f:
    data = f.read()

# Finding all objects that have an id and a title, then filtering them based on having an overview.
topics = re.findall(r"\{\s*id\s*:\s*'([^']+)',\s*title\s*:\s*'([^']+)'(.*?overview\s*:\s*'([^']+)')(.*?)(?=\s*\{(?:\s*id|\s*icon|\s*chapters)\s*:|\Z)", data, re.DOTALL)

for t in topics[:15]:
    print(t[0], "|||", t[1])
