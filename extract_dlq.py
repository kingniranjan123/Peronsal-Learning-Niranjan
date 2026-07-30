import re

with open('spark_study_portal.html', encoding='utf-8') as f:
    data = f.read()

block = data[data.find('const DLQ_MAP = {'):data.find('};', data.find('const DLQ_MAP = {'))]
keys = re.findall(r"'([^']+)'\s*:", block)
# print keys except rdds (which is already done)
keys = [k for k in keys if k != 'rdds']
print("Total keys:", len(keys))
print(keys[:10])
