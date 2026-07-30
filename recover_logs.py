import os, json, re

brain_dir = r'C:\Users\kingn\.gemini\antigravity\brain'
target_dir = r'D:\Desktop\13th August 2023\python-output\python-inputs\a-process-telegram-uploads\Spark-In-Action\Curriculum_DeepDives'

recovered = 0

for d in os.listdir(brain_dir):
    log_file = os.path.join(brain_dir, d, '.system_generated', 'logs', 'transcript_full.jsonl')
    if not os.path.exists(log_file): 
        continue
        
    with open(log_file, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            if 'write_to_file' in line or 'replace_file_content' in line:
                try:
                    data = json.loads(line)
                    for tc in data.get('tool_calls', []):
                        name = tc.get('name', '')
                        if name == 'write_to_file' or name == 'replace_file_content':
                            args = tc.get('args', {})
                            target = args.get('TargetFile', '')
                            if 'Curriculum_DeepDives' in target:
                                content = args.get('CodeContent') or args.get('ReplacementContent')
                                if content and len(content) > 5000:
                                    fname = os.path.basename(target)
                                    out_path = os.path.join(target_dir, fname)
                                    if os.path.exists(out_path) and os.path.getsize(out_path) < 1000:
                                        if content.startswith('<🔥 Master Class:'):
                                            content = '# ' + content[1:content.find('>')] + content[content.find('>')+1:]
                                        with open(out_path, 'w', encoding='utf-8') as out_f:
                                            out_f.write(content)
                                        recovered += 1
                except Exception as e:
                    pass

print(f'Recovered {recovered} files!')
