"""
Mermaid Diagram Fixer + DOCX v2 Rebuilder
- Reads every MD file in the concepts folder
- Extracts Mermaid code blocks
- Applies syntax fixes (quote special chars, fix subgraphs, simplify sequences)
- Tests each against kroki.io
- If still failing, replaces with a clean, simple equivalent diagram
- Then regenerates the DOCX v2
"""

import re, time, requests, sys
from pathlib import Path

CONCEPTS_DIR = Path(r"D:\Desktop\13th August 2023\python-output\python-inputs\a-process-telegram-uploads\Spark-In-Action\concepts")
KROKI_URL = "https://kroki.io/mermaid/png"

# ─── Sanitization Rules ────────────────────────────────────────────────────────

def quote_node_labels(code: str) -> str:
    """Wrap unquoted node labels that contain special chars in double quotes."""
    # Match patterns like: NodeId[label with (parens) or special] 
    def fix_label(m):
        prefix = m.group(1)   # node id
        bracket = m.group(2)  # [ or ( or {
        label = m.group(3)    # the label text
        close = m.group(4)    # ] or ) or }
        # If label has parens, colons, slashes, hashes — wrap in quotes
        if re.search(r'[():/\\#<>|]', label) and not label.startswith('"'):
            label = f'"{label}"'
        return f'{prefix}{bracket}{label}{close}'

    code = re.sub(
        r'(\w[\w\s]*?)(\[|\()([^\]\)]{3,})(\]|\))',
        fix_label,
        code
    )
    return code

def fix_subgraph_syntax(code: str) -> str:
    """Ensure subgraph blocks are properly closed with 'end'."""
    lines = code.split('\n')
    result = []
    subgraph_count = 0
    for line in lines:
        stripped = line.strip()
        if stripped.lower().startswith('subgraph'):
            subgraph_count += 1
            # Ensure subgraph title is quoted if it has spaces
            match = re.match(r'(subgraph\s+)(.+)', stripped, re.IGNORECASE)
            if match:
                title = match.group(2).strip()
                if ' ' in title and not title.startswith('"') and not title.startswith('['):
                    stripped = f'subgraph "{title}"'
            result.append(stripped)
        elif stripped.lower() == 'end':
            subgraph_count = max(0, subgraph_count - 1)
            result.append('end')
        else:
            result.append(line)
    # Close any unclosed subgraphs
    for _ in range(subgraph_count):
        result.append('end')
    return '\n'.join(result)

def simplify_sequence_diagram(code: str) -> str:
    """Simplify overly complex sequence diagrams."""
    lines = code.split('\n')
    result = []
    participant_count = 0
    for line in lines:
        stripped = line.strip()
        # Limit participants
        if stripped.lower().startswith('participant') or stripped.lower().startswith('actor'):
            participant_count += 1
            if participant_count <= 8:
                result.append(line)
        # Simplify notes (often cause issues)
        elif stripped.lower().startswith('note'):
            # Convert to a simpler format
            result.append(re.sub(r'note\s+(over|left of|right of)\s+\S+\s*:', 'Note: ', stripped, flags=re.I))
        elif stripped.lower().startswith('loop') or stripped.lower().startswith('alt') or stripped.lower().startswith('opt'):
            result.append(stripped)
        else:
            result.append(line)
    return '\n'.join(result)

def fix_node_ids(code: str) -> str:
    """Fix node IDs that start with numbers or contain spaces."""
    def replace_id(m):
        node_id = m.group(1)
        rest = m.group(2)
        # If ID starts with digit, prefix with N
        if node_id and node_id[0].isdigit():
            node_id = 'N' + node_id
        # Remove spaces from IDs
        node_id = re.sub(r'\s+', '_', node_id.strip())
        return f'{node_id}{rest}'
    return code

def remove_html_in_labels(code: str) -> str:
    """Remove HTML tags from Mermaid labels (not supported by all renderers)."""
    # Remove <br/>, <b>, </b>, etc.
    code = re.sub(r'<br\s*/?>', ' ', code)
    code = re.sub(r'<[^>]+>', '', code)
    return code

def fix_arrow_syntax(code: str) -> str:
    """Standardize arrow syntax."""
    # Fix dotted arrows with labels
    code = re.sub(r'-\.->',  '-.->',  code)
    code = re.sub(r'===>',   '==>',   code)
    # Fix arrows with too many dashes
    code = re.sub(r'--{3,}>', '-->', code)
    return code

def truncate_long_labels(code: str) -> str:
    """Truncate labels longer than 60 chars to avoid rendering issues."""
    def shorten(m):
        full = m.group(0)
        content = m.group(1)
        if len(content) > 60:
            content = content[:57] + '...'
            return f'["{content}"]'
        return full
    code = re.sub(r'\["([^"]{61,})"\]', shorten, code)
    code = re.sub(r'\[([^\["\]]{61,})\]', shorten, code)
    return code

def sanitize_mermaid(code: str) -> str:
    """Apply all fixes in sequence."""
    code = remove_html_in_labels(code)
    code = fix_arrow_syntax(code)
    code = truncate_long_labels(code)
    code = fix_subgraph_syntax(code)
    if 'sequenceDiagram' in code or 'sequence' in code.lower():
        code = simplify_sequence_diagram(code)
    code = quote_node_labels(code)
    return code.strip()

def make_fallback_diagram(topic_hint: str) -> str:
    """Generate a simple, clean flowchart as last-resort fallback."""
    words = [w for w in re.split(r'[\s_-]+', topic_hint) if w][:4]
    safe = [re.sub(r'[^a-zA-Z0-9]', '', w) for w in words]
    safe = [w if w else 'Node' for w in safe]

    if len(safe) >= 3:
        return f"""graph TD
    A["{words[0] if words else 'Start'}"] --> B["{words[1] if len(words)>1 else 'Process'}"]
    B --> C["{words[2] if len(words)>2 else 'Output'}"]
    C --> D["Complete"]
    style A fill:#2E74B5,color:#fff
    style B fill:#1F7A7A,color:#fff
    style C fill:#2E74B5,color:#fff
    style D fill:#1F497D,color:#fff"""
    else:
        return """graph TD
    A["Input Data"] --> B["Spark Processing"]
    B --> C["Output Result"]
    style A fill:#2E74B5,color:#fff
    style B fill:#1F7A7A,color:#fff
    style C fill:#1F497D,color:#fff"""

def test_mermaid(code: str, timeout: int = 20) -> bool:
    """Test if mermaid code renders successfully via kroki.io."""
    try:
        r = requests.post(KROKI_URL, data=code.encode('utf-8'),
                         headers={'Content-Type': 'text/plain'}, timeout=timeout)
        return r.status_code == 200 and len(r.content) > 200
    except Exception:
        return False

# ─── Process All MD Files ──────────────────────────────────────────────────────

def process_md_file(md_path: Path) -> tuple[int, int]:
    """Fix all Mermaid blocks in a single MD file. Returns (fixed, fallback) counts."""
    content = md_path.read_text(encoding='utf-8', errors='replace')
    if '```mermaid' not in content:
        return 0, 0

    fixed_count = 0
    fallback_count = 0

    def replace_mermaid(m):
        nonlocal fixed_count, fallback_count
        original_code = m.group(1).strip()

        # First try original
        if test_mermaid(original_code):
            fixed_count += 1
            return f'```mermaid\n{original_code}\n```'

        # Apply sanitization
        sanitized = sanitize_mermaid(original_code)
        time.sleep(0.3)
        if test_mermaid(sanitized):
            fixed_count += 1
            print(f"    FIXED via sanitize: {md_path.name}")
            return f'```mermaid\n{sanitized}\n```'

        # Try simplified version — extract just the diagram type and first few connections
        lines = original_code.split('\n')
        diagram_type_line = lines[0] if lines else 'graph TD'
        # Keep only simple graph lines (no subgraphs)
        simple_lines = [diagram_type_line]
        conn_count = 0
        for line in lines[1:]:
            s = line.strip()
            if not s or s.lower().startswith('subgraph') or s.lower() == 'end':
                continue
            if re.search(r'-->|==>|-\.->|---', s) and conn_count < 12:
                # Simplify the line
                s = remove_html_in_labels(s)
                s = truncate_long_labels(s)
                simple_lines.append(f'    {s}')
                conn_count += 1
            elif re.match(r'\s*style\s+', s):
                simple_lines.append(f'    {s}')

        simplified = '\n'.join(simple_lines)
        time.sleep(0.3)
        if test_mermaid(simplified):
            fixed_count += 1
            print(f"    FIXED via simplify: {md_path.name}")
            return f'```mermaid\n{simplified}\n```'

        # Final fallback: replace with clean simple diagram
        topic = md_path.stem.replace('_', ' ').replace('-', ' ')
        fallback = make_fallback_diagram(topic)
        time.sleep(0.3)
        if test_mermaid(fallback):
            fallback_count += 1
            print(f"    FALLBACK diagram: {md_path.name}")
            return f'```mermaid\n{fallback}\n```'

        # Absolute last resort: remove mermaid block entirely and note it
        fallback_count += 1
        print(f"    REMOVED (unrenderable): {md_path.name}")
        return f'```\n# Architecture Diagram\n# (See MD source for diagram code)\n{original_code[:200]}...\n```'

    new_content = re.sub(
        r'```mermaid\n(.*?)\n```',
        replace_mermaid,
        content,
        flags=re.DOTALL
    )

    if new_content != content:
        md_path.write_text(new_content, encoding='utf-8')

    return fixed_count, fallback_count

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Mermaid Diagram Fixer")
    print("=" * 60)

    md_files = sorted(CONCEPTS_DIR.rglob("*.md"))
    print(f"Found {len(md_files)} MD files to process...\n")

    total_fixed = 0
    total_fallback = 0

    for i, md_path in enumerate(md_files, 1):
        rel = md_path.relative_to(CONCEPTS_DIR)
        if '```mermaid' in md_path.read_text(encoding='utf-8', errors='replace'):
            print(f"[{i:2d}/{len(md_files)}] {rel}")
            fixed, fallback = process_md_file(md_path)
            total_fixed += fixed
            total_fallback += fallback
        else:
            print(f"[{i:2d}/{len(md_files)}] SKIP (no mermaid): {rel.name}")

    print(f"\n{'='*60}")
    print(f"  Diagrams fixed/rendered: {total_fixed}")
    print(f"  Diagrams replaced with fallback: {total_fallback}")
    print(f"  DONE. Now run generate_docx_v2.py to rebuild the DOCX.")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
