import urllib.request
import json
import os
import concurrent.futures
from html import escape

# Constants
API_URL = "https://gutendex.com/books/?sort=ascending"
BASE_DIR = os.path.join(os.getcwd(), "Books", "Gutenberg")
MAX_BOOKS = 30

def fetch_books_metadata():
    req = urllib.request.Request(
        API_URL, 
        data=None, 
        headers={'User-Agent': 'Mozilla/5.0'}
    )
    try:
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data.get('results', [])[:MAX_BOOKS]
    except Exception as e:
        print(f"Error fetching metadata: {e}")
        return []

def sanitize_filename(name):
    return "".join([c if c.isalnum() or c in " ._-()" else "_" for c in name]).strip()

def download_book(book):
    title = book.get('title', 'Unknown Title')
    authors = ", ".join([a.get('name', 'Unknown') for a in book.get('authors', [])])
    subjects = book.get('subjects', ['Uncategorized'])
    # Pick the first subject as category
    category = sanitize_filename(subjects[0].split('--')[0].strip()) if subjects else "Uncategorized"
    
    # Get download url
    formats = book.get('formats', {})
    download_url = None
    for fmt, url in formats.items():
        if 'text/plain' in fmt and '.txt' in url:
            download_url = url
            break
        if 'text/html' in fmt and '.htm' in url:
            download_url = url
    
    if not download_url:
        # Fallback to epub
        download_url = formats.get('application/epub+zip', None)

    if not download_url:
        print(f"Skipping '{title}' - No suitable download link found.")
        return book

    # Create folder
    cat_dir = os.path.join(BASE_DIR, category)
    os.makedirs(cat_dir, exist_ok=True)
    
    # Extract extension
    ext = ".txt"
    if ".epub" in download_url:
        ext = ".epub"
    elif ".htm" in download_url:
        ext = ".html"

    filename = sanitize_filename(f"{title} - {authors}")[:100] + ext
    filepath = os.path.join(cat_dir, filename)

    try:
        if not os.path.exists(filepath):
            req = urllib.request.Request(download_url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req) as response, open(filepath, 'wb') as out_file:
                out_file.write(response.read())
            print(f"Downloaded: {title}")
        else:
            print(f"Already exists: {title}")
    except Exception as e:
        print(f"Error downloading '{title}': {e}")
    
    return book

def generate_html_report(books):
    html = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <title>Gutenberg Master List</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background-color: #f4f4f9; padding: 20px; }
            h1 { color: #333; text-align: center; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; background-color: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
            th, td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #ddd; }
            th { background-color: #3b82f6; color: white; font-weight: bold; }
            tr:hover { background-color: #f1f5f9; }
            .print-btn { display: block; margin: 0 auto 20px; padding: 10px 20px; background-color: #3b82f6; color: white; border: none; border-radius: 5px; cursor: pointer; font-size: 16px; }
            @media print { .print-btn { display: none; } }
        </style>
    </head>
    <body>
        <h1>📚 Project Gutenberg Master List</h1>
        <button class="print-btn" onclick="window.print()">Print to PDF</button>
        <table>
            <thead>
                <tr>
                    <th>Title</th>
                    <th>Author(s)</th>
                    <th>Category</th>
                    <th>Downloads</th>
                </tr>
            </thead>
            <tbody>
    '''
    
    for book in books:
        title = escape(book.get('title', 'Unknown'))
        authors = escape(", ".join([a.get('name', '') for a in book.get('authors', [])]))
        subjects = book.get('subjects', [])
        category = escape(subjects[0].split('--')[0] if subjects else 'Uncategorized')
        downloads = book.get('download_count', 0)
        
        html += f'''
                <tr>
                    <td><strong>{title}</strong></td>
                    <td>{authors}</td>
                    <td>{category}</td>
                    <td>{downloads}</td>
                </tr>
        '''
        
    html += '''
            </tbody>
        </table>
    </body>
    </html>
    '''
    
    report_path = os.path.join(BASE_DIR, "Gutenberg_Master_List.html")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\\nGenerated Master List HTML at: {report_path}")

def main():
    os.makedirs(BASE_DIR, exist_ok=True)
    print("Fetching metadata from Gutendex...")
    books = fetch_books_metadata()
    if not books:
        print("No books found or failed to fetch metadata.")
        return

    print(f"Found {len(books)} books. Starting download with threading...")
    
    # Download concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        list(executor.map(download_book, books))

    print("Generating Master HTML List...")
    generate_html_report(books)

if __name__ == "__main__":
    main()
