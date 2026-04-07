"""
Download CbCR PDF reports found by the website scraper.
"""

import pandas as pd
import requests
import os
import re
import time
from urllib.parse import urlparse
from paths import REGISTERS_DIR

REPORTS_DIR = os.path.join(os.path.dirname(REGISTERS_DIR), 'collected_reports')
os.makedirs(REPORTS_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TJN-CbCR-Research/1.0',
}

def sanitize_filename(name, max_len=80):
    """Create a safe filename from company name."""
    name = re.sub(r'[^\w\s-]', '', str(name))
    name = re.sub(r'\s+', '_', name).strip('_')
    return name[:max_len]

def download_pdf(url, company_name, country_iso, bvd_id):
    """Download a PDF and save with a descriptive filename."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30,
                            allow_redirects=True, verify=False, stream=True)

        if resp.status_code != 200:
            return False, f'HTTP {resp.status_code}'

        content_type = resp.headers.get('content-type', '')
        if 'pdf' not in content_type and not url.lower().endswith('.pdf'):
            return False, f'Not PDF: {content_type[:30]}'

        # Build filename
        safe_name = sanitize_filename(company_name)
        # Try to extract year from URL
        year_match = re.search(r'20[12]\d', url)
        year_str = year_match.group() if year_match else 'unknown'

        filename = f'{country_iso}_{safe_name}_{year_str}_cbcr.pdf'
        filepath = os.path.join(REPORTS_DIR, filename)

        # Don't re-download
        if os.path.exists(filepath):
            return True, 'already_exists'

        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(filepath) / 1024
        return True, f'OK ({size_kb:.0f} KB)'

    except Exception as e:
        return False, str(e)[:60]


def main():
    import warnings
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    print('=== Downloading CbCR PDFs ===\n')

    df = pd.read_csv(os.path.join(REGISTERS_DIR, 'website_cbcr_search.csv'))
    found = df[(df['status'] == 'found') & (df['pdf_links'].str.len() > 0)].copy()
    print(f'Firms with PDF links: {len(found)}')

    # Collect all PDF URLs with their firm info
    downloads = []
    for _, row in found.iterrows():
        pdfs = str(row['pdf_links']).split(' | ')
        for url in pdfs:
            url = url.strip()
            if url and url != 'nan':
                downloads.append({
                    'url': url,
                    'company_name': row['company_name'],
                    'country_iso': row['country_iso'],
                    'bvd_id': row['bvd_id'],
                })

    print(f'Total PDF URLs to download: {len(downloads)}')

    # Download
    success = 0
    failed = 0
    results = []

    for i, dl in enumerate(downloads):
        ok, msg = download_pdf(dl['url'], dl['company_name'], dl['country_iso'], dl['bvd_id'])
        status = 'OK' if ok else 'FAIL'

        if ok:
            success += 1
        else:
            failed += 1

        results.append({**dl, 'status': status, 'message': msg})
        print(f'  [{i+1}/{len(downloads)}] {status:4} {dl["company_name"][:35]:35} {msg}')
        time.sleep(0.5)

    # Save download log
    log_path = os.path.join(REPORTS_DIR, 'download_log.csv')
    pd.DataFrame(results).to_csv(log_path, index=False)

    print(f'\n{"="*60}')
    print(f'DOWNLOAD SUMMARY')
    print(f'{"="*60}')
    print(f'Total URLs: {len(downloads)}')
    print(f'Success: {success}')
    print(f'Failed: {failed}')
    print(f'Saved to: {REPORTS_DIR}')
    print(f'Log: {log_path}')

    # List downloaded files
    files = [f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf')]
    print(f'\nDownloaded PDFs ({len(files)}):')
    for f in sorted(files):
        size = os.path.getsize(os.path.join(REPORTS_DIR, f)) / 1024
        print(f'  {f} ({size:.0f} KB)')

    print('\nDone.')


if __name__ == '__main__':
    main()
