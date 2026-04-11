"""
Download CbCR PDFs from additional sources (EBA Pillar 3, Fair Tax Foundation, etc.)
and integrate them into the tracking database.

Filters out non-report PDFs (criteria docs, briefings, etc.) and matches
downloaded reports to firms in the database where possible.
"""

import pandas as pd
import requests
import os
import re
import time
import sqlite3
from urllib.parse import urlparse, unquote
from paths import OUTPUT_DIR

SOURCES_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'additional_sources')
REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')
DB_PATH = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TJN-CbCR-Research/1.0',
}

# Skip PDFs that are clearly not CbCR reports
SKIP_PATTERNS = [
    'criteria', 'accreditation', 'model-tax', 'briefing', 'explainer',
    'standard-criteria', 'kpis-of-responsible', 'seven-magnificent',
    'Model-tax', 'Model-Tax', 'fair-tax-policy', 'Fair-Tax-Policy',
]


def sanitize_filename(name, max_len=80):
    name = re.sub(r'[^\w\s-]', '', str(name))
    name = re.sub(r'\s+', '_', name).strip('_')
    return name[:max_len]


def extract_company_from_eba_url(url):
    """Extract company name from EBA transparency exercise URL."""
    # Format: .../AT_529900S9YO2JHTIIDG38_TR_2025.pdf
    # The title column has the actual bank name
    return None  # We'll use the title instead


def download_pdf(url, filename, timeout=30):
    """Download a PDF, return (success, filepath, message)."""
    filepath = os.path.join(REPORTS_DIR, filename)
    if os.path.exists(filepath):
        return True, filepath, 'already_exists'

    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout,
                            allow_redirects=True, verify=False, stream=True)
        if resp.status_code != 200:
            return False, None, f'HTTP {resp.status_code}'

        content_type = resp.headers.get('content-type', '')
        if 'pdf' not in content_type and not url.lower().endswith('.pdf'):
            return False, None, f'Not PDF: {content_type[:30]}'

        with open(filepath, 'wb') as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        size_kb = os.path.getsize(filepath) / 1024
        if size_kb < 5:  # Too small to be a real report
            os.remove(filepath)
            return False, None, f'Too small ({size_kb:.0f} KB)'

        return True, filepath, f'OK ({size_kb:.0f} KB)'
    except Exception as e:
        return False, None, str(e)[:60]


def match_to_firm(company_name, conn):
    """Try to match a company name to a firm in the database.

    Uses strict matching to avoid false positives.
    """
    cur = conn.cursor()
    clean = company_name.strip()
    if not clean or clean == 'nan':
        return None, None

    # Exact match
    cur.execute("SELECT bvd_id, company_name FROM firms WHERE company_name = ? COLLATE NOCASE",
                (clean,))
    row = cur.fetchone()
    if row:
        return row[0], row[1]

    # Match where firm name starts with the search term (or vice versa)
    # This handles cases like "Iberdrola" matching "IBERDROLA S.A."
    cur.execute("""
        SELECT bvd_id, company_name FROM firms
        WHERE company_name LIKE ? COLLATE NOCASE
        ORDER BY LENGTH(company_name) ASC
        LIMIT 1
    """, (f'{clean}%',))
    row = cur.fetchone()
    if row:
        return row[0], row[1]

    # Also try: DB name starts with our search term's first significant words
    # e.g. "BAWAG Group AG" -> search for "BAWAG%"
    stop_words = {'THE', 'AND', 'GROUP', 'LTD', 'PLC', 'INC', 'CORP', 'AG', 'SA',
                  'NV', 'SE', 'SPA', 'GMBH', 'BV', 'AB', 'OY', 'AS', 'A/S'}
    words = [w for w in clean.split() if w.upper() not in stop_words and len(w) > 2]
    if words and len(words[0]) >= 4:
        # Use first meaningful word, require it to start the firm name
        cur.execute("""
            SELECT bvd_id, company_name FROM firms
            WHERE company_name LIKE ? COLLATE NOCASE
            ORDER BY LENGTH(company_name) ASC
            LIMIT 1
        """, (f'{words[0]}%',))
        row = cur.fetchone()
        if row:
            return row[0], row[1]

    return None, None


def main():
    import warnings
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    print('=' * 60)
    print('DOWNLOADING ADDITIONAL SOURCE PDFs')
    print('=' * 60)

    df = pd.read_csv(os.path.join(SOURCES_DIR, 'additional_sources.csv'))
    pdfs = df[df['url'].str.contains('.pdf', case=False, na=False)].copy()
    print(f'Total PDF links: {len(pdfs)}')

    # Filter out non-report PDFs
    mask = pdfs['url'].apply(lambda u: not any(p in str(u) for p in SKIP_PATTERNS))
    pdfs = pdfs[mask].copy()
    print(f'After filtering non-reports: {len(pdfs)}')

    conn = sqlite3.connect(DB_PATH)
    results = []
    downloaded = 0
    matched = 0

    for i, (_, row) in enumerate(pdfs.iterrows()):
        url = str(row['url']).strip()
        source = row['source']
        title = str(row.get('title', ''))

        # Build filename based on source
        if source == 'eba_pillar3':
            # Use title (bank name) + country from URL
            bank_name = sanitize_filename(title) if title and title != 'nan' else 'unknown_bank'
            country_match = re.search(r'/([A-Z]{2})_', url)
            country = country_match.group(1) if country_match else 'EU'
            year_match = re.search(r'_(\d{4})\.pdf', url)
            year = year_match.group(1) if year_match else 'unknown'
            filename = f'{country}_{bank_name}_{year}_eba_pillar3.pdf'
        else:
            # General: use URL basename or title
            url_basename = unquote(urlparse(url).path.split('/')[-1])
            if url_basename.endswith('.pdf'):
                filename = f'additional_{source}_{url_basename}'
            else:
                safe_title = sanitize_filename(title) if title and title != 'nan' else 'unknown'
                filename = f'additional_{source}_{safe_title}.pdf'

        # Download
        ok, filepath, msg = download_pdf(url, filename)

        # Try to match to a firm
        bvd_id, matched_name = None, None
        if ok and title and title != 'nan':
            bvd_id, matched_name = match_to_firm(title, conn)
            if bvd_id:
                matched += 1

        if ok:
            downloaded += 1

        results.append({
            'url': url,
            'source': source,
            'title': title,
            'filename': filename,
            'status': 'OK' if ok else 'FAIL',
            'message': msg,
            'bvd_id': bvd_id,
            'matched_name': matched_name,
        })

        if ok and msg != 'already_exists':
            match_info = f' -> {matched_name}' if matched_name else ''
            t = title[:40] if title and title != 'nan' else filename[:40]
            try:
                print(f'  [{i+1}] OK   {t:40} {msg}{match_info}')
            except UnicodeEncodeError:
                t_safe = t.encode('ascii', 'replace').decode()
                print(f'  [{i+1}] OK   {t_safe:40} {msg}')

        time.sleep(0.3)

    # Save download log
    log_path = os.path.join(SOURCES_DIR, 'download_log.csv')
    pd.DataFrame(results).to_csv(log_path, index=False)

    # --- Register downloaded reports in the database ---
    print('\n=== Registering in database ===')
    cur = conn.cursor()
    reports_added = 0

    for r in results:
        if r['status'] != 'OK' or not r['bvd_id']:
            continue

        # Check if report already exists
        cur.execute("""
            SELECT report_id FROM reports
            WHERE bvd_id = ? AND source = ?
        """, (r['bvd_id'], r['source']))

        if cur.fetchone():
            continue  # Already registered

        # Extract year from filename
        year_match = re.search(r'(\d{4})', r['filename'])
        year = int(year_match.group(1)) if year_match else 0

        cur.execute("""
            INSERT INTO reports (bvd_id, report_year, source, source_url,
                                 collection_date, data_extracted)
            VALUES (?, ?, ?, ?, date('now'), 0)
        """, (r['bvd_id'], year, r['source'], r['url']))
        reports_added += 1

    conn.commit()

    # Summary
    print(f'\n{"=" * 60}')
    print(f'DOWNLOAD SUMMARY')
    print(f'{"=" * 60}')
    print(f'PDFs downloaded: {downloaded}')
    print(f'Matched to firms: {matched}')
    print(f'Reports added to DB: {reports_added}')

    # DB totals
    cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports")
    print(f'\nTotal firms with reports: {cur.fetchone()[0]}')
    cur.execute("SELECT source, COUNT(*) FROM reports GROUP BY source")
    print('By source:')
    for row in cur.fetchall():
        print(f'  {row[0]}: {row[1]}')

    conn.close()
    print(f'\nLog saved to: {log_path}')
    print('Done.')


if __name__ == '__main__':
    main()
