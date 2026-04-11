"""
Collect CbCR reports from Ireland's CRO (Companies Registration Office).

The CRO publishes public CbCR reports at:
  https://cro.ie/publications/document-library/

Reports are filed under EU Directive 2021/2101. The site uses a WordPress
FacetWP plugin with paginated results. This script:
1. Scrapes the document library for all filed reports
2. Downloads new PDFs
3. Extracts structured CbCR data (EU directive format)
4. Imports into the tracking database

Re-run periodically to catch new filings (big wave expected Dec 2026).
"""

import subprocess
import json
import fitz  # PyMuPDF
import re
import os
import sqlite3
import pandas as pd
from datetime import date
from bs4 import BeautifulSoup
from paths import OUTPUT_DIR

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')
DB_PATH = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')
BASE_URL = 'https://cro.ie'
LIBRARY_URL = f'{BASE_URL}/publications/document-library/'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
}


def curl_fetch(url):
    """Fetch URL using curl (bypasses Python requests blocks)."""
    result = subprocess.run(
        ['curl', '-sL', '-A', HEADERS['User-Agent'], url],
        capture_output=True, text=True, timeout=60)
    return result.stdout


def get_report_list():
    """Scrape the CRO document library for all CbCR report entries."""
    reports = []
    print(f'  Fetching: {LIBRARY_URL}', flush=True)
    html = curl_fetch(LIBRARY_URL)
    if not html:
        print('  Failed to fetch page', flush=True)
        return reports

    soup = BeautifulSoup(html, 'html.parser')

    # Try FacetWP preload data first (most reliable)
    for script in soup.find_all('script'):
        text = script.string or ''
        if 'FWP_JSON' in text:
            match = re.search(r'window\.FWP_JSON\s*=\s*({.*?});', text, re.DOTALL)
            if match:
                fwp = json.loads(match.group(1))
                pager = fwp.get('preload_data', {}).get('settings', {}).get('pager', {})
                total = pager.get('total_rows', 0)
                print(f'  FacetWP total_rows: {total}', flush=True)

    # Extract report cards
    cards = soup.select('article.card-document')

    # Also find direct links to /document-library/ subpages
    all_links = soup.select('a[href*="/document-library/"]')
    seen_urls = set()

    for card in cards:
        link = card.select_one('a[href]')
        if link:
            href = link.get('href', '')
            title = link.get_text(strip=True)
            if href and href not in seen_urls and href != LIBRARY_URL:
                seen_urls.add(href)
                reports.append({'title': title, 'detail_url': href})

    for link in all_links:
        href = link.get('href', '')
        if (href and href not in seen_urls
                and href != LIBRARY_URL
                and 'country-by-country-report-details' not in href
                and href.startswith('https://cro.ie/document-library/')):
            title = link.get_text(strip=True) or href.split('/')[-2]
            seen_urls.add(href)
            reports.append({'title': title, 'detail_url': href})

    return reports


def get_report_details(detail_url):
    """Fetch a report detail page to get the PDF download URL."""
    html = curl_fetch(detail_url)
    if not html:
        return None

    soup = BeautifulSoup(html, 'html.parser')

    # Find PDF download link
    pdf_link = soup.select_one('a[href$=".pdf"][download]')
    if not pdf_link:
        pdf_link = soup.select_one('a[href$=".pdf"]')

    pdf_url = pdf_link.get('href') if pdf_link else None
    return {'pdf_url': pdf_url}


def download_pdf(pdf_url, filename):
    """Download a PDF if not already present."""
    path = os.path.join(REPORTS_DIR, filename)
    if os.path.exists(path):
        print(f'  Already downloaded: {filename}', flush=True)
        return path

    subprocess.run(
        ['curl', '-sL', '-A', HEADERS['User-Agent'], '-o', path, pdf_url],
        timeout=60)

    if os.path.exists(path) and os.path.getsize(path) > 0:
        size_kb = os.path.getsize(path) / 1024
        print(f'  Downloaded: {filename} ({size_kb:.0f} KB)', flush=True)
        return path
    else:
        print(f'  Download failed', flush=True)
        return None


def parse_eu_cbcr_pdf(pdf_path):
    """Parse a CbCR report in EU Directive 2021/2101 format.

    These reports have a standard structure:
    - Page 1: General info (company name, country, financial year, currency)
    - Page 2+: Jurisdiction-level data table
    - Later pages: Subsidiary listings
    """
    doc = fitz.open(pdf_path)
    all_text = ''
    for page in doc:
        all_text += page.get_text() + '\n'

    # Extract general info from page 1
    page1 = doc[0].get_text()
    meta = {}

    # Company name — line after "Name of the ultimate parent"
    m = re.search(r'Name of the ultimate parent[^\n]*\n([^\n]+)', page1)
    if m:
        meta['company_name'] = m.group(1).strip()

    # Country
    m = re.search(r'Country where.*registered office\n([^\n]+)', page1)
    if m:
        meta['upe_country'] = m.group(1).strip()

    # Financial year
    m = re.search(r'start date\n(\d{4}-\d{2}-\d{2})', page1)
    if m:
        meta['fy_start'] = m.group(1)
    m = re.search(r'end date\n(\d{4}-\d{2}-\d{2})', page1)
    if m:
        meta['fy_end'] = m.group(1)
        meta['report_year'] = int(m.group(1)[:4])

    # Currency
    m = re.search(r'Reporting currency\n([A-Z]{3})', page1)
    if m:
        meta['currency'] = m.group(1)

    doc.close()

    # Parse jurisdiction data from text
    # Look for country codes (2 uppercase letters) followed by numbers
    lines = all_text.split('\n')
    data_rows = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        # Match country codes or 'n/a' (for aggregated)
        if re.match(r'^[A-Z]{2}$', line) or line == 'n/a':
            code = line
            # Collect numbers from following lines
            numbers = []
            j = i + 1
            while j < len(lines):
                val = lines[j].strip()
                cleaned = val.replace(',', '').replace(' ', '')
                if re.match(r'^-?[\d,]+$', cleaned) or re.match(r'^\([\d,]+\)$', cleaned):
                    numbers.append(val)
                    j += 1
                else:
                    break

            if len(numbers) >= 4:
                # Look back for jurisdiction name
                jur_name = lines[i - 1].strip() if i > 0 else ''

                def parse_num(s):
                    s = s.replace(',', '').strip()
                    if s.startswith('(') and s.endswith(')'):
                        return -float(s[1:-1])
                    try:
                        return float(s)
                    except ValueError:
                        return None

                row = {
                    'jurisdiction_name': jur_name,
                    'jurisdiction_iso': code if code != 'n/a' else None,
                    'revenue': parse_num(numbers[0]) if len(numbers) > 0 else None,
                    'profit_before_tax': parse_num(numbers[1]) if len(numbers) > 1 else None,
                    'tax_paid': parse_num(numbers[2]) if len(numbers) > 2 else None,
                    'tax_accrued': parse_num(numbers[3]) if len(numbers) > 3 else None,
                    'accumulated_earnings': parse_num(numbers[4]) if len(numbers) > 4 else None,
                    'employees': parse_num(numbers[5]) if len(numbers) > 5 else None,
                }
                data_rows.append(row)
            i = j
        else:
            i += 1

    return meta, data_rows


def import_to_db(meta, data_rows, source_url):
    """Import extracted CbCR data into the tracking database."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    company_name = meta.get('company_name', 'Unknown')
    report_year = meta.get('report_year', 0)
    currency = meta.get('currency', 'EUR')

    # Try to find the firm in our database
    cur.execute("SELECT bvd_id FROM firms WHERE company_name LIKE ? LIMIT 1",
                (f'%{company_name}%',))
    firm = cur.fetchone()
    bvd_id = firm[0] if firm else None

    if not bvd_id:
        # Try partial match
        words = company_name.split()
        if len(words) >= 2:
            cur.execute("SELECT bvd_id, company_name FROM firms WHERE company_name LIKE ? AND company_name LIKE ?",
                        (f'%{words[0]}%', f'%{words[1]}%'))
            matches = cur.fetchall()
            if len(matches) == 1:
                bvd_id = matches[0][0]
                print(f'  Matched to firm: {matches[0][1]} ({bvd_id})', flush=True)

    if not bvd_id:
        print(f'  WARNING: Could not match "{company_name}" to any firm in database', flush=True)
        conn.close()
        return False

    # Check if report already exists
    cur.execute("""SELECT report_id FROM reports
                   WHERE bvd_id=? AND source='cro_ireland' AND report_year=?""",
                (bvd_id, report_year))
    existing = cur.fetchone()

    if existing:
        report_id = existing[0]
        cur.execute("DELETE FROM report_data WHERE report_id=?", (report_id,))
        cur.execute("UPDATE reports SET data_extracted=1, source_url=? WHERE report_id=?",
                    (source_url, report_id))
        print(f'  Updated existing report #{report_id}', flush=True)
    else:
        cur.execute("""INSERT INTO reports (bvd_id, report_year, source, source_url, collection_date, data_extracted)
                       VALUES (?, ?, 'cro_ireland', ?, ?, 1)""",
                    (bvd_id, report_year, source_url, date.today().isoformat()))
        report_id = cur.lastrowid
        print(f'  Created report #{report_id}', flush=True)

    # Insert jurisdiction data
    for row in data_rows:
        cur.execute("""INSERT INTO report_data (
                           report_id, jurisdiction_code, jurisdiction_name,
                           revenue, profit_before_tax, tax_paid, tax_accrued,
                           employees, accumulated_earnings, currency)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (report_id, row.get('jurisdiction_iso'), row.get('jurisdiction_name'),
                     row.get('revenue'), row.get('profit_before_tax'),
                     row.get('tax_paid'), row.get('tax_accrued'),
                     row.get('employees'), row.get('accumulated_earnings'),
                     currency))

    conn.commit()
    conn.close()
    return True


def main():
    print('=== CRO Ireland CbCR Collection ===\n', flush=True)

    # Step 1: Get list of reports
    print('Fetching report list from CRO...', flush=True)
    reports = get_report_list()
    print(f'Found {len(reports)} reports\n', flush=True)

    if not reports:
        print('No reports found via scraping.', flush=True)

    imported = 0
    for report in reports:
        title = report['title']
        detail_url = report['detail_url']
        print(f'Processing: {title}', flush=True)

        # Get PDF URL from detail page
        details = get_report_details(detail_url)
        if not details or not details.get('pdf_url'):
            print(f'  No PDF found, skipping', flush=True)
            continue

        pdf_url = details['pdf_url']

        # Generate filename
        slug = re.sub(r'[^\w]+', '_', title).strip('_').upper()
        filename = f'IE_CRO_{slug}_cbcr.pdf'

        # Download PDF
        pdf_path = download_pdf(pdf_url, filename)
        if not pdf_path:
            continue

        # Parse
        meta, data_rows = parse_eu_cbcr_pdf(pdf_path)
        print(f'  Parsed: {meta.get("company_name", "?")} ({meta.get("report_year", "?")}), '
              f'{len(data_rows)} jurisdictions, currency: {meta.get("currency", "?")}', flush=True)

        if not data_rows:
            print(f'  No data extracted, skipping import', flush=True)
            continue

        # Import
        if import_to_db(meta, data_rows, pdf_url):
            imported += 1

    print(f'\n{"="*60}', flush=True)
    print(f'SUMMARY', flush=True)
    print(f'{"="*60}', flush=True)
    print(f'Reports found:    {len(reports)}', flush=True)
    print(f'Reports imported: {imported}', flush=True)

    # Show DB stats
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM reports WHERE source='cro_ireland'")
    print(f'CRO reports in DB: {cur.fetchone()[0]}', flush=True)
    conn.close()

    print('\nDone.', flush=True)


if __name__ == '__main__':
    main()
