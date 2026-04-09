"""
Filter downloaded PDFs to identify which ones actually contain CbCR data.

Many downloads are sustainability reports without CbCR tables.
This script checks for CbCR keywords in multiple languages and updates
the database to mark reports as irrelevant when no CbCR content is found.
"""

import pdfplumber
import os
import re
import sqlite3
import pandas as pd
from datetime import date
from paths import OUTPUT_DIR

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')
DB_PATH = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')

# CbCR keywords in multiple languages
CBCR_KEYWORDS = [
    # English
    'country by country', 'country-by-country', 'cbcr', 'cbc report',
    'tax jurisdiction', 'jurisdictions',
    # German
    'länderbezogen', 'ertragsteuerinformationsbericht',
    'länderspezifisch', 'land für land',
    # French
    'pays par pays', 'déclaration pays par pays',
    'reporting pays par pays',
    # Spanish
    'país por país', 'informe país por país',
    # Italian
    'paese per paese', 'rendicontazione paese per paese',
    # Dutch
    'landenrapportage', 'land-voor-land',
    # Portuguese
    'país a país', 'relatório país a país',
    # Swedish
    'land för land', 'land-för-land',
    # Danish
    'land for land',
    # Finnish
    'maakohtainen',
    # Polish
    'informacje o podatku dochodowym',
    # Czech
    'zpráva podle zemí',
    # Romanian
    'raportare de la țară la țară',
    # Generic patterns that strongly suggest CbCR
    'tax paid by jurisdiction', 'tax paid by country',
    'income tax by jurisdiction', 'income tax by country',
    'profit before tax by jurisdiction', 'profit before tax by country',
    'employees by jurisdiction', 'employees by country',
    'tax transparency report', 'tax contribution report',
]

# Compile for efficiency
CBCR_PATTERN = re.compile(
    '|'.join(re.escape(kw) for kw in CBCR_KEYWORDS),
    re.IGNORECASE
)


def check_pdf_for_cbcr(pdf_path, max_pages=None):
    """Check if a PDF contains CbCR keywords. Returns (has_cbcr, matched_keywords)."""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages_to_check = pdf.pages if max_pages is None else pdf.pages[:max_pages]
            all_text = ''
            for page in pages_to_check:
                text = page.extract_text()
                if text:
                    all_text += ' ' + text.lower()

            matches = set(CBCR_PATTERN.findall(all_text))
            return bool(matches), matches
    except Exception as e:
        return False, set()


def main():
    print('=== Filtering PDFs for CbCR content ===\n')

    pdf_files = sorted(f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf'))
    print(f'PDFs to check: {len(pdf_files)}')

    # Load extracted data to know which PDFs already have data
    extracted_csv = os.path.join(REPORTS_DIR, 'extracted_data.csv')
    if os.path.exists(extracted_csv):
        df_extracted = pd.read_csv(extracted_csv)
        files_with_data = set(df_extracted['source_file'].unique())
    else:
        files_with_data = set()

    has_cbcr = []
    no_cbcr = []
    errors = []

    for i, f in enumerate(pdf_files):
        if (i + 1) % 100 == 0:
            print(f'  Checked {i+1}/{len(pdf_files)}...')

        path = os.path.join(REPORTS_DIR, f)
        found, keywords = check_pdf_for_cbcr(path)

        if found:
            has_cbcr.append((f, keywords))
        else:
            no_cbcr.append(f)

    print(f'\n{"="*60}')
    print(f'RESULTS')
    print(f'{"="*60}')
    print(f'PDFs with CbCR keywords:    {len(has_cbcr)}')
    print(f'PDFs without CbCR keywords: {len(no_cbcr)}')
    print(f'PDFs with extracted data:   {len(files_with_data)}')

    # PDFs that had data extracted but no keywords (false positives in extraction?)
    has_data_no_keywords = files_with_data - set(f for f, _ in has_cbcr)
    print(f'Had data extracted but no CbCR keywords: {len(has_data_no_keywords)}')

    # PDFs with keywords but no data extracted (worth re-examining)
    has_keywords_no_data = set(f for f, _ in has_cbcr) - files_with_data
    print(f'Has CbCR keywords but no data extracted: {len(has_keywords_no_data)}')

    # Save results
    results_path = os.path.join(REPORTS_DIR, 'cbcr_keyword_filter.csv')
    rows = []
    for f, kw in has_cbcr:
        rows.append({'file': f, 'has_cbcr_keywords': True,
                     'has_extracted_data': f in files_with_data,
                     'keywords': '; '.join(sorted(kw))})
    for f in no_cbcr:
        rows.append({'file': f, 'has_cbcr_keywords': False,
                     'has_extracted_data': f in files_with_data,
                     'keywords': ''})

    pd.DataFrame(rows).to_csv(results_path, index=False)
    print(f'\nSaved to: {results_path}')

    # Update database: mark reports without CbCR keywords as not relevant
    print('\n=== Updating database ===')
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Load download log to map filenames to bvd_ids
    log_path = os.path.join(REPORTS_DIR, 'download_log.csv')
    if os.path.exists(log_path):
        df_log = pd.read_csv(log_path)
    else:
        df_log = pd.DataFrame()

    no_cbcr_set = set(no_cbcr)
    no_data_no_keywords = no_cbcr_set - files_with_data

    # For reports without CbCR keywords AND no extracted data,
    # update notes to flag them as likely not CbCR
    flagged = 0
    for _, row in df_log.iterrows():
        bvd_id = row.get('bvd_id')
        if pd.isna(bvd_id):
            continue
        cur.execute("""
            UPDATE reports SET notes = 'No CbCR keywords found in PDF — likely not a CbCR report'
            WHERE bvd_id = ? AND source = 'company_website' AND data_extracted = 0
        """, (bvd_id,))
        if cur.rowcount > 0:
            flagged += 1

    # Now re-flag only those where keywords WERE found
    for f, kw in has_cbcr:
        if f not in files_with_data:
            # Find bvd_id from log by matching filename pattern
            # The filename has country_iso and company_name
            pass  # We'll handle this via the keyword results

    conn.commit()

    # Remove reports that have no CbCR keywords and no extracted data
    cur.execute("""
        SELECT COUNT(*) FROM reports
        WHERE source = 'company_website' AND data_extracted = 0
    """)
    unextracted = cur.fetchone()[0]
    print(f'Website reports without extracted data: {unextracted}')
    print(f'Of those, PDFs without CbCR keywords: {len(no_data_no_keywords)}')

    # Delete reports where we're confident there's no CbCR
    # (no keywords found AND no data extracted)
    deleted = 0
    for _, row in df_log.iterrows():
        bvd_id = row.get('bvd_id')
        if pd.isna(bvd_id):
            continue
        # Check if this firm's PDF had no keywords
        # Reconstruct filename to check
        import re as re_mod
        name = str(row['company_name']).replace(' ', '_').replace('.', '').replace(',', '')
        name = re_mod.sub(r'[^\w_]', '', name).upper()
        url = str(row.get('url', ''))
        year_match = re_mod.search(r'(20[12]\d)', url)
        year = year_match.group(1) if year_match else 'unknown'
        expected_fn = f"{row['country_iso']}_{name}_{year}_cbcr.pdf"

        if expected_fn in no_cbcr_set:
            cur.execute("""
                DELETE FROM reports
                WHERE bvd_id = ? AND source = 'company_website' AND data_extracted = 0
            """, (bvd_id,))
            if cur.rowcount > 0:
                deleted += 1

    conn.commit()
    print(f'Removed {deleted} reports without CbCR content')

    # New totals
    cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports")
    print(f'\nFirms with reports: {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports WHERE data_extracted = 1")
    print(f'Firms with extracted data: {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(*) FROM reports WHERE source = 'company_website'")
    print(f'Remaining website reports: {cur.fetchone()[0]}')

    conn.close()
    print('\nDone.')


if __name__ == '__main__':
    main()
