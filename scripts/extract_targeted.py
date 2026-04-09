"""
Targeted CbCR extraction: find CbCR pages within larger reports.

Unlike the broad extractor that tries every table, this script:
1. Scans each PDF for pages mentioning CbCR keywords
2. Only extracts tables from those specific pages (± 1 page)
3. Handles PDFs that the broad extractor missed

This catches CbCR tables embedded in annual/sustainability reports
where the broad extractor failed because non-CbCR tables confused it.
"""

import pdfplumber
import pandas as pd
import os
import re
from datetime import date
from paths import OUTPUT_DIR

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')

# Keywords that indicate a CbCR section (must appear on or near the page with the table)
PAGE_KEYWORDS = [
    'country by country', 'country-by-country', 'cbcr',
    'tax jurisdiction', 'by jurisdiction',
    'länderbezogen', 'ertragsteuerinformation',
    'pays par pays', 'país por país', 'paese per paese',
    'land-voor-land', 'land för land',
    'gri 207', 'gri207',
    'tax paid by country', 'tax paid by jurisdiction',
    'income tax by country', 'income tax by jurisdiction',
    'profit before tax by jurisdiction', 'profit before tax by country',
    'tax transparency', 'pillar 3',
]

# From the main extractor
from extract_pdf_data import (
    try_parse_cbcr_table, extract_year_from_filename,
    extract_company_info
)


def find_cbcr_pages(pdf_path, max_pages=None):
    """Find page numbers that mention CbCR keywords."""
    cbcr_pages = set()
    try:
        with pdfplumber.open(pdf_path) as pdf:
            pages = pdf.pages if max_pages is None else pdf.pages[:max_pages]
            for page in pages:
                text = (page.extract_text() or '').lower()
                for kw in PAGE_KEYWORDS:
                    if kw in text:
                        page_num = page.page_number
                        # Include this page and neighbors
                        cbcr_pages.update([page_num - 1, page_num, page_num + 1])
                        break
    except Exception:
        pass
    return sorted(p for p in cbcr_pages if p >= 1)


def extract_from_pages(pdf_path, page_numbers):
    """Extract tables only from specific pages."""
    results = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num in page_numbers:
                if page_num > len(pdf.pages):
                    continue
                page = pdf.pages[page_num - 1]  # 0-indexed
                tables = page.extract_tables()
                for table_idx, table in enumerate(tables):
                    if len(table) < 3:
                        continue
                    cbcr_data = try_parse_cbcr_table(table, page_num, table_idx + 1)
                    if cbcr_data:
                        results.extend(cbcr_data)
    except Exception as e:
        print(f'  Error: {e}')
    return results


def main():
    print('=== Targeted CbCR Extraction ===\n')

    # Load the keyword filter results to know which PDFs have CbCR keywords
    filter_csv = os.path.join(REPORTS_DIR, 'cbcr_keyword_filter.csv')
    if os.path.exists(filter_csv):
        df_filter = pd.read_csv(filter_csv)
        # PDFs with keywords but no extracted data yet
        candidates = df_filter[
            (df_filter['has_cbcr_keywords'] == True) &
            (df_filter['has_extracted_data'] == False)
        ]['file'].tolist()
        print(f'PDFs with CbCR keywords but no data: {len(candidates)}')
    else:
        # Fall back to checking all PDFs that weren't already extracted
        print('No keyword filter results found, checking all unextracted PDFs...')
        extracted_csv = os.path.join(REPORTS_DIR, 'extracted_data.csv')
        if os.path.exists(extracted_csv):
            already = set(pd.read_csv(extracted_csv)['source_file'].unique())
        else:
            already = set()
        all_pdfs = sorted(f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf'))
        candidates = [f for f in all_pdfs if f not in already]
        print(f'Unextracted PDFs to check: {len(candidates)}')

    all_extracted = []
    success = 0

    for i, f in enumerate(candidates):
        if (i + 1) % 50 == 0:
            print(f'  Progress: {i+1}/{len(candidates)}, found data in {success}')

        path = os.path.join(REPORTS_DIR, f)
        country, company = extract_company_info(f)
        year = extract_year_from_filename(f)

        # Step 1: find CbCR pages
        cbcr_pages = find_cbcr_pages(path)
        if not cbcr_pages:
            continue

        # Step 2: extract tables from those pages only
        data = extract_from_pages(path, cbcr_pages)

        if data:
            for row in data:
                row['company_name'] = company
                row['country_iso'] = country
                row['report_year'] = year
                row['source_file'] = f
            all_extracted.extend(data)
            success += 1
            jur_count = len([r for r in data if r.get('jurisdiction')])
            print(f'  OK   {f[:60]:60} {len(data)} rows, {jur_count} jurisdictions (pages {cbcr_pages})')

    # Save
    if all_extracted:
        df = pd.DataFrame(all_extracted)
        col_order = ['company_name', 'country_iso', 'report_year', 'jurisdiction',
                     'revenue', 'profit', 'tax_paid', 'tax_accrued', 'employees',
                     'tangible_assets', 'source_file', '_page', '_table']
        cols = [c for c in col_order if c in df.columns]
        cols += [c for c in df.columns if c not in cols]
        df = df[cols]

        csv_path = os.path.join(REPORTS_DIR, 'extracted_data_targeted.csv')
        df.to_csv(csv_path, index=False)

        print(f'\n{"="*60}')
        print(f'TARGETED EXTRACTION SUMMARY')
        print(f'{"="*60}')
        print(f'Candidates checked: {len(candidates)}')
        print(f'New data extracted: {success} PDFs')
        print(f'Total new rows: {len(df)}')
        print(f'Unique companies: {df["company_name"].nunique()}')
        print(f'\nSaved to: {csv_path}')
    else:
        print('\nNo additional data found.')

    print('\nDone.')


if __name__ == '__main__':
    main()
