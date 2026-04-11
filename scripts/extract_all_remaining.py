"""
Fast combined filter + targeted extraction for remaining unextracted PDFs.

Processes PDFs that either:
  - Were never attempted by the broad extractor
  - Were attempted but yielded no clean CbCR data

Strategy: use PyMuPDF (fitz) for fast keyword scanning (10-50x faster than
pdfplumber), then pdfplumber only for table extraction on matching pages.
Saves incrementally so progress isn't lost if interrupted.
"""

import fitz  # PyMuPDF — fast text extraction for keyword scanning
import pdfplumber
import pandas as pd
import os
import re
import sys
import json
from datetime import date
from pathlib import Path
from paths import OUTPUT_DIR

# Import extraction helpers from existing scripts
from extract_pdf_data import try_parse_cbcr_table, extract_year_from_filename, extract_company_info

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')

# CbCR keywords — scan for these to identify relevant pages
CBCR_KEYWORDS = [
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
    'land for land', 'maakohtainen',
    'landenrapportage', 'länderspezifisch',
]

PROGRESS_FILE = os.path.join(REPORTS_DIR, 'extraction_progress.json')
OUTPUT_CSV = os.path.join(REPORTS_DIR, 'extracted_data_new.csv')


def find_cbcr_pages(pdf_path):
    """Find pages mentioning CbCR keywords using PyMuPDF (fast)."""
    cbcr_pages = set()
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(len(doc)):
            text = doc[page_num].get_text().lower()
            for kw in CBCR_KEYWORDS:
                if kw in text:
                    pn = page_num + 1  # 1-indexed
                    cbcr_pages.update([pn - 1, pn, pn + 1, pn + 2])
                    break
        doc.close()
    except Exception:
        pass
    return sorted(p for p in cbcr_pages if p >= 1)


def extract_from_pages(pdf_path, page_numbers):
    """Extract CbCR tables from specific pages only."""
    results = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for pn in page_numbers:
                if pn > len(pdf.pages):
                    continue
                page = pdf.pages[pn - 1]
                tables = page.extract_tables()
                for tidx, table in enumerate(tables):
                    if len(table) < 3:
                        continue
                    data = try_parse_cbcr_table(table, pn, tidx + 1)
                    if data:
                        results.extend(data)
    except Exception:
        pass
    return results


def load_progress():
    """Load set of already-processed filenames."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return set(json.load(f))
    return set()


def save_progress(processed):
    """Save progress incrementally."""
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(sorted(processed), f)


def main():
    print('=== Fast Combined Filter + Extraction ===\n', flush=True)

    # Determine which PDFs need processing
    all_pdfs = sorted(f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf'))
    print(f'Total PDFs in directory: {len(all_pdfs)}', flush=True)

    # PDFs that already have clean extracted data (from the first round)
    clean_path = os.path.join(REPORTS_DIR, 'extracted_data_clean.csv')
    if os.path.exists(clean_path):
        df_clean = pd.read_csv(clean_path)
        already_clean = set(df_clean['source_file'].unique())
    else:
        already_clean = set()
    print(f'Already have clean data: {len(already_clean)} PDFs', flush=True)

    # Resume from progress if interrupted
    processed = load_progress()
    print(f'Previously processed in this run: {len(processed)} PDFs', flush=True)

    # Candidates: everything not already clean and not already processed
    candidates = [f for f in all_pdfs if f not in already_clean and f not in processed]
    print(f'Candidates to process: {len(candidates)}', flush=True)

    # Load any existing new extraction results (for append)
    if os.path.exists(OUTPUT_CSV):
        existing_new = pd.read_csv(OUTPUT_CSV)
        all_extracted = existing_new.to_dict('records')
        print(f'Existing new extractions: {len(all_extracted)} rows', flush=True)
    else:
        all_extracted = []

    success = 0
    no_keywords = 0
    keywords_no_data = 0
    batch_size = 50  # Save every N PDFs

    for i, f in enumerate(candidates):
        if (i + 1) % 50 == 0:
            print(f'  Progress: {i+1}/{len(candidates)} | extracted: {success} | no keywords: {no_keywords} | keywords but no data: {keywords_no_data}', flush=True)
            # Save incrementally
            save_progress(processed)
            if all_extracted:
                pd.DataFrame(all_extracted).to_csv(OUTPUT_CSV, index=False)

        path = os.path.join(REPORTS_DIR, f)
        country, company = extract_company_info(f)
        year = extract_year_from_filename(f)

        # Step 1: find CbCR pages
        pages = find_cbcr_pages(path)

        if not pages:
            no_keywords += 1
            processed.add(f)
            continue

        # Step 2: extract from those pages
        data = extract_from_pages(path, pages)

        if data:
            for row in data:
                row['company_name'] = company
                row['country_iso'] = country
                row['report_year'] = year
                row['source_file'] = f
            all_extracted.extend(data)
            success += 1
            jur_count = len([r for r in data if r.get('jurisdiction')])
            safe_name = f[:55].encode('ascii', 'replace').decode('ascii')
            print(f'  OK   {safe_name:55} {len(data):3d} rows, {jur_count:3d} jurisdictions (pages {pages[:5]})', flush=True)
        else:
            keywords_no_data += 1

        processed.add(f)

    # Final save
    save_progress(processed)

    if all_extracted:
        df = pd.DataFrame(all_extracted)
        col_order = ['company_name', 'country_iso', 'report_year', 'jurisdiction',
                     'revenue', 'profit', 'tax_paid', 'tax_accrued', 'employees',
                     'tangible_assets', 'source_file', '_page', '_table']
        cols = [c for c in col_order if c in df.columns]
        cols += [c for c in df.columns if c not in cols]
        df = df[cols]
        df.to_csv(OUTPUT_CSV, index=False)

    print(f'\n{"="*60}', flush=True)
    print(f'EXTRACTION SUMMARY', flush=True)
    print(f'{"="*60}', flush=True)
    print(f'Candidates processed:        {len(candidates)}', flush=True)
    print(f'No CbCR keywords found:      {no_keywords}', flush=True)
    print(f'Keywords found, no tables:    {keywords_no_data}', flush=True)
    print(f'New data extracted:           {success} PDFs', flush=True)
    if all_extracted:
        df = pd.DataFrame(all_extracted)
        print(f'Total new rows:              {len(df)}', flush=True)
        print(f'Unique companies:            {df["company_name"].nunique()}', flush=True)
        print(f'\nSaved to: {OUTPUT_CSV}', flush=True)
    else:
        print('No additional data found.', flush=True)

    print('\nDone.', flush=True)


if __name__ == '__main__':
    main()
