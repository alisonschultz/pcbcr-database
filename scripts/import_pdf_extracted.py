"""
Import PDF-extracted CbCR data into the tracking database.

Maps extracted data back to firms via the download log, updates report
records with correct years, and inserts jurisdiction-level data.
"""

import sqlite3
import pandas as pd
import os
import re
from datetime import date
from paths import OUTPUT_DIR

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')
DB_PATH = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')

print('=== Importing PDF-extracted data ===\n')

# --- 1. Load extracted data and download log ---

df = pd.read_csv(os.path.join(REPORTS_DIR, 'extracted_data.csv'))
print(f'Extracted data: {len(df)} rows from {df["source_file"].nunique()} PDFs')

log = pd.read_csv(os.path.join(REPORTS_DIR, 'download_log.csv'))
print(f'Download log: {len(log)} entries')

# --- 2. Build filename -> bvd_id mapping from download log ---

# The download script names files as CC_COMPANY_NAME_YEAR_cbcr.pdf
# Reconstruct filenames from the log to map back
def make_filename(row):
    name = str(row['company_name']).replace(' ', '_').replace('.', '').replace(',', '')
    name = re.sub(r'[^\w_]', '', name).upper()
    # Extract year from URL
    url = str(row.get('url', ''))
    year_match = re.search(r'(20[12]\d)', url)
    year = year_match.group(1) if year_match else 'unknown'
    return f"{row['country_iso']}_{name}_{year}_cbcr.pdf"

log['expected_filename'] = log.apply(make_filename, axis=1)

# Build mapping: filename -> bvd_id
file_to_bvd = dict(zip(log['expected_filename'], log['bvd_id']))

# Also try matching by company name + country from extracted data
name_country_to_bvd = {}
for _, row in log.iterrows():
    key = (str(row['company_name']).upper().strip(), str(row['country_iso']).strip())
    name_country_to_bvd[key] = row['bvd_id']

# --- 3. Match extracted rows to bvd_ids ---

def find_bvd_id(row):
    # Try exact filename match
    fname = row['source_file']
    if fname in file_to_bvd:
        return file_to_bvd[fname]

    # Try name + country match
    name = str(row['company_name']).upper().strip()
    country = str(row['country_iso']).upper().strip()
    key = (name, country)
    if key in name_country_to_bvd:
        return name_country_to_bvd[key]

    return None

df['bvd_id'] = df.apply(find_bvd_id, axis=1)
matched = df['bvd_id'].notna().sum()
total = len(df)
matched_files = df[df['bvd_id'].notna()]['source_file'].nunique()
total_files = df['source_file'].nunique()
print(f'\nMatched: {matched}/{total} rows ({matched_files}/{total_files} PDFs)')

# --- 4. Import into database ---

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
today = date.today().isoformat()

reports_updated = 0
reports_created = 0
data_rows_added = 0
skipped_no_bvd = 0

# Group by source file (each file = one report)
for source_file, group in df.groupby('source_file'):
    bvd_id = group['bvd_id'].iloc[0]
    if pd.isna(bvd_id):
        skipped_no_bvd += 1
        continue

    # Get year from extracted data (most common non-NaN year in this file)
    years = group['report_year'].dropna()
    year = int(years.mode().iloc[0]) if len(years) > 0 else 0

    # Check if a report record already exists for this firm + source
    cur.execute("""
        SELECT report_id, report_year FROM reports
        WHERE bvd_id = ? AND source = 'company_website'
    """, (bvd_id,))
    existing = cur.fetchone()

    if existing:
        report_id = existing[0]
        # Update year if we extracted one and the existing is 0
        if year > 0 and existing[1] == 0:
            cur.execute("UPDATE reports SET report_year = ?, data_extracted = 1 WHERE report_id = ?",
                        (year, report_id))
        else:
            cur.execute("UPDATE reports SET data_extracted = 1 WHERE report_id = ?",
                        (report_id,))
        reports_updated += 1
    else:
        cur.execute("""
            INSERT INTO reports (bvd_id, report_year, source, collection_date, data_extracted)
            VALUES (?, ?, 'company_website', ?, 1)
        """, (bvd_id, year, today))
        report_id = cur.lastrowid
        reports_created += 1

    # Delete any existing data for this report (in case of re-import)
    cur.execute("DELETE FROM report_data WHERE report_id = ?", (report_id,))

    # Insert jurisdiction-level data
    for _, row in group.iterrows():
        jurisdiction = row.get('jurisdiction')
        if pd.isna(jurisdiction):
            jurisdiction = None

        cur.execute("""
            INSERT INTO report_data (
                report_id, jurisdiction_name,
                revenue, profit_before_tax, tax_paid, tax_accrued,
                employees, tangible_assets
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            report_id,
            jurisdiction,
            row['revenue'] if pd.notna(row.get('revenue')) else None,
            row['profit'] if pd.notna(row.get('profit')) else None,
            row['tax_paid'] if pd.notna(row.get('tax_paid')) else None,
            row['tax_accrued'] if pd.notna(row.get('tax_accrued')) else None,
            row['employees'] if pd.notna(row.get('employees')) else None,
            row['tangible_assets'] if pd.notna(row.get('tangible_assets')) else None,
        ))
        data_rows_added += 1

conn.commit()

# --- 5. Summary ---

print(f'\n{"="*60}')
print(f'IMPORT SUMMARY')
print(f'{"="*60}')
print(f'Reports updated:    {reports_updated}')
print(f'Reports created:    {reports_created}')
print(f'Data rows added:    {data_rows_added}')
print(f'Skipped (no match): {skipped_no_bvd} PDFs')

# Database totals
cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports")
print(f'\nTotal firms with reports: {cur.fetchone()[0]}')
cur.execute("SELECT COUNT(*) FROM reports WHERE data_extracted = 1")
print(f'Reports with extracted data: {cur.fetchone()[0]}')
cur.execute("SELECT COUNT(*) FROM report_data")
print(f'Total data rows: {cur.fetchone()[0]}')

cur.execute("""
    SELECT source, COUNT(*) as reports, COUNT(DISTINCT bvd_id) as firms,
           SUM(CASE WHEN data_extracted = 1 THEN 1 ELSE 0 END) as extracted
    FROM reports GROUP BY source
""")
print(f'\nBy source:')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} reports, {row[2]} firms, {row[3]} with data')

conn.close()
print('\nDone.')
