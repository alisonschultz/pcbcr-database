"""
Update the unified CbCR dataset to include cleaned PDF-extracted data.

Adds company_website data to the existing unified dataset (which already
has TAXPLORER + Banks CbCR data) without rebuilding from scratch.
"""

import pandas as pd
import sqlite3
import os
import re
from paths import OUTPUT_DIR

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')
DB_PATH = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')

print('=== Updating Unified CbCR Dataset ===\n')

# Load existing unified dataset
unified_path = os.path.join(OUTPUT_DIR, 'cbcr_unified.csv')
df_unified = pd.read_csv(unified_path)
print(f'Existing unified data: {len(df_unified)} rows, {df_unified["company_name"].nunique()} companies')
print(f'Sources: {df_unified["source"].value_counts().to_dict()}')

# Remove any existing company_website rows (to avoid duplicates on re-run)
df_unified = df_unified[df_unified['source'] != 'company_website'].copy()
print(f'After removing old website data: {len(df_unified)} rows')

# Load cleaned PDF-extracted data
clean_path = os.path.join(REPORTS_DIR, 'extracted_data_clean.csv')
df_clean = pd.read_csv(clean_path)
print(f'\nCleaned PDF data: {len(df_clean)} rows, {df_clean["source_file"].nunique()} files')

# Load download log for bvd_id mapping
log_path = os.path.join(REPORTS_DIR, 'download_log.csv')
log = pd.read_csv(log_path)

def make_filename(row):
    name = str(row['company_name']).replace(' ', '_').replace('.', '').replace(',', '')
    name = re.sub(r'[^\w_]', '', name).upper()
    url = str(row.get('url', ''))
    year_match = re.search(r'(20[12]\d)', url)
    year = year_match.group(1) if year_match else 'unknown'
    return f"{row['country_iso']}_{name}_{year}_cbcr.pdf"

log['expected_filename'] = log.apply(make_filename, axis=1)
file_to_bvd = dict(zip(log['expected_filename'], log['bvd_id']))
file_to_company = dict(zip(log['expected_filename'], log['company_name']))

# Also match by name + country
name_country_to_bvd = {}
for _, row in log.iterrows():
    key = (str(row['company_name']).upper().strip(), str(row['country_iso']).strip())
    name_country_to_bvd[key] = row['bvd_id']

# Map clean data to bvd_ids and build unified format
rows = []
for source_file, group in df_clean.groupby('source_file'):
    # Find bvd_id
    bvd_id = file_to_bvd.get(source_file)
    if not bvd_id:
        name = str(group['company_name'].iloc[0]).upper().strip()
        country = str(group['country_iso'].iloc[0]).upper().strip()
        bvd_id = name_country_to_bvd.get((name, country))

    company_name = file_to_company.get(source_file, group['company_name'].iloc[0])
    country_iso = group['country_iso'].iloc[0]
    report_year = group['report_year'].iloc[0] if pd.notna(group['report_year'].iloc[0]) else 0

    for _, jrow in group.iterrows():
        rows.append({
            'company_name': company_name,
            'bvd_id': bvd_id,
            'upe_country_iso': country_iso,
            'sector': None,
            'report_year': int(report_year) if pd.notna(report_year) else 0,
            'jurisdiction_iso': None,
            'jurisdiction_name': jrow.get('jurisdiction'),
            'total_revenues': jrow.get('revenue'),
            'unrelated_revenues': None,
            'related_revenues': None,
            'profit_before_tax': jrow.get('profit'),
            'tax_accrued': jrow.get('tax_accrued'),
            'tax_paid': jrow.get('tax_paid'),
            'employees': jrow.get('employees'),
            'tangible_assets': jrow.get('tangible_assets'),
            'stated_capital': None,
            'accumulated_earnings': None,
            'currency': None,
            'source': 'company_website',
            'source_detail': f'PDF extraction from {source_file}',
        })

df_website = pd.DataFrame(rows)
print(f'Website rows to add: {len(df_website)}')
print(f'With bvd_id: {df_website["bvd_id"].notna().sum()}')

# Combine
df_combined = pd.concat([df_unified, df_website], ignore_index=True)

# Save
df_combined.to_csv(unified_path, index=False)
print(f'\nUpdated unified dataset: {len(df_combined)} rows')
print(f'Companies: {df_combined["company_name"].nunique()}')
print(f'Sources: {df_combined["source"].value_counts().to_dict()}')

# Also update the DB data counts
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports")
print(f'\nDB: {cur.fetchone()[0]} firms with reports')
cur.execute("SELECT COUNT(*) FROM reports WHERE data_extracted = 1")
print(f'DB: {cur.fetchone()[0]} reports with data')
cur.execute("SELECT COUNT(*) FROM report_data")
print(f'DB: {cur.fetchone()[0]} data rows')

cur.execute("""
    SELECT source, COUNT(*) as reports,
           SUM(CASE WHEN data_extracted = 1 THEN 1 ELSE 0 END) as with_data
    FROM reports GROUP BY source
""")
print('\nBy source:')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} reports, {row[2]} with data')

conn.close()
print('\nDone.')
