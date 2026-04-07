"""
Collect public CbCR data from EU Tax Observatory sources.

Sources:
1. Banks CbCR micro dataset (direct XLSX download)
2. TAXPLORER company-level CbCR (manual download from Shiny app required)
3. Documentation PDF for data structure

After running this script:
- Banks data is downloaded and parsed automatically
- For company-level data, follow manual instructions printed at the end
"""

import pandas as pd
import requests
import os
from paths import OUTPUT_DIR, TAXOBS_DIR

# --- 1. Download Banks CbCR dataset ---

BANKS_URL = 'https://taxobservatory.world/www-site/uploads/2022/02/Data_cbcr_bank-2.xlsx'
BANKS_DOC_URL = 'https://www.taxobservatory.eu/www-site/uploads/2022/02/description_bank_cbcr-1.pdf'

print('=== Banks CbCR Dataset ===')
banks_path = os.path.join(TAXOBS_DIR, 'banks_cbcr_raw.xlsx')

if not os.path.exists(banks_path):
    print(f'Downloading banks CbCR data from EU Tax Observatory...')
    resp = requests.get(BANKS_URL, timeout=60)
    resp.raise_for_status()
    with open(banks_path, 'wb') as f:
        f.write(resp.content)
    print(f'  Saved to {banks_path} ({len(resp.content)/1024:.0f} KB)')
else:
    print(f'  Already downloaded: {banks_path}')

# Download documentation
banks_doc_path = os.path.join(TAXOBS_DIR, 'banks_cbcr_description.pdf')
if not os.path.exists(banks_doc_path):
    print(f'Downloading documentation...')
    try:
        resp = requests.get(BANKS_DOC_URL, timeout=60)
        resp.raise_for_status()
        with open(banks_doc_path, 'wb') as f:
            f.write(resp.content)
        print(f'  Saved to {banks_doc_path}')
    except Exception as e:
        print(f'  Warning: Could not download documentation: {e}')

# Parse banks data
print('\nParsing banks CbCR data...')
try:
    # Try reading all sheets to understand structure
    xls = pd.ExcelFile(banks_path)
    print(f'  Sheets: {xls.sheet_names}')

    # Read the main data sheet (try common sheet names)
    df_banks = None
    for sheet in xls.sheet_names:
        df_test = pd.read_excel(banks_path, sheet_name=sheet)
        print(f'  Sheet "{sheet}": {df_test.shape[0]} rows x {df_test.shape[1]} cols')
        print(f'    Columns: {list(df_test.columns[:15])}')
        if df_test.shape[0] > 10:  # Main data sheet
            if df_banks is None or df_test.shape[0] > df_banks.shape[0]:
                df_banks = df_test
                main_sheet = sheet

    if df_banks is not None:
        print(f'\n  Main data sheet: "{main_sheet}" with {len(df_banks)} rows')
        print(f'  Columns: {list(df_banks.columns)}')
        print(f'\n  Sample data (first 3 rows):')
        print(df_banks.head(3).to_string())

        # Save as CSV for easier processing
        banks_csv = os.path.join(TAXOBS_DIR, 'banks_cbcr.csv')
        df_banks.to_csv(banks_csv, index=False)
        print(f'\n  Saved parsed data to {banks_csv}')

        # Summary
        print(f'\n  === Banks CbCR Summary ===')
        if 'bank_name' in [c.lower().replace(' ', '_') for c in df_banks.columns]:
            name_col = [c for c in df_banks.columns if 'bank' in c.lower() or 'name' in c.lower() or 'company' in c.lower()]
            if name_col:
                print(f'  Unique banks: {df_banks[name_col[0]].nunique()}')
        for c in df_banks.columns:
            if 'year' in c.lower():
                print(f'  Years: {sorted(df_banks[c].dropna().unique())}')
                break
        for c in df_banks.columns:
            if 'country' in c.lower() or 'jurisdiction' in c.lower():
                print(f'  Unique jurisdictions: {df_banks[c].nunique()}')
                break

except Exception as e:
    print(f'  Error parsing banks data: {e}')

# --- 2. TAXPLORER company-level data ---

print('\n\n=== TAXPLORER Company-Level CbCR ===')

# Download documentation
TAXPLORER_DOC_URL = 'https://taxobservatory.world//www-site/uploads/2023/02/Public_CbCRs_dataset_documentation-2.pdf'
taxplorer_doc_path = os.path.join(TAXOBS_DIR, 'taxplorer_documentation.pdf')

if not os.path.exists(taxplorer_doc_path):
    print('Downloading TAXPLORER documentation...')
    try:
        resp = requests.get(TAXPLORER_DOC_URL, timeout=60)
        resp.raise_for_status()
        with open(taxplorer_doc_path, 'wb') as f:
            f.write(resp.content)
        print(f'  Saved to {taxplorer_doc_path}')
    except Exception as e:
        print(f'  Warning: Could not download: {e}')

# Check if user has already manually downloaded the TAXPLORER data
taxplorer_candidates = [
    os.path.join(TAXOBS_DIR, 'taxplorer_data.xlsx'),
    os.path.join(TAXOBS_DIR, 'taxplorer_data.csv'),
    os.path.join(TAXOBS_DIR, 'company_cbcr_data.xlsx'),
    os.path.join(TAXOBS_DIR, 'company_cbcr_data.csv'),
    os.path.join(TAXOBS_DIR, 'public_cbcr.xlsx'),
    os.path.join(TAXOBS_DIR, 'public_cbcr.csv'),
]

taxplorer_found = None
for path in taxplorer_candidates:
    if os.path.exists(path):
        taxplorer_found = path
        break

# Also check for any xlsx/csv in the tax_observatory directory
if taxplorer_found is None:
    for f in os.listdir(TAXOBS_DIR):
        if ('taxplorer' in f.lower() or 'company' in f.lower() or 'cbcr' in f.lower()) and \
           f.endswith(('.xlsx', '.csv')) and 'bank' not in f.lower():
            taxplorer_found = os.path.join(TAXOBS_DIR, f)
            break

if taxplorer_found:
    print(f'  Found TAXPLORER data: {taxplorer_found}')
    if taxplorer_found.endswith('.csv'):
        df_taxplorer = pd.read_csv(taxplorer_found)
    else:
        df_taxplorer = pd.read_excel(taxplorer_found)
    print(f'  Shape: {df_taxplorer.shape}')
    print(f'  Columns: {list(df_taxplorer.columns)}')
    print(f'  Sample:\n{df_taxplorer.head(3).to_string()}')

    taxplorer_csv = os.path.join(TAXOBS_DIR, 'taxplorer_cbcr.csv')
    df_taxplorer.to_csv(taxplorer_csv, index=False)
    print(f'  Saved to {taxplorer_csv}')
else:
    print("""
  TAXPLORER company-level data requires manual download.
  The Shiny app does not support direct URL download.

  Instructions:
  1. Visit https://taxobservatory.shinyapps.io/company_cbcr_data/
  2. In the app, look for a "Download" button or export option
  3. Download all available data (all companies, all years)
  4. Save the file to: {dir}
     Name it: taxplorer_data.xlsx (or .csv)
  5. Re-run this script to parse it

  Alternative: Visit https://www.taxplorer.eu/Download for bulk download

  The dataset should contain ~100 MNEs with CbCR data for 2017-2021.
  Variables: company name, jurisdiction, revenue, profit, tax, employees.
""".format(dir=TAXOBS_DIR))

# --- 3. Match to master firm list ---

print('\n=== Matching to Master Firm List ===')
master_path = os.path.join(OUTPUT_DIR, 'master_firm_list.csv')
df_master = pd.read_csv(master_path, usecols=['bvd_id', 'company_name', 'country_iso', 'regime_classification'])
print(f'  Master list: {len(df_master)} firms')

# Match banks data
if df_banks is not None:
    # Identify the bank name column
    name_cols = [c for c in df_banks.columns if any(kw in c.lower() for kw in ['bank', 'name', 'company', 'institution', 'entity'])]
    if name_cols:
        bank_name_col = name_cols[0]
        bank_names = df_banks[bank_name_col].dropna().unique()
        print(f'\n  Banks dataset: {len(bank_names)} unique institutions (column: {bank_name_col})')

        # Simple name matching
        from rapidfuzz import fuzz, process
        import re

        def normalize(s):
            if pd.isna(s):
                return ''
            s = str(s).upper()
            for suffix in [' PLC', ' LTD', ' LIMITED', ' AG', ' SA', ' SE', ' NV', ' GROUP',
                          ' HOLDINGS', ' HOLDING', ' & CO', ' S.A.', ' N.V.']:
                s = s.replace(suffix, '')
            s = re.sub(r'[^\w\s]', '', s)
            return re.sub(r'\s+', ' ', s).strip()

        master_names = dict(zip(df_master['company_name'].apply(normalize), df_master['bvd_id']))
        master_name_list = list(master_names.keys())

        matched_banks = []
        unmatched_banks = []
        for name in bank_names:
            norm = normalize(name)
            if not norm:
                continue
            # Exact
            if norm in master_names:
                matched_banks.append((name, master_names[norm], 'exact'))
                continue
            # Fuzzy
            result = process.extractOne(norm, master_name_list, scorer=fuzz.ratio, score_cutoff=80)
            if result:
                matched_banks.append((name, master_names[result[0]], f'fuzzy_{result[1]:.0f}'))
            else:
                unmatched_banks.append(name)

        print(f'  Matched to master list: {len(matched_banks)}')
        print(f'  Unmatched: {len(unmatched_banks)}')
        if unmatched_banks:
            print(f'  Unmatched banks: {unmatched_banks[:10]}')

        # Save matching results
        match_df = pd.DataFrame(matched_banks, columns=['tax_obs_name', 'bvd_id', 'match_method'])
        match_df.to_csv(os.path.join(TAXOBS_DIR, 'banks_master_match.csv'), index=False)
        print(f'  Saved bank matching to banks_master_match.csv')

print('\nDone.')
