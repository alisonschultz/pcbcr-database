"""
Build master firm list from Orbis exports.
Merges all exports, deduplicates, and classifies by CbCR regime.
"""

import pandas as pd
import os
from paths import ORBIS_DIR, OUTPUT_DIR

DATA_DIR = ORBIS_DIR  # Where Orbis exports live

EU_EEA_COUNTRIES = {
    'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR',
    'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK',
    'SI', 'ES', 'SE',  # EU27
    'NO', 'IS', 'LI',  # EEA
}

EXTRACTIVE_NACE = {'05', '06', '07', '08', '09'}
LOGGING_NACE = {'022', '0220'}

# --- 1. Load all files ---

def load_orbis_exports(pattern, source_label):
    """Load and concatenate Orbis export files matching a pattern."""
    frames = []
    for f in sorted(os.listdir(DATA_DIR)):
        if f.endswith('.xlsx') and pattern in f.lower():
            path = os.path.join(DATA_DIR, f)
            print(f'  Loading {f}...')
            df = pd.read_excel(path, sheet_name='Results', engine='calamine')
            df['_source'] = source_label
            frames.append(df)
    if frames:
        return pd.concat(frames, ignore_index=True)
    return pd.DataFrame()

print('Loading MNE exports...')
df_mne = load_orbis_exports('large_mne', 'large_mne')
print(f'  -> {len(df_mne)} rows')

print('Loading bank exports...')
df_banks = load_orbis_exports('banks', 'bank')
print(f'  -> {len(df_banks)} rows')

print('Loading extractive exports...')
df_extr = load_orbis_exports('extractive', 'extractive')
print(f'  -> {len(df_extr)} rows')

# --- 2. Harmonize columns and merge ---

# Banks and extractives have total assets columns; MNEs don't
# Find common columns and merge
all_cols = set(df_mne.columns) | set(df_banks.columns) | set(df_extr.columns)
df_all = pd.concat([df_mne, df_banks, df_extr], ignore_index=True)

# Clean up
df_all = df_all.drop(columns=['Unnamed: 0'], errors='ignore')
df_all = df_all.rename(columns={
    'Company name Latin alphabet': 'company_name',
    'Country ISO code': 'country_iso',
    'NACE Rev. 2, core code (4 digits)': 'nace_code',
    'NACE Rev. 2 main section': 'nace_section',
    'BvD ID number': 'bvd_id',
    'BvD sectors': 'bvd_sector',
    'Consolidation code': 'consolidation_code',
    'Website address': 'website',
    'ISIN number (All)': 'isin',
    'Standardized legal form': 'legal_form',
    'Date of incorporation': 'date_incorporation',
    'GUO - Name': 'guo_name',
    'GUO - BvD ID number': 'guo_bvd_id',
    'GUO - Country ISO code': 'guo_country_iso',
    'Quoted': 'listed',
})

# Rename revenue columns
rev_cols = {}
emp_cols = {}
asset_cols = {}
for c in df_all.columns:
    if 'Operating revenue' in str(c):
        year = str(c).split()[-1]
        rev_cols[c] = f'revenue_eur_{year}'
    elif 'Number of employees' in str(c):
        year = str(c).split()[-1]
        emp_cols[c] = f'employees_{year}'
    elif 'Total assets' in str(c):
        year = str(c).split()[-1]
        asset_cols[c] = f'total_assets_eur_{year}'

df_all = df_all.rename(columns={**rev_cols, **emp_cols, **asset_cols})

# Convert numeric columns - Orbis uses "n.a." for missing values
for c in df_all.columns:
    if c.startswith(('revenue_eur_', 'employees_', 'total_assets_eur_')):
        df_all[c] = pd.to_numeric(df_all[c], errors='coerce')

# --- 3. Deduplicate by BvD ID ---

print(f'\nTotal rows before dedup: {len(df_all)}')

# Keep track of which sources each BvD ID came from
source_map = df_all.groupby('bvd_id')['_source'].apply(lambda x: ','.join(sorted(set(x)))).to_dict()

# Deduplicate: keep the row with the most data (fewest NaN)
df_all['_nan_count'] = df_all.isna().sum(axis=1)
df_all = df_all.sort_values('_nan_count').drop_duplicates(subset='bvd_id', keep='first')
df_all = df_all.drop(columns=['_nan_count'])
df_all['orbis_sources'] = df_all['bvd_id'].map(source_map)

print(f'Total rows after dedup: {len(df_all)}')

# --- 4. Classify by CbCR regime ---

def get_nace_prefix(nace):
    """Get 2-digit NACE prefix."""
    if pd.isna(nace):
        return ''
    return str(nace).strip()[:2]

def get_nace_3digit(nace):
    """Get 3-digit NACE code."""
    if pd.isna(nace):
        return ''
    s = str(nace).strip().replace('.', '')
    return s[:3] if len(s) >= 3 else s

# Revenue columns for threshold check
rev_year_cols = sorted([c for c in df_all.columns if c.startswith('revenue_eur_')], reverse=True)

def has_revenue_750m(row):
    """Check if firm had >= €750M revenue in at least 1 recent year."""
    for c in rev_year_cols[:3]:  # last 3 years
        val = row.get(c)
        if pd.notna(val) and val >= 750000:  # th EUR
            return True
    return False

def has_revenue_750m_2years(row):
    """Check if firm had >= €750M revenue in at least 2 of last 3 years."""
    count = 0
    for c in rev_year_cols[:3]:
        val = row.get(c)
        if pd.notna(val) and val >= 750000:
            count += 1
    return count >= 2

# Classify regimes
regimes = []
for _, row in df_all.iterrows():
    r = []
    country = str(row.get('country_iso', '')).strip().upper()
    nace2 = get_nace_prefix(row.get('nace_code'))
    nace3 = get_nace_3digit(row.get('nace_code'))
    sector = str(row.get('bvd_sector', '')).lower()
    is_listed = str(row.get('listed', '')).strip().upper() == 'QUOTED'

    # EU 2021/2101 - general directive
    if has_revenue_750m_2years(row):
        if country in EU_EEA_COUNTRIES:
            r.append('EU_2021_2101')
        else:
            # Non-EU MNE - potentially in scope if they have EU subsidiaries
            # We flag them as candidates for now
            r.append('EU_2021_2101_CANDIDATE')

    # CRD IV - banks
    if 'bank' in sector or nace2 == '64':
        if country in EU_EEA_COUNTRIES:
            r.append('CRD_IV')
        else:
            r.append('CRD_IV_CANDIDATE')

    # Extractives Directive
    if nace2 in EXTRACTIVE_NACE or nace3 in LOGGING_NACE:
        if country in EU_EEA_COUNTRIES:
            r.append('EXTRACTIVES_DIRECTIVE')
        else:
            r.append('EXTRACTIVES_CANDIDATE')

    # Australia - revenue threshold is AUD 10M (~EUR 6M), very broad
    # For now flag all large MNEs as potential Australian scope
    if country == 'AU' or (has_revenue_750m(row) and country != 'AU'):
        # Will need ATO data to confirm Australian income
        pass  # Too broad to flag here

    if not r:
        r.append('OUT_OF_SCOPE')

    regimes.append('|'.join(r))

df_all['regime_classification'] = regimes

# --- 5. Summary statistics ---

print('\n=== REGIME CLASSIFICATION SUMMARY ===')
for regime in sorted(df_all['regime_classification'].unique()):
    count = (df_all['regime_classification'] == regime).sum()
    print(f'  {regime}: {count}')

print(f'\n=== BY COUNTRY (top 20) ===')
print(df_all['country_iso'].value_counts().head(20).to_string())

print(f'\n=== BY SOURCE ===')
print(df_all['orbis_sources'].value_counts().to_string())

# Firms that are definitely in scope (EU-headquartered + meets threshold OR EU bank/extractive)
in_scope = df_all[df_all['regime_classification'].str.contains('EU_2021_2101|CRD_IV|EXTRACTIVES_DIRECTIVE', regex=True) &
                   ~df_all['regime_classification'].str.contains('CANDIDATE|OUT_OF_SCOPE', regex=True)]
print(f'\n=== DEFINITELY IN SCOPE: {len(in_scope)} firms ===')

# --- 6. Save ---

# Save as CSV (primary format — xlsx has openpyxl compatibility issues)
csv_path = os.path.join(OUTPUT_DIR, 'master_firm_list.csv')
df_all.to_csv(csv_path, index=False)
print(f'Saved CSV to: {csv_path}')

# Save in-scope subset
inscope_path = os.path.join(OUTPUT_DIR, 'firms_in_scope.csv')
in_scope.to_csv(inscope_path, index=False)
print(f'Saved in-scope firms to: {inscope_path} ({len(in_scope)} firms)')
