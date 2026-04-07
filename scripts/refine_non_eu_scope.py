"""
Refine non-EU MNE scope by checking for EU/EEA subsidiaries.

Under Directive 2021/2101, non-EU MNEs with €750M+ revenue must publish
CbCR if they have a medium or large subsidiary or branch in the EU.

This script processes Orbis subsidiary exports to determine which
CANDIDATE firms should be reclassified as in-scope.

BEFORE RUNNING: Export subsidiary data from Orbis for candidate firms.
See instructions at bottom of this file.
"""

import pandas as pd
import sqlite3
import os
from paths import OUTPUT_DIR, ORBIS_DIR, DB_PATH

EU_EEA_COUNTRIES = {
    'AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'GR',
    'HU', 'IE', 'IT', 'LV', 'LT', 'LU', 'MT', 'NL', 'PL', 'PT', 'RO', 'SK',
    'SI', 'ES', 'SE',  # EU27
    'NO', 'IS', 'LI',  # EEA
}

# --- 1. Identify candidate firms ---

print('Loading master firm list...')
df_master = pd.read_csv(os.path.join(OUTPUT_DIR, 'master_firm_list.csv'))

candidates = df_master[df_master['regime_classification'].str.contains('CANDIDATE', na=False)]
print(f'  Total candidate firms: {len(candidates)}')
print(f'  By classification:')
print(candidates['regime_classification'].value_counts().to_string())

# Export candidate BvD IDs for Orbis lookup
candidate_ids_path = os.path.join(OUTPUT_DIR, 'candidate_bvd_ids.csv')
candidates[['bvd_id', 'company_name', 'country_iso', 'regime_classification']].to_csv(candidate_ids_path, index=False)
print(f'\n  Saved candidate BvD IDs to {candidate_ids_path}')
print(f'  Use this file to look up subsidiaries in Orbis.')

# --- 2. Look for subsidiary export files ---

subsidiary_candidates = []
for f in os.listdir(ORBIS_DIR):
    if 'subsidiar' in f.lower() and f.endswith(('.xlsx', '.csv')):
        subsidiary_candidates.append(f)

if not subsidiary_candidates:
    print(f"""
  No subsidiary export files found in {ORBIS_DIR}.

  ============================================================
  ORBIS EXPORT INSTRUCTIONS
  ============================================================

  To determine which non-EU candidates have EU subsidiaries:

  1. Go to Orbis web interface
  2. Upload the BvD IDs from: {candidate_ids_path}
     (or search for the candidate firms)
  3. For each firm, export subsidiary/ownership data:
     - Subsidiary name
     - Subsidiary BvD ID
     - Subsidiary country (ISO code)
     - Ownership percentage
     - Subsidiary type (subsidiary, branch, etc.)
  4. Save the export as: orbis_export_subsidiaries.xlsx
     in: {ORBIS_DIR}
  5. Re-run this script.

  Note: You may need to do this in batches due to Orbis export limits.
  Focus on EU_2021_2101_CANDIDATE firms first ({len(candidates[candidates['regime_classification'].str.contains('EU_2021_2101_CANDIDATE')])} firms),
  as they are the most likely to be in scope.
  ============================================================
""")
    print('Done (awaiting subsidiary data).')
    exit(0)

# --- 3. Process subsidiary data ---

print(f'\nFound subsidiary files: {subsidiary_candidates}')

dfs = []
for f in subsidiary_candidates:
    path = os.path.join(ORBIS_DIR, f)
    print(f'  Loading {f}...')
    if f.endswith('.csv'):
        df = pd.read_csv(path)
    else:
        # Orbis exports have "Results" sheet with data, "Search summary" with metadata
        try:
            df = pd.read_excel(path, sheet_name='Results', engine='calamine')
        except Exception:
            df = pd.read_excel(path, engine='calamine')
    print(f'    {len(df)} rows, columns: {list(df.columns)[:8]}...')
    dfs.append(df)

df_subs = pd.concat(dfs, ignore_index=True)
print(f'  Total subsidiary rows: {len(df_subs)}')
print(f'  Columns: {list(df_subs.columns)}')

# --- Handle Orbis grouped format ---
# Orbis exports parent company name on first row, then NaN for continuation rows.
# Subsidiary columns are prefixed with "SUB - ".
# We need to forward-fill the parent company name.

# Detect column names
parent_name_col = None
sub_country_col = None
sub_id_col = None
sub_name_col = None

for col in df_subs.columns:
    cl = str(col).lower()
    if 'company name' in cl and 'sub' not in cl:
        parent_name_col = col
    elif 'sub' in cl and 'country' in cl:
        sub_country_col = col
    elif 'sub' in cl and 'bvd' in cl:
        sub_id_col = col
    elif 'sub' in cl and 'name' in cl:
        sub_name_col = col

print(f'\n  Parent name column: {parent_name_col}')
print(f'  Subsidiary country column: {sub_country_col}')
print(f'  Subsidiary BvD ID column: {sub_id_col}')
print(f'  Subsidiary name column: {sub_name_col}')

if not parent_name_col or not sub_country_col:
    print('  ERROR: Could not identify required columns.')
    print(f'  Available columns: {list(df_subs.columns)}')
    exit(1)

# Forward-fill parent company name (Orbis grouped format)
df_subs[parent_name_col] = df_subs[parent_name_col].ffill()

# Drop rows where subsidiary data is entirely empty (parent-only header rows with no subs)
df_subs = df_subs.dropna(subset=[sub_country_col])
print(f'  Rows with subsidiary data: {len(df_subs)}')

# Match parent names back to BvD IDs from our candidate list
parent_name_to_bvd = dict(zip(
    candidates['company_name'].str.upper().str.strip(),
    candidates['bvd_id']
))
df_subs['_parent_bvd'] = df_subs[parent_name_col].str.upper().str.strip().map(parent_name_to_bvd)
matched_parents = df_subs['_parent_bvd'].notna().sum()
total_parents = df_subs[parent_name_col].nunique()
print(f'  Parent name -> BvD ID matched: {matched_parents} of {len(df_subs)} rows ({df_subs["_parent_bvd"].nunique()} of {total_parents} unique parents)')

# For unmatched parents, try fuzzy matching
unmatched_names = df_subs[df_subs['_parent_bvd'].isna()][parent_name_col].unique()
if len(unmatched_names) > 0:
    print(f'  Fuzzy matching {len(unmatched_names)} unmatched parent names...')
    from rapidfuzz import fuzz, process
    candidate_names = list(parent_name_to_bvd.keys())
    for name in unmatched_names:
        norm = str(name).upper().strip()
        result = process.extractOne(norm, candidate_names, scorer=fuzz.ratio, score_cutoff=85)
        if result:
            parent_name_to_bvd[norm] = parent_name_to_bvd[result[0]]
    df_subs['_parent_bvd'] = df_subs[parent_name_col].str.upper().str.strip().map(parent_name_to_bvd)
    print(f'  After fuzzy matching: {df_subs["_parent_bvd"].notna().sum()} of {len(df_subs)} rows matched')

# --- 4. Flag candidates with EU subsidiaries ---

df_subs['_eu_subsidiary'] = df_subs[sub_country_col].astype(str).str.upper().str.strip().isin(EU_EEA_COUNTRIES)

# Group by parent BvD ID
eu_subs = df_subs[df_subs['_eu_subsidiary'] & df_subs['_parent_bvd'].notna()]
eu_parents = eu_subs.groupby('_parent_bvd').agg(
    eu_subsidiary_count=('_eu_subsidiary', 'sum'),
    eu_countries=(sub_country_col, lambda x: ','.join(sorted(set(x.astype(str).str.upper().str.strip()))))
).reset_index()
eu_parents.columns = ['_parent_bvd', 'eu_subsidiary_count', 'eu_countries']

print(f'\n  Candidates with EU/EEA subsidiaries: {len(eu_parents)}')
print(f'  Total EU/EEA subsidiary links: {eu_subs.shape[0]}')
print(f'  Top subsidiary countries:')
print(f'  {eu_subs[sub_country_col].value_counts().head(15).to_string()}')

# --- 5. Update regime classifications ---

# Merge back to master list
candidate_set = set(candidates['bvd_id'])
eu_parent_set = set(eu_parents['_parent_bvd'])

updated = 0
new_classifications = []
for _, row in df_master.iterrows():
    classification = row['regime_classification']
    bvd_id = row['bvd_id']

    if bvd_id in eu_parent_set:
        # Upgrade CANDIDATE to confirmed
        classification = classification.replace('EU_2021_2101_CANDIDATE', 'EU_2021_2101_VIA_SUBSIDIARY')
        classification = classification.replace('CRD_IV_CANDIDATE', 'CRD_IV_VIA_SUBSIDIARY')
        classification = classification.replace('EXTRACTIVES_CANDIDATE', 'EXTRACTIVES_VIA_SUBSIDIARY')
        updated += 1

    new_classifications.append(classification)

df_master['regime_classification'] = new_classifications

print(f'\n  Upgraded {updated} firms from CANDIDATE to confirmed (via EU subsidiary)')

# Save updated master list
master_path = os.path.join(OUTPUT_DIR, 'master_firm_list.csv')
df_master.to_csv(master_path, index=False)
print(f'  Saved updated master_firm_list.csv')

# Update database
if os.path.exists(DB_PATH):
    print('\nUpdating database...')
    conn = sqlite3.connect(DB_PATH)
    for _, row in df_master[df_master['regime_classification'].str.contains('VIA_SUBSIDIARY', na=False)].iterrows():
        conn.execute("""
            UPDATE firms SET regime_classification = ? WHERE bvd_id = ?
        """, (row['regime_classification'], row['bvd_id']))
    conn.commit()
    conn.close()
    print(f'  Database updated.')

# Summary
print('\n' + '='*60)
print('SCOPE REFINEMENT SUMMARY')
print('='*60)
print(f'Candidate firms checked:     {len(candidates)}')
print(f'With EU/EEA subsidiaries:    {updated}')
print(f'Still candidates:            {len(candidates) - updated}')
print('\nUpdated classification counts:')
print(df_master['regime_classification'].value_counts().to_string())
print('\nDone.')
