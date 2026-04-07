"""
Cross-validate master firm list against Compustat via WRDS.
Queries Compustat Global and North America for large MNEs,
then matches to our Orbis-derived master_firm_list.csv.
"""

import wrds
import pandas as pd
import os
import re
from rapidfuzz import fuzz, process
from paths import OUTPUT_DIR, WRDS_DIR

# EUR 750M threshold in thousands (Compustat reports in millions for NA, local currency for Global)
# We'll handle currency conversion below

# --- 1. Connect to WRDS ---

print('Connecting to WRDS...')
db = wrds.Connection()
print('Connected.')

# --- 2. Discover available tables and columns ---

print('\nDiscovering Compustat table structure...')
try:
    # Check what columns exist in key tables
    for tbl in ['comp.g_funda', 'comp.g_names', 'comp.g_security', 'comp.security']:
        try:
            cols = db.raw_sql(f"SELECT column_name FROM information_schema.columns WHERE table_schema || '.' || table_name = '{tbl}' ORDER BY ordinal_position")
            print(f'  {tbl}: {list(cols["column_name"][:20])}{"..." if len(cols) > 20 else ""}')
        except Exception as e:
            print(f'  {tbl}: not accessible ({type(e).__name__})')
except Exception as e:
    print(f'  Discovery failed: {e}')

# --- 3. Query Compustat Global ---

print('\nQuerying Compustat Global (comp.g_funda + g_names)...')
try:
    # First: diagnose what's in g_funda (sample rows, no filters)
    print('  Diagnosing g_funda...')
    diag = db.raw_sql("SELECT COUNT(*) as n FROM comp.g_funda")
    print(f'  Total rows in g_funda: {diag["n"].iloc[0]}')

    diag2 = db.raw_sql("""
        SELECT datafmt, consol, indfmt, popsrc, COUNT(*) as n
        FROM comp.g_funda
        WHERE fyear >= 2020
        GROUP BY datafmt, consol, indfmt, popsrc
        ORDER BY n DESC
        LIMIT 20
    """)
    print(f'  Filter combos (fyear>=2020):\n{diag2.to_string()}')

    # Check if revt/sale exist and have data
    diag3 = db.raw_sql("""
        SELECT COUNT(*) as n,
               SUM(CASE WHEN revt IS NOT NULL AND revt > 0 THEN 1 ELSE 0 END) as has_revt,
               SUM(CASE WHEN sale IS NOT NULL AND sale > 0 THEN 1 ELSE 0 END) as has_sale
        FROM comp.g_funda
        WHERE fyear >= 2020 AND datafmt = 'STD' AND consol = 'C'
    """)
    print(f'  Revenue data (STD, C): {diag3.to_string()}')

    # Determine correct datafmt value from diagnostics
    datafmt_val = 'HIST_STD' if diag2['datafmt'].iloc[0] == 'HIST_STD' else 'STD'
    print(f'  Using datafmt = {datafmt_val!r}')

    query_global = f"""
        SELECT
            a.gvkey, b.conm, a.fyear, a.datadate,
            a.revt, a.sale, a.at,
            a.curcd, b.fic, b.isin
        FROM comp.g_funda a
        INNER JOIN comp.g_names b ON a.gvkey = b.gvkey
        WHERE a.datafmt = '{datafmt_val}'
            AND a.consol = 'C'
            AND a.fyear >= 2020
            AND (a.revt > 0 OR a.sale > 0)
    """
    df_global = db.raw_sql(query_global)
    print(f'  -> {len(df_global)} rows')

    if len(df_global) > 0:
        print(f'  ISINs available: {df_global["isin"].notna().sum()} of {len(df_global)} rows')
        print(f'  Countries (top 10): {df_global["fic"].value_counts().head(10).to_dict()}')
except Exception as e:
    print(f'  Global query failed: {e}')
    import traceback; traceback.print_exc()
    df_global = pd.DataFrame()

# --- 4. Query Compustat North America ---

print('\nQuerying Compustat North America (comp.funda)...')
query_na = """
    SELECT
        a.gvkey, a.conm, a.fyear, a.datadate,
        a.revt, a.sale, a.at,
        a.curcd, a.fic
    FROM comp.funda a
    WHERE a.datafmt = 'STD'
        AND a.indfmt = 'INDL'
        AND a.consol = 'C'
        AND a.fyear >= 2020
        AND (a.revt > 0 OR a.sale > 0)
"""
df_na = db.raw_sql(query_na)
print(f'  -> {len(df_na)} rows')

# Get ISINs for NA firms - try multiple tables
print('Querying ISINs for NA firms...')
df_na['isin'] = None
for isin_table, isin_query in [
    ('comp.security', "SELECT DISTINCT gvkey, isin FROM comp.security WHERE isin IS NOT NULL"),
    ('comp.g_security', "SELECT DISTINCT gvkey, isin FROM comp.g_security WHERE isin IS NOT NULL"),
    ('comp.secm', "SELECT DISTINCT gvkey, isin FROM comp.secm WHERE isin IS NOT NULL"),
]:
    try:
        df_isin = db.raw_sql(isin_query)
        if len(df_isin) > 0:
            df_na = df_na.drop(columns=['isin'], errors='ignore')
            df_na = df_na.merge(df_isin.drop_duplicates('gvkey'), on='gvkey', how='left')
            print(f'  {isin_table}: ISINs matched for {df_na["isin"].notna().sum()} of {len(df_na)} NA rows')
            break
    except Exception as e:
        print(f'  {isin_table}: {type(e).__name__} - trying next...')

db.close()
print('WRDS connection closed.')

# --- 5. Combine and filter to large MNEs ---

df_na['_source_db'] = 'compustat_na'
if len(df_global) > 0:
    df_global['_source_db'] = 'compustat_global'
if 'isin' not in df_na.columns:
    df_na['isin'] = None
if len(df_global) > 0 and 'isin' not in df_global.columns:
    df_global['isin'] = None

# Only concat non-empty frames to avoid FutureWarning
frames = [df for df in [df_global, df_na] if len(df) > 0]
df_comp = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# Use best available revenue figure
df_comp['revenue'] = df_comp['revt'].fillna(df_comp['sale'])

# Approximate EUR conversion for threshold filtering
# Most Compustat data is in local currency (millions for some, actual for others)
# Compustat Global: amounts in thousands of local currency
# Compustat NA: amounts in millions USD
# We use a rough filter here — exact matching happens later

# For NA data (USD millions), convert to EUR thousands: * 1000 / ~1.08
# For Global data, currency varies — we keep all firms with revenue > 500M in any currency
# as a broad filter, then refine during matching

df_comp['revenue_for_filter'] = df_comp['revenue']
na_mask = df_comp['_source_db'] == 'compustat_na'
# NA data is in millions USD → convert to thousands EUR (approx)
df_comp.loc[na_mask, 'revenue_for_filter'] = df_comp.loc[na_mask, 'revenue'] * 1000 / 1.08

# Keep firms with max revenue >= 500K thousands (= 500M) in any year as broad filter
max_rev = df_comp.groupby('gvkey')['revenue_for_filter'].max().reset_index()
max_rev.columns = ['gvkey', 'max_revenue']
large_gvkeys = max_rev[max_rev['max_revenue'] >= 500000]['gvkey']
df_comp_large = df_comp[df_comp['gvkey'].isin(large_gvkeys)]

# Get latest year per firm for matching
df_comp_latest = df_comp_large.sort_values('fyear', ascending=False).drop_duplicates('gvkey', keep='first')
print(f'\nCompustat large MNEs (revenue >= ~500M EUR): {len(df_comp_latest)} firms')

# --- 6. Load Orbis master list ---

print('\nLoading Orbis master firm list...')
df_orbis = pd.read_csv(os.path.join(OUTPUT_DIR, 'master_firm_list.csv'))
print(f'  -> {len(df_orbis)} firms')

# --- 7. Name normalization for matching ---

def normalize_name(name):
    """Normalize company name for matching."""
    if pd.isna(name):
        return ''
    s = str(name).upper()
    # Remove common suffixes
    for suffix in [' PLC', ' LTD', ' LIMITED', ' INC', ' INCORPORATED', ' CORP',
                   ' CORPORATION', ' AG', ' SA', ' SE', ' NV', ' BV', ' AB',
                   ' AS', ' A/S', ' OYJ', ' SPA', ' GMBH', ' CO', ' GROUP',
                   ' HOLDINGS', ' HOLDING', ' & CO', ' PUBLIC COMPANY',
                   ' PUBLIC LIMITED COMPANY']:
        s = s.replace(suffix, '')
    # Remove punctuation
    s = re.sub(r'[^\w\s]', '', s)
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    return s

df_comp_latest = df_comp_latest.copy()
df_comp_latest['name_normalized'] = df_comp_latest['conm'].apply(normalize_name)

df_orbis = df_orbis.copy()
df_orbis['name_normalized'] = df_orbis['company_name'].apply(normalize_name)

# --- 8. Match on ISIN first, then fuzzy name ---

print('\nMatching...')

# Build ISIN lookup from Orbis (currently empty, but future-proofed)
orbis_isin_map = {}
if 'isin' in df_orbis.columns:
    for _, row in df_orbis[df_orbis['isin'].notna()].iterrows():
        for isin in str(row['isin']).split('|'):
            isin = isin.strip()
            if isin:
                orbis_isin_map[isin] = row['bvd_id']

# Build name lookup
orbis_names = dict(zip(df_orbis['name_normalized'], df_orbis['bvd_id']))
orbis_name_list = list(orbis_names.keys())
orbis_country_map = dict(zip(df_orbis['bvd_id'], df_orbis['country_iso']))

# Country code mapping (Compustat FIC → ISO2)
# Compustat uses ISO3 for some, ISO2 for others — we'll handle mismatches in fuzzy matching

matches = []
unmatched_comp = []

for _, row in df_comp_latest.iterrows():
    matched = False
    match_method = None
    orbis_bvd_id = None

    # Try ISIN match
    if pd.notna(row.get('isin')):
        isin = str(row['isin']).strip()
        if isin in orbis_isin_map:
            orbis_bvd_id = orbis_isin_map[isin]
            match_method = 'isin'
            matched = True

    # Try exact name match
    if not matched and row['name_normalized']:
        if row['name_normalized'] in orbis_names:
            orbis_bvd_id = orbis_names[row['name_normalized']]
            match_method = 'exact_name'
            matched = True

    # Try fuzzy name match
    if not matched and row['name_normalized']:
        result = process.extractOne(
            row['name_normalized'],
            orbis_name_list,
            scorer=fuzz.ratio,
            score_cutoff=85
        )
        if result:
            best_name, score, _ = result
            orbis_bvd_id = orbis_names[best_name]
            match_method = f'fuzzy_name_{score:.0f}'
            matched = True

    if matched:
        matches.append({
            'gvkey': row['gvkey'],
            'compustat_name': row['conm'],
            'compustat_country': row.get('fic', ''),
            'compustat_revenue': row['revenue'],
            'compustat_fyear': row['fyear'],
            'compustat_isin': row.get('isin', ''),
            'orbis_bvd_id': orbis_bvd_id,
            'match_method': match_method,
        })
    else:
        unmatched_comp.append({
            'gvkey': row['gvkey'],
            'compustat_name': row['conm'],
            'compustat_country': row.get('fic', ''),
            'compustat_revenue': row['revenue'],
            'compustat_fyear': row['fyear'],
            'compustat_isin': row.get('isin', ''),
            'compustat_source': row['_source_db'],
        })

df_matched = pd.DataFrame(matches)
df_unmatched_comp = pd.DataFrame(unmatched_comp)

# Find Orbis firms not in Compustat
matched_bvd_ids = set(df_matched['orbis_bvd_id']) if len(df_matched) > 0 else set()
df_orbis_only = df_orbis[~df_orbis['bvd_id'].isin(matched_bvd_ids)]

# --- 9. Enrich matched data ---

if len(df_matched) > 0:
    orbis_cols = ['bvd_id', 'company_name', 'country_iso', 'regime_classification']
    df_matched = df_matched.merge(
        df_orbis[orbis_cols],
        left_on='orbis_bvd_id',
        right_on='bvd_id',
        how='left'
    )

# --- 10. Save outputs ---

matched_path = os.path.join(WRDS_DIR, 'wrds_matched.csv')
df_matched.to_csv(matched_path, index=False)
print(f'\nSaved {len(df_matched)} matched firms to wrds_matched.csv')

unmatched_path = os.path.join(WRDS_DIR, 'wrds_only.csv')
df_unmatched_comp.to_csv(unmatched_path, index=False)
print(f'Saved {len(df_unmatched_comp)} Compustat-only firms to wrds_only.csv')

orbis_only_path = os.path.join(WRDS_DIR, 'orbis_only.csv')
df_orbis_only[['bvd_id', 'company_name', 'country_iso', 'regime_classification']].to_csv(orbis_only_path, index=False)
print(f'Saved {len(df_orbis_only)} Orbis-only firms to orbis_only.csv')

# --- 11. Summary ---

print('\n' + '='*60)
print('CROSS-VALIDATION SUMMARY')
print('='*60)
print(f'Compustat large MNEs queried:  {len(df_comp_latest)}')
print(f'Orbis master list firms:       {len(df_orbis)}')
print(f'Matched (both sources):        {len(df_matched)}')
print(f'Compustat-only (missing):      {len(df_unmatched_comp)}')
print(f'Orbis-only (private/small):    {len(df_orbis_only)}')

if len(df_matched) > 0:
    print(f'\nMatch methods:')
    print(df_matched['match_method'].value_counts().to_string())

    print(f'\nMatched firms by regime:')
    print(df_matched['regime_classification'].value_counts().head(10).to_string())

print(f'\nCompustat-only firms by country (top 15):')
if len(df_unmatched_comp) > 0:
    print(df_unmatched_comp['compustat_country'].value_counts().head(15).to_string())

print('\nDone.')
