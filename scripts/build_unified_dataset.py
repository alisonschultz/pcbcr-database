"""
Build a unified CbCR dataset from all collected sources.

Standardizes data from TAXPLORER (MNCs) and Banks CbCR into a single
format aligned with the EU Directive 2021/2101 required fields.
Also updates the tracking database with new report records.
"""

import pandas as pd
import sqlite3
import os
from datetime import date
from paths import OUTPUT_DIR, TAXOBS_DIR, DB_PATH
from rapidfuzz import fuzz, process

# --- Unified schema ---
# Aligns with EU Directive 2021/2101, Article 48c of Directive 2013/34/EU
# All monetary values in EUR

UNIFIED_COLUMNS = [
    'company_name',          # Name of ultimate parent undertaking
    'bvd_id',                # BvD ID (our internal identifier, if matched)
    'upe_country_iso',       # Country of ultimate parent (ISO 3166-1 alpha-2 for UPE, alpha-3 for jurisdictions)
    'sector',                # Industry sector
    'report_year',           # Fiscal year
    'jurisdiction_iso',      # Jurisdiction (ISO 3166-1 alpha-2 for UPE, alpha-3 for jurisdictions)
    'jurisdiction_name',     # Jurisdiction name
    'total_revenues',        # Total revenue
    'unrelated_revenues',    # Third-party revenue
    'related_revenues',      # Related-party revenue
    'profit_before_tax',     # Profit or loss before income tax
    'tax_accrued',           # Income tax accrued (current year)
    'tax_paid',              # Income tax paid (cash basis)
    'employees',             # Number of employees (FTE)
    'tangible_assets',       # Tangible assets other than cash
    'stated_capital',        # Stated capital
    'accumulated_earnings',  # Accumulated earnings
    'currency',              # Currency (should be EUR for EU filings)
    'source',                # Data source identifier
    'source_detail',         # Additional source info
]

# --- 1. Load and standardize TAXPLORER data ---

print('=== Loading TAXPLORER data ===')
taxplorer_files = [f for f in os.listdir(TAXOBS_DIR)
                   if 'euto' in f.lower() or 'taxplorer' in f.lower() or 'mnc' in f.lower()]
if not taxplorer_files:
    taxplorer_files = [f for f in os.listdir(TAXOBS_DIR)
                       if f.endswith('.csv') and 'bank' not in f.lower() and 'match' not in f.lower()]

if taxplorer_files:
    taxplorer_path = os.path.join(TAXOBS_DIR, taxplorer_files[0])
    print(f'  File: {taxplorer_files[0]}')
    df_tax = pd.read_csv(taxplorer_path)
    print(f'  {len(df_tax)} rows, {df_tax["mnc"].nunique()} MNCs')

    df_tax_unified = pd.DataFrame({
        'company_name': df_tax['mnc'],
        'bvd_id': None,
        'upe_country_iso': df_tax['upe_code'],
        'sector': df_tax['sector'],
        'report_year': df_tax['year'],
        'jurisdiction_iso': df_tax['jur_code'],
        'jurisdiction_name': df_tax['jur_name'],
        'total_revenues': df_tax['total_revenues'],
        'unrelated_revenues': df_tax.get('unrelated_revenues'),
        'related_revenues': df_tax.get('related_revenues'),
        'profit_before_tax': df_tax['profit_before_tax'],
        'tax_accrued': df_tax.get('tax_accrued'),
        'tax_paid': df_tax['tax_paid'],
        'employees': df_tax['employees'],
        'tangible_assets': df_tax.get('tangible_assets'),
        'stated_capital': df_tax.get('stated_capital'),
        'accumulated_earnings': df_tax.get('accumulated_earnings'),
        'currency': df_tax['currency'],
        'source': 'taxplorer',
        'source_detail': 'EU Tax Observatory TAXPLORER',
    })
else:
    print('  No TAXPLORER file found!')
    df_tax_unified = pd.DataFrame(columns=UNIFIED_COLUMNS)

# --- 2. Load and standardize Banks CbCR data ---

print('\n=== Loading Banks CbCR data ===')
banks_path = os.path.join(TAXOBS_DIR, 'banks_cbcr.csv')
if os.path.exists(banks_path):
    df_banks = pd.read_csv(banks_path)
    print(f'  {len(df_banks)} rows, {df_banks["bank"].nunique()} banks')

    # Banks data is in millions EUR — convert to EUR
    df_banks_unified = pd.DataFrame({
        'company_name': df_banks['bank'],
        'bvd_id': None,
        'upe_country_iso': df_banks['hq_code'],
        'sector': 'Banking',
        'report_year': df_banks['year'],
        'jurisdiction_iso': df_banks['code'],
        'jurisdiction_name': df_banks['country'],
        'total_revenues': df_banks['net_banking_income'] * 1e6,  # millions -> EUR
        'unrelated_revenues': None,
        'related_revenues': None,
        'profit_before_tax': df_banks['earnings_before_tax'] * 1e6,
        'tax_accrued': None,
        'tax_paid': df_banks['corporate_tx'] * 1e6,
        'employees': df_banks['staff'],
        'tangible_assets': None,
        'stated_capital': None,
        'accumulated_earnings': None,
        'currency': 'EUR',
        'source': 'tax_observatory_banks',
        'source_detail': 'EU Tax Observatory Banks CbCR (' + df_banks['data source'].fillna('') + ')',
    })
else:
    print('  Banks CbCR not found!')
    df_banks_unified = pd.DataFrame(columns=UNIFIED_COLUMNS)

# --- 3. Combine ---

print('\n=== Combining datasets ===')
df_unified = pd.concat([df_tax_unified, df_banks_unified], ignore_index=True)
print(f'  Total: {len(df_unified)} rows')
print(f'  Companies: {df_unified["company_name"].nunique()}')
print(f'  Years: {sorted(df_unified["report_year"].unique())}')

# --- 4. Match company names to master firm list ---

print('\n=== Matching to master firm list ===')
df_master = pd.read_csv(os.path.join(OUTPUT_DIR, 'master_firm_list.csv'),
                        usecols=['bvd_id', 'company_name', 'country_iso'])

# ISO3 to ISO2 mapping for country matching (TAXPLORER uses ISO3, Orbis uses ISO2)
ISO3_TO_ISO2 = {
    'AUS': 'AU', 'AUT': 'AT', 'BEL': 'BE', 'BMU': 'BM', 'BRA': 'BR',
    'CAN': 'CA', 'CHE': 'CH', 'COL': 'CO', 'DEU': 'DE', 'DNK': 'DK',
    'ESP': 'ES', 'FIN': 'FI', 'FRA': 'FR', 'GBR': 'GB', 'IND': 'IN',
    'IRL': 'IE', 'ITA': 'IT', 'JPN': 'JP', 'LUX': 'LU', 'MEX': 'MX',
    'NLD': 'NL', 'NOR': 'NO', 'PHL': 'PH', 'SVK': 'SK', 'SWE': 'SE',
    'USA': 'US', 'ZAF': 'ZA',
}

# Build name lookup, also indexed by country
master_names = {}
master_by_country = {}
for _, row in df_master.iterrows():
    if pd.notna(row['company_name']):
        key = str(row['company_name']).upper().strip()
        master_names[key] = row['bvd_id']
        country = str(row.get('country_iso', '')).strip()
        if country:
            master_by_country.setdefault(country, {})[key] = row['bvd_id']

master_name_list = list(master_names.keys())

# Manual mappings for known companies that use abbreviated/trading names
MANUAL_MATCHES = {
    'EISAI': 'JP6010001000001',
    '\u00d8RSTED': 'DK36213728',           # Ørsted
    'INDITEX': 'ESA15075062',              # Industria de Diseño Textil S.A.
    'ENDESA': 'ESA28023430',
    'RED ELECTRICA': 'ESA78003662',        # now Redeia Corporación
    'L&G': 'GB01417162',                   # Legal & General Group PLC
    'LBBW': 'DEFEB47734',                  # Landesbank Baden-Württemberg
    'Nord LB': 'GBFC012190',              # Norddeutsche Landesbank
    'Helaba': 'DEFEB40185',               # Sparkassen-Finanzgruppe Hessen-Thüringen
    'PRISA': 'ESA28297059',               # Promotora de Informaciones S.A.
    'DURATEX': 'BR97837181000147',         # now Dexco S.A.
    'USIMINAS': 'BR60894730000105',        # Usinas Siderúrgicas de Minas Gerais
    'Bankia BFA': 'ESA08663619',           # merged into CaixaBank
    'ORSTED': 'DK36213728',
}

# Get unique companies with their countries
company_countries = df_unified.groupby('company_name')['upe_country_iso'].first().to_dict()

# Apply manual matches first
company_to_bvd = {}
for name, bvd_id in MANUAL_MATCHES.items():
    company_to_bvd[name] = bvd_id

# Match each unique company — use country-filtered + token_set_ratio for short names
unique_companies = df_unified['company_name'].unique()
matched = len([n for n in unique_companies if n in company_to_bvd])
match_details = []

for name in unique_companies:
    if name in company_to_bvd:
        continue  # Already matched (manual or prior)

    norm = str(name).upper().strip()
    country_iso3 = company_countries.get(name, '')
    country_iso2 = ISO3_TO_ISO2.get(country_iso3, country_iso3)

    # 1. Exact match
    if norm in master_names:
        company_to_bvd[name] = master_names[norm]
        match_details.append((name, 'exact', master_names[norm]))
        matched += 1
        continue

    # 2. Country-filtered fuzzy match with token_set_ratio (handles "BHP" -> "BHP GROUP LIMITED")
    country_names = master_by_country.get(country_iso2, {})
    if country_names:
        country_name_list = list(country_names.keys())
        # token_set_ratio handles partial/subset matches well
        result = process.extractOne(norm, country_name_list, scorer=fuzz.token_set_ratio, score_cutoff=90)
        if result:
            company_to_bvd[name] = country_names[result[0]]
            match_details.append((name, f'country_fuzzy_{result[1]:.0f}', result[0][:50]))
            matched += 1
            continue

    # 3. Global fuzzy match with token_set_ratio (lower threshold since no country filter)
    result = process.extractOne(norm, master_name_list, scorer=fuzz.token_set_ratio, score_cutoff=95)
    if result:
        company_to_bvd[name] = master_names[result[0]]
        match_details.append((name, f'global_fuzzy_{result[1]:.0f}', result[0][:50]))
        matched += 1
        continue

    # 4. Standard fuzzy ratio (for similar-length names)
    if country_names:
        result = process.extractOne(norm, country_name_list, scorer=fuzz.ratio, score_cutoff=70)
        if result:
            company_to_bvd[name] = country_names[result[0]]
            match_details.append((name, f'country_ratio_{result[1]:.0f}', result[0][:50]))
            matched += 1
            continue

df_unified['bvd_id'] = df_unified['company_name'].map(company_to_bvd)
print(f'  Matched {matched} of {len(unique_companies)} companies to BvD IDs')
unmatched = [n for n in unique_companies if n not in company_to_bvd]
print(f'  Unmatched: {len(unmatched)}')
if unmatched:
    print(f'  Examples: {unmatched[:10]}')

# Standardize ISO codes to ISO2
df_unified['upe_country_iso'] = df_unified['upe_country_iso'].map(ISO3_TO_ISO2).fillna(df_unified['upe_country_iso'])

# --- 5. Save unified dataset ---

unified_path = os.path.join(OUTPUT_DIR, 'cbcr_unified.csv')
df_unified.to_csv(unified_path, index=False)
print(f'\n  Saved unified dataset to {unified_path}')
print(f'  {len(df_unified)} rows, {df_unified["company_name"].nunique()} companies')

# --- 6. Update tracking database ---

print('\n=== Updating tracking database ===')
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Drop and recreate report_data to reload cleanly
cur.execute("DELETE FROM report_data")
cur.execute("DELETE FROM reports")
conn.commit()

today = date.today().isoformat()
reports_added = 0
data_rows_added = 0

for (company, year, source), group in df_unified.groupby(['company_name', 'report_year', 'source']):
    bvd_id = group['bvd_id'].iloc[0]
    if pd.isna(bvd_id):
        # Create placeholder for unmatched companies
        bvd_id = f'UNMATCHED_{str(company)[:40].replace(" ", "_").upper()}'
        upe_country = group['upe_country_iso'].iloc[0]
        sector = group['sector'].iloc[0]
        try:
            cur.execute("""
                INSERT OR IGNORE INTO firms (bvd_id, company_name, country_iso, bvd_sector, regime_classification)
                VALUES (?, ?, ?, ?, 'UNKNOWN')
            """, (bvd_id, company, upe_country, sector))
        except Exception:
            pass

    # Insert report
    cur.execute("""
        INSERT OR IGNORE INTO reports (bvd_id, report_year, source, collection_date, data_extracted)
        VALUES (?, ?, ?, ?, 1)
    """, (bvd_id, int(year), source, today))

    # Get report_id
    cur.execute("""
        SELECT report_id FROM reports
        WHERE bvd_id = ? AND report_year = ? AND source = ?
    """, (bvd_id, int(year), source))
    row = cur.fetchone()
    if not row:
        continue
    report_id = row[0]
    reports_added += 1

    # Insert jurisdiction-level data
    for _, jrow in group.iterrows():
        cur.execute("""
            INSERT INTO report_data (
                report_id, jurisdiction_code, jurisdiction_name,
                revenue, profit_before_tax, tax_paid, tax_accrued,
                employees, tangible_assets, stated_capital,
                accumulated_earnings, currency
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            report_id,
            jrow.get('jurisdiction_iso'),
            jrow.get('jurisdiction_name'),
            jrow['total_revenues'] if pd.notna(jrow.get('total_revenues')) else None,
            jrow['profit_before_tax'] if pd.notna(jrow.get('profit_before_tax')) else None,
            jrow['tax_paid'] if pd.notna(jrow.get('tax_paid')) else None,
            jrow['tax_accrued'] if pd.notna(jrow.get('tax_accrued')) else None,
            jrow['employees'] if pd.notna(jrow.get('employees')) else None,
            jrow['tangible_assets'] if pd.notna(jrow.get('tangible_assets')) else None,
            jrow['stated_capital'] if pd.notna(jrow.get('stated_capital')) else None,
            jrow['accumulated_earnings'] if pd.notna(jrow.get('accumulated_earnings')) else None,
            jrow.get('currency', 'EUR'),
        ))
        data_rows_added += 1

conn.commit()

# --- 7. Summary ---

print(f'  Reports added: {reports_added}')
print(f'  Data rows added: {data_rows_added}')

cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports")
print(f'  Firms with reports: {cur.fetchone()[0]}')

cur.execute("""
    SELECT source, COUNT(*) as n_reports, COUNT(DISTINCT bvd_id) as n_firms
    FROM reports GROUP BY source
""")
print(f'\n  By source:')
for row in cur.fetchall():
    print(f'    {row[0]}: {row[1]} reports from {row[2]} firms')

cur.execute("""
    SELECT COUNT(*) FROM firms f
    WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
      AND f.regime_classification NOT LIKE '%CANDIDATE%'
      AND f.bvd_id NOT IN (SELECT DISTINCT bvd_id FROM reports)
""")
gap = cur.fetchone()[0]
print(f'\n  Compliance gap (in-scope, no report): {gap} firms')

conn.close()

# --- 8. Print data format specification ---

print(f"""
{'='*60}
UNIFIED CbCR DATA FORMAT
{'='*60}
File: {unified_path}
Aligned with EU Directive 2021/2101 (Article 48c)

Columns:
  company_name         — Name of ultimate parent undertaking
  bvd_id               — Bureau van Dijk ID (links to master firm list)
  upe_country_iso      — Country of ultimate parent (ISO 3166-1 alpha-2 for UPE, alpha-3 for jurisdictions)
  sector               — Industry sector
  report_year          — Fiscal year of report
  jurisdiction_iso     — Jurisdiction code (ISO 3166-1 alpha-2 for UPE, alpha-3 for jurisdictions)
  jurisdiction_name    — Jurisdiction name
  total_revenues       — Total revenue (EUR)
  unrelated_revenues   — Third-party revenue (EUR)
  related_revenues     — Related-party revenue (EUR)
  profit_before_tax    — Profit or loss before income tax (EUR)
  tax_accrued          — Income tax accrued, current year (EUR)
  tax_paid             — Income tax paid, cash basis (EUR)
  employees            — Number of employees (FTE)
  tangible_assets      — Tangible assets other than cash (EUR)
  stated_capital       — Stated capital (EUR)
  accumulated_earnings — Accumulated earnings (EUR)
  currency             — Currency (always EUR)
  source               — Data source identifier
  source_detail        — Additional source information

One row = one company x year x jurisdiction observation.
All monetary values in EUR.
""")

print('Done.')
