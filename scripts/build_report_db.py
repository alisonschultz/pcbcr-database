"""
Build SQLite report tracking database.
Combines master firm list with collected CbCR report data to track
which firms have reports and identify compliance gaps.
"""

import sqlite3
import pandas as pd
import os
from paths import OUTPUT_DIR, WRDS_DIR, TAXOBS_DIR, DB_PATH

# --- 1. Create database schema ---

print('Creating database...')
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.executescript("""
    DROP TABLE IF EXISTS report_data;
    DROP TABLE IF EXISTS reports;
    DROP TABLE IF EXISTS firms;

    CREATE TABLE firms (
        bvd_id TEXT PRIMARY KEY,
        company_name TEXT NOT NULL,
        country_iso TEXT,
        nace_code TEXT,
        bvd_sector TEXT,
        website TEXT,
        listed TEXT,
        guo_name TEXT,
        guo_bvd_id TEXT,
        guo_country_iso TEXT,
        regime_classification TEXT,
        orbis_sources TEXT,
        compustat_gvkey TEXT,
        compustat_match_method TEXT
    );

    CREATE TABLE reports (
        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bvd_id TEXT NOT NULL REFERENCES firms(bvd_id),
        report_year INTEGER NOT NULL,
        source TEXT NOT NULL,          -- 'tax_observatory_banks', 'taxplorer', 'national_register', 'company_website'
        source_url TEXT,
        report_format TEXT,            -- 'xlsx', 'pdf', 'ixbrl', 'csv'
        collection_date TEXT,
        data_extracted INTEGER DEFAULT 0,
        notes TEXT,
        UNIQUE(bvd_id, report_year, source)
    );

    CREATE TABLE report_data (
        data_id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_id INTEGER NOT NULL REFERENCES reports(report_id),
        jurisdiction_code TEXT,
        jurisdiction_name TEXT,
        revenue REAL,
        profit_before_tax REAL,
        tax_paid REAL,
        tax_accrued REAL,
        employees REAL,
        tangible_assets REAL,
        stated_capital REAL,
        accumulated_earnings REAL,
        currency TEXT DEFAULT 'EUR'
    );

    CREATE INDEX idx_firms_country ON firms(country_iso);
    CREATE INDEX idx_firms_regime ON firms(regime_classification);
    CREATE INDEX idx_reports_firm ON reports(bvd_id);
    CREATE INDEX idx_reports_year ON reports(report_year);
    CREATE INDEX idx_data_report ON report_data(report_id);
""")
conn.commit()
print('  Schema created.')

# --- 2. Load master firm list ---

print('\nLoading master firm list...')
df_master = pd.read_csv(os.path.join(OUTPUT_DIR, 'master_firm_list.csv'))
print(f'  {len(df_master)} firms')

# Insert firms
firm_cols = ['bvd_id', 'company_name', 'country_iso', 'nace_code', 'bvd_sector',
             'website', 'listed', 'guo_name', 'guo_bvd_id', 'guo_country_iso',
             'regime_classification', 'orbis_sources']

for col in firm_cols:
    if col not in df_master.columns:
        df_master[col] = None

df_firms = df_master[firm_cols].copy()
df_firms = df_firms.dropna(subset=['bvd_id', 'company_name'])
df_firms = df_firms.where(df_firms.notna(), None)
df_firms.to_sql('firms', conn, if_exists='append', index=False)
print(f'  Inserted {len(df_firms)} firms into database.')

# --- 3. Add WRDS cross-validation results (if available) ---

wrds_matched_path = os.path.join(WRDS_DIR, 'wrds_matched.csv')
if os.path.exists(wrds_matched_path):
    print('\nLoading WRDS match data...')
    df_wrds = pd.read_csv(wrds_matched_path)
    for _, row in df_wrds.iterrows():
        if pd.notna(row.get('orbis_bvd_id')) and pd.notna(row.get('gvkey')):
            cur.execute("""
                UPDATE firms SET compustat_gvkey = ?, compustat_match_method = ?
                WHERE bvd_id = ?
            """, (str(row['gvkey']), str(row.get('match_method', '')), str(row['orbis_bvd_id'])))
    conn.commit()
    print(f'  Updated {len(df_wrds)} firms with Compustat links.')
else:
    print('\n  WRDS match data not yet available (run wrds_crossvalidation.py first)')

# --- 4. Import Banks CbCR data ---

banks_csv = os.path.join(TAXOBS_DIR, 'banks_cbcr.csv')
banks_match_csv = os.path.join(TAXOBS_DIR, 'banks_master_match.csv')

if os.path.exists(banks_csv) and os.path.exists(banks_match_csv):
    print('\nImporting Banks CbCR data...')
    df_banks = pd.read_csv(banks_csv)
    df_match = pd.read_csv(banks_match_csv)

    # Build name-to-bvd mapping
    name_to_bvd = dict(zip(df_match['tax_obs_name'], df_match['bvd_id']))

    # For unmatched banks, create placeholder firm entries
    unmatched_bank_names = set(df_banks['bank'].unique()) - set(name_to_bvd.keys())
    for bank_name in unmatched_bank_names:
        bank_rows = df_banks[df_banks['bank'] == bank_name]
        hq_code = bank_rows['hq_code'].iloc[0] if 'hq_code' in bank_rows.columns else None
        placeholder_id = f'TAXOBS_BANK_{bank_name[:30].replace(" ", "_").upper()}'
        name_to_bvd[bank_name] = placeholder_id
        try:
            cur.execute("""
                INSERT OR IGNORE INTO firms (bvd_id, company_name, country_iso, regime_classification)
                VALUES (?, ?, ?, 'CRD_IV')
            """, (placeholder_id, bank_name, hq_code))
        except sqlite3.IntegrityError:
            pass

    conn.commit()

    # Insert reports and data
    from datetime import date
    today = date.today().isoformat()
    reports_added = 0
    data_rows_added = 0

    for bank_name in df_banks['bank'].unique():
        bvd_id = name_to_bvd.get(bank_name)
        if not bvd_id:
            continue

        bank_data = df_banks[df_banks['bank'] == bank_name]
        for year in bank_data['year'].unique():
            year_int = int(year)
            # Insert report record
            cur.execute("""
                INSERT OR IGNORE INTO reports (bvd_id, report_year, source, collection_date, data_extracted)
                VALUES (?, ?, 'tax_observatory_banks', ?, 1)
            """, (bvd_id, year_int, today))
            report_id = cur.lastrowid
            if report_id == 0:
                # Already existed — get the ID
                cur.execute("""
                    SELECT report_id FROM reports
                    WHERE bvd_id = ? AND report_year = ? AND source = 'tax_observatory_banks'
                """, (bvd_id, year_int))
                row = cur.fetchone()
                if row:
                    report_id = row[0]
                else:
                    continue
            reports_added += 1

            # Insert jurisdiction-level data
            year_data = bank_data[bank_data['year'] == year]
            for _, jrow in year_data.iterrows():
                cur.execute("""
                    INSERT INTO report_data (
                        report_id, jurisdiction_code, jurisdiction_name,
                        revenue, profit_before_tax, tax_paid, employees
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    report_id,
                    jrow.get('code'),
                    jrow.get('country'),
                    jrow.get('net_banking_income') if pd.notna(jrow.get('net_banking_income')) else None,
                    jrow.get('earnings_before_tax') if pd.notna(jrow.get('earnings_before_tax')) else None,
                    jrow.get('corporate_tx') if pd.notna(jrow.get('corporate_tx')) else None,
                    jrow.get('staff') if pd.notna(jrow.get('staff')) else None,
                ))
                data_rows_added += 1

    conn.commit()
    print(f'  Added {reports_added} bank report records')
    print(f'  Added {data_rows_added} jurisdiction-level data rows')
else:
    print('\n  Banks CbCR data not yet available (run collect_taxobservatory.py first)')

# --- 5. Import TAXPLORER data (if available) ---

taxplorer_csv = os.path.join(TAXOBS_DIR, 'taxplorer_cbcr.csv')
if os.path.exists(taxplorer_csv):
    print('\nImporting TAXPLORER company-level data...')
    df_tax = pd.read_csv(taxplorer_csv)
    print(f'  {len(df_tax)} rows loaded')
    print(f'  Columns: {list(df_tax.columns)}')
    # Parsing depends on actual column structure — will be updated once data is available
    print('  (Detailed import will be implemented once column structure is confirmed)')
else:
    print('\n  TAXPLORER data not yet available (manual download required)')

# --- 6. Summary statistics ---

print('\n' + '='*60)
print('DATABASE SUMMARY')
print('='*60)

cur.execute("SELECT COUNT(*) FROM firms")
print(f'Total firms:                    {cur.fetchone()[0]}')

cur.execute("SELECT COUNT(*) FROM firms WHERE regime_classification NOT LIKE '%OUT_OF_SCOPE%'")
print(f'Firms in scope (any regime):    {cur.fetchone()[0]}')

cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports")
print(f'Firms with any report:          {cur.fetchone()[0]}')

cur.execute("SELECT COUNT(*) FROM reports")
print(f'Total report records:           {cur.fetchone()[0]}')

cur.execute("SELECT COUNT(*) FROM report_data")
print(f'Total data rows:                {cur.fetchone()[0]}')

cur.execute("""
    SELECT source, COUNT(*) as n_reports, COUNT(DISTINCT bvd_id) as n_firms
    FROM reports
    GROUP BY source
""")
print(f'\nReports by source:')
for row in cur.fetchall():
    print(f'  {row[0]}: {row[1]} reports from {row[2]} firms')

# Compliance gap: in-scope firms without reports
cur.execute("""
    SELECT COUNT(*) FROM firms f
    WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
      AND f.regime_classification NOT LIKE '%CANDIDATE%'
      AND f.bvd_id NOT IN (SELECT DISTINCT bvd_id FROM reports)
""")
gap = cur.fetchone()[0]
print(f'\nCompliance gap (in-scope, no report found): {gap} firms')

cur.execute("""
    SELECT regime_classification, COUNT(*) as n
    FROM firms
    WHERE regime_classification NOT LIKE '%OUT_OF_SCOPE%'
      AND regime_classification NOT LIKE '%CANDIDATE%'
      AND bvd_id NOT IN (SELECT DISTINCT bvd_id FROM reports)
    GROUP BY regime_classification
    ORDER BY n DESC
    LIMIT 10
""")
print('  By regime:')
for row in cur.fetchall():
    print(f'    {row[0]}: {row[1]}')

conn.close()
print(f'\nDatabase saved to: {DB_PATH}')
print('Done.')
