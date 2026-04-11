"""
Clean extracted CbCR data by removing non-CbCR table rows.

The broad PDF extractor picked up many non-CbCR tables (balance sheets,
employee demographics, asset breakdowns, etc.). This script:

1. Filters rows to only keep valid country/jurisdiction names
2. Removes tables that are clearly not CbCR (HR data, balance sheet items)
3. Validates that remaining data looks like CbCR (has jurisdiction + financials)
4. Updates the database with cleaned data
"""

import pandas as pd
import sqlite3
import os
import re
from datetime import date
from paths import OUTPUT_DIR

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')
DB_PATH = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')

# Comprehensive list of valid country/jurisdiction names for CbCR
# Includes ISO names, common variants, and CbCR-specific aggregate categories
VALID_JURISDICTIONS = {
    # EU member states
    'austria', 'belgium', 'bulgaria', 'croatia', 'cyprus', 'czech republic',
    'czechia', 'denmark', 'estonia', 'finland', 'france', 'germany', 'greece',
    'hungary', 'ireland', 'italy', 'latvia', 'lithuania', 'luxembourg', 'malta',
    'netherlands', 'poland', 'portugal', 'romania', 'slovakia', 'slovenia',
    'spain', 'sweden',
    # EEA / EFTA
    'iceland', 'liechtenstein', 'norway', 'switzerland',
    # Other Europe
    'albania', 'andorra', 'armenia', 'azerbaijan', 'belarus', 'bosnia',
    'bosnia and herzegovina', 'georgia', 'kosovo', 'moldova', 'monaco',
    'montenegro', 'north macedonia', 'russia', 'russian federation', 'serbia',
    'turkey', 'turkiye', 'ukraine', 'united kingdom', 'uk', 'great britain',
    'england', 'scotland', 'wales', 'northern ireland', 'gibraltar', 'guernsey',
    'jersey', 'isle of man',
    # Americas
    'united states', 'us', 'usa', 'u.s.', 'u.s.a.', 'u.s', 'canada', 'mexico',
    'argentina', 'brazil', 'chile', 'colombia', 'peru', 'venezuela', 'ecuador',
    'uruguay', 'paraguay', 'bolivia', 'costa rica', 'panama', 'cuba',
    'dominican republic', 'guatemala', 'honduras', 'el salvador', 'nicaragua',
    'jamaica', 'trinidad', 'trinidad and tobago', 'puerto rico', 'bermuda',
    'cayman islands', 'bahamas', 'barbados', 'belize',
    'british virgin islands', 'curacao', 'aruba', 'suriname', 'guyana',
    # Asia
    'china', 'japan', 'south korea', 'korea', 'north korea', 'taiwan',
    'hong kong', 'macau', 'macao', 'india', 'pakistan', 'bangladesh',
    'sri lanka', 'nepal', 'myanmar', 'thailand', 'vietnam', 'cambodia',
    'laos', 'malaysia', 'singapore', 'indonesia', 'philippines', 'brunei',
    'mongolia', 'kazakhstan', 'uzbekistan', 'turkmenistan', 'kyrgyzstan',
    'tajikistan', 'afghanistan',
    # Middle East
    'saudi arabia', 'uae', 'united arab emirates', 'qatar', 'kuwait', 'bahrain',
    'oman', 'yemen', 'iraq', 'iran', 'israel', 'jordan', 'lebanon', 'syria',
    'palestine',
    # Africa
    'south africa', 'nigeria', 'kenya', 'egypt', 'morocco', 'algeria', 'tunisia',
    'libya', 'ethiopia', 'ghana', 'tanzania', 'uganda', 'mozambique', 'angola',
    'cameroon', 'ivory coast', "cote d'ivoire", 'senegal', 'mali', 'niger',
    'burkina faso', 'guinea', 'chad', 'zimbabwe', 'zambia', 'botswana',
    'namibia', 'mauritius', 'madagascar', 'rwanda', 'congo',
    'democratic republic of congo', 'drc', 'gabon', 'equatorial guinea',
    'mauritania', 'sierra leone', 'liberia', 'togo', 'benin', 'malawi',
    'somalia', 'eritrea', 'djibouti', 'eswatini', 'lesotho',
    # Oceania
    'australia', 'new zealand', 'fiji', 'papua new guinea',
    # CbCR aggregate / special categories (keep these)
    'total', 'group total', 'consolidated total', 'grand total',
    'other', 'others', 'other countries', 'other jurisdictions',
    'rest of world', 'rest of the world', 'row',
    'unallocated', 'not allocated', 'intercompany', 'eliminations',
    'consolidation adjustments', 'adjustments',
    'international', 'worldwide', 'global',
    'europe', 'asia', 'americas', 'africa', 'middle east',
    'asia pacific', 'asia-pacific', 'apac', 'emea', 'latam',
    'latin america', 'north america', 'sub-saharan africa',
    # Tax haven / offshore territories commonly in CbCR
    'british virgin islands', 'bvi', 'cayman islands', 'bermuda',
    'channel islands', 'isle of man', 'gibraltar', 'mauritius',
    'seychelles', 'bahamas', 'curacao', 'aruba', 'saint kitts',
    'saint lucia', 'antigua', 'turks and caicos',
    # Common abbreviations
    'de', 'fr', 'it', 'es', 'nl', 'be', 'at', 'ch', 'se', 'no', 'dk',
    'fi', 'pt', 'ie', 'pl', 'cz', 'hu', 'ro', 'bg', 'hr', 'sk', 'si',
    'lt', 'lv', 'ee', 'mt', 'cy', 'lu', 'gb', 'cn', 'jp', 'kr', 'in',
    'br', 'mx', 'au', 'nz', 'za', 'ng', 'eg', 'ke',
}

# Patterns that clearly indicate NON-CbCR table rows
NON_CBCR_PATTERNS = [
    # Financial statement items
    r'^total assets', r'^total liabilities', r'^total equity',
    r'^total (current|non.?current|financial|comprehensive|operating)',
    r'^total income', r'^total expense', r'^total revenue', r'^total tax',
    r'^total self', r'^total ghg', r'^total \d{4}',
    r'^trade payables', r'^trade receivables', r'^cash and cash',
    r'^lease liab', r'^investments$', r'^provisions',
    r'^deferred tax', r'^goodwill', r'^intangible',
    r'^property.?plant', r'^right.?of.?use', r'^borrowings',
    r'^financial (assets|liab)', r'^other (assets|liab)',
    r'^other\s+(financial|current|non.?current|operating|comprehensive|intangible|receiv|payab|equity|income|expense|asset|bank)',
    r'^current (assets|liab)', r'^non.?current',
    r'^share capital', r'^retained earnings', r'^reserves',
    r'^derivative', r'^inventory', r'^inventories',
    r'^impairment', r'^depreciation', r'^amortis[ae]tion',
    r'^operating (profit|loss|income|expense)', r'^net (income|profit|loss|interest|sales|revenue|book|carrying)',
    r'^revenue from', r'^cost\b', r'^cost of',
    r'^gross (profit|margin)', r'^ebitda', r'^ebit$',
    r'^interest (income|expense)', r'^tax (benefit|credit)',
    r'^income tax\b', r'^profit (for|before|after)',
    r'^(net|gross) revenue', r'^fee', r'^revenue$',
    r'^dividend', r'^equity\b', r'^debt\b', r'^loan',
    r'^deposit', r'^securit', r'^bond', r'^hedg', r'^swap',
    r'^instrument', r'^fair value', r'^at fair value',
    r'^capital\b', r'^share\b', r'^stock\b',
    # HR / employee data
    r'^female', r'^male', r'^gender', r'^diversity',
    r'^full.?time', r'^part.?time', r'^permanent', r'^temporary',
    r'^headcount', r'^average .*(employee|staff)',
    r'^new (hires|employees)', r'^leavers', r'^turnover rate',
    r'^training', r'^compensation', r'^salary',
    r'^of which (women|men|female|male)',
    r'^fte', r'^women$', r'^men$',
    # Environmental / ESG
    r'^co2', r'^carbon', r'^emissions', r'^energy',
    r'^water', r'^waste', r'^renewable', r'^gwh', r'^mwh',
    r'^scope [123]', r'^greenhouse', r'^electricity',
    r'^(non.?)?hazardous', r'^groundwater', r'^kg$',
    # Foreign language financial terms
    r'^summe', r'^strumenti', r'^risultato', r'^ricavi',
    r'^attivit', r'^passivit', r'^patrimonio', r'^debiti',
    r'^crediti', r'^proventi', r'^oneri', r'^costi',
    r'^utile', r'^perdita', r'^imposte',
    r'^gst$', r'^usd$', r'^eur$', r'^gbp$',
    # Other noise
    r'^additions$', r'^disposals', r'^carrying amount',
    r'^at [\d]+ (january|december|march)',
    r'^as at', r'^balance at', r'^\d{1,2}/\d{1,2}/\d{4}',
    r'^fy\d{4}', r'^q[1-4] ', r'^h[12] ',
    r'^number$', r'^%$', r'^n/a$', r'^-$', r'^notes?$',
    r'^page \d', r'^source:', r'^see note',
    r'^corporate guarantee',
]

NON_CBCR_RE = re.compile('|'.join(NON_CBCR_PATTERNS), re.IGNORECASE)


def is_valid_jurisdiction(text):
    """Check if text is a valid country/jurisdiction name."""
    if not text or pd.isna(text):
        return False
    t = str(text).lower().strip().rstrip('.*:')
    if len(t) < 2 or len(t) > 50:
        return False
    # First check if it matches a non-CbCR pattern (takes priority)
    if NON_CBCR_RE.match(t):
        return False
    # Exact match
    if t in VALID_JURISDICTIONS:
        return True
    # Check if the text is a close variant of a valid jurisdiction
    # Only allow prefix matching if the text is short (likely a country name)
    # This prevents "Other financial liabilities" matching "Other"
    if len(t) <= 30:
        for j in VALID_JURISDICTIONS:
            if len(j) >= 4:
                # text starts with a jurisdiction AND is not much longer
                if t.startswith(j) and len(t) <= len(j) + 15:
                    return True
                # jurisdiction starts with text (abbreviation)
                if j.startswith(t) and len(t) >= 4:
                    return True
    return False


def is_non_cbcr_row(jurisdiction):
    """Check if jurisdiction text indicates a non-CbCR table row."""
    if not jurisdiction or pd.isna(jurisdiction):
        return True
    t = str(jurisdiction).strip()
    return bool(NON_CBCR_RE.match(t))


def main():
    print('=== Cleaning Extracted CbCR Data ===\n')

    df = pd.read_csv(os.path.join(REPORTS_DIR, 'extracted_data.csv'))
    print(f'Original data: {len(df)} rows from {df["source_file"].nunique()} PDFs')
    print(f'Original jurisdictions: {df["jurisdiction"].nunique()}')

    # Step 1: Remove rows with non-CbCR jurisdictions
    mask_non_cbcr = df['jurisdiction'].apply(is_non_cbcr_row)
    removed_non_cbcr = mask_non_cbcr.sum()
    print(f'\nRows matching non-CbCR patterns: {removed_non_cbcr}')

    # Step 2: Check remaining rows for valid jurisdictions
    df_filtered = df[~mask_non_cbcr].copy()
    mask_valid = df_filtered['jurisdiction'].apply(is_valid_jurisdiction)
    invalid_jurs = df_filtered[~mask_valid]['jurisdiction'].value_counts()
    print(f'Rows with valid jurisdictions: {mask_valid.sum()}')
    print(f'Rows with invalid jurisdictions: {(~mask_valid).sum()}')

    if len(invalid_jurs) > 0:
        print(f'\nTop 30 invalid jurisdiction values (will be removed):')
        for jur, count in invalid_jurs.head(30).items():
            print(f'  {jur}: {count}')

    # Keep only valid jurisdiction rows
    df_clean = df_filtered[mask_valid].copy()

    # Step 3: Remove files where we have fewer than 2 valid rows
    # (likely not real CbCR tables)
    file_counts = df_clean.groupby('source_file').size()
    small_files = file_counts[file_counts < 2].index
    df_clean = df_clean[~df_clean['source_file'].isin(small_files)]
    print(f'\nRemoved {len(small_files)} files with <2 valid rows')

    # Summary
    print(f'\n{"="*60}')
    print(f'CLEANING SUMMARY')
    print(f'{"="*60}')
    print(f'Original:  {len(df)} rows, {df["source_file"].nunique()} files')
    print(f'Cleaned:   {len(df_clean)} rows, {df_clean["source_file"].nunique()} files')
    print(f'Removed:   {len(df) - len(df_clean)} rows ({100*(len(df)-len(df_clean))/len(df):.1f}%)')
    print(f'Remaining jurisdictions: {df_clean["jurisdiction"].nunique()}')

    # Show top jurisdictions in cleaned data
    print(f'\nTop 20 jurisdictions (cleaned):')
    print(df_clean['jurisdiction'].value_counts().head(20).to_string())

    # Save cleaned data
    clean_path = os.path.join(REPORTS_DIR, 'extracted_data_clean.csv')
    df_clean.to_csv(clean_path, index=False)
    print(f'\nSaved cleaned data to: {clean_path}')

    # Step 4: Update database — re-import with cleaned data
    print('\n=== Updating database ===')
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Load download log for filename -> bvd_id mapping
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

    # Also match by name + country
    name_country_to_bvd = {}
    for _, row in log.iterrows():
        key = (str(row['company_name']).upper().strip(), str(row['country_iso']).strip())
        name_country_to_bvd[key] = row['bvd_id']

    def find_bvd_id(row):
        fname = row['source_file']
        if fname in file_to_bvd:
            return file_to_bvd[fname]
        name = str(row['company_name']).upper().strip()
        country = str(row['country_iso']).upper().strip()
        key = (name, country)
        if key in name_country_to_bvd:
            return name_country_to_bvd[key]
        return None

    df_clean['bvd_id'] = df_clean.apply(find_bvd_id, axis=1)
    matched = df_clean['bvd_id'].notna().sum()
    print(f'Matched {matched}/{len(df_clean)} clean rows to firms')

    # Clear existing company_website extracted data and re-import
    # First, get all report_ids for company_website with data_extracted=1
    cur.execute("""
        SELECT report_id FROM reports
        WHERE source = 'company_website' AND data_extracted = 1
    """)
    report_ids = [r[0] for r in cur.fetchall()]
    print(f'Clearing data from {len(report_ids)} existing website reports')

    for rid in report_ids:
        cur.execute("DELETE FROM report_data WHERE report_id = ?", (rid,))

    # Reset data_extracted flag
    cur.execute("""
        UPDATE reports SET data_extracted = 0
        WHERE source = 'company_website'
    """)

    # Re-import cleaned data
    reports_updated = 0
    data_rows_added = 0

    for source_file, group in df_clean.groupby('source_file'):
        bvd_id = group['bvd_id'].iloc[0]
        if pd.isna(bvd_id):
            continue

        years = group['report_year'].dropna()
        year = int(years.mode().iloc[0]) if len(years) > 0 else 0

        cur.execute("""
            SELECT report_id FROM reports
            WHERE bvd_id = ? AND source = 'company_website'
        """, (bvd_id,))
        existing = cur.fetchone()

        if existing:
            report_id = existing[0]
            cur.execute("""
                UPDATE reports SET data_extracted = 1, report_year = CASE WHEN ? > 0 THEN ? ELSE report_year END
                WHERE report_id = ?
            """, (year, year, report_id))
        else:
            cur.execute("""
                INSERT INTO reports (bvd_id, report_year, source, collection_date, data_extracted)
                VALUES (?, ?, 'company_website', ?, 1)
            """, (bvd_id, year, date.today().isoformat()))
            report_id = cur.lastrowid

        reports_updated += 1

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

    # Final totals
    print(f'\nReports updated: {reports_updated}')
    print(f'Data rows added: {data_rows_added}')

    cur.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports")
    print(f'\nTotal firms with reports: {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(*) FROM reports WHERE data_extracted = 1")
    print(f'Reports with data: {cur.fetchone()[0]}')
    cur.execute("SELECT COUNT(*) FROM report_data")
    print(f'Total data rows: {cur.fetchone()[0]}')

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


if __name__ == '__main__':
    main()
