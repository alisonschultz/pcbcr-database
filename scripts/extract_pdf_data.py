"""
Extract CbCR data from downloaded PDF reports.

Uses pdfplumber to find tables with jurisdiction-level tax data.
Identifies CbCR tables by header keywords and country names.
"""

import pdfplumber
import pandas as pd
import os
import re
import json
from datetime import date
from paths import OUTPUT_DIR

REPORTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'collected_reports')

# Header keywords that identify CbCR table columns
HEADER_PATTERNS = {
    'jurisdiction': ['country', 'jurisdiction', 'tax jurisdiction', 'territory', 'region', 'location',
                     'jurisdic', 'land', 'pays', 'paese', 'staat',
                     # Romanian
                     'jurisdicție', 'jurisdictie', 'codul de'],
    'revenue': ['revenue', 'turnover', 'income', 'net banking income', 'total revenue',
                # Romanian
                'venituri', 'cifra de afaceri',
                # German/French/other
                'umsatz', 'chiffre d\'affaires', 'ricavi', 'ingresos'],
    'profit': ['profit', 'loss', 'earnings before tax', 'profit before tax', 'pre-tax', 'pbt',
               # Romanian
               'profit (pierdere)', 'profit brut', 'profit înainte',
               # Other
               'gewinn', 'bénéfice', 'utile', 'beneficio'],
    'tax_paid': ['tax paid', 'taxes paid', 'cash tax', 'income tax paid', 'corporate tax',
                 # Romanian
                 'impozit pe profit plătit', 'impozit pe profit platit', 'plătit în numerar',
                 'platit in numerar'],
    'tax_accrued': ['tax accrued', 'current tax', 'tax charge', 'tax expense', 'income tax expense',
                    # Romanian
                    'impozit pe profit acumulat', 'impozit acumulat'],
    'employees': ['employee', 'staff', 'headcount', 'fte', 'people', 'number of emp',
                  # Romanian
                  'salariați', 'salariati', 'angajați', 'angajati', 'număr de salariat',
                  'numar de salariat',
                  # Other
                  'mitarbeiter', 'employés', 'dipendenti', 'empleados'],
    'tangible_assets': ['tangible asset', 'fixed asset', 'property plant',
                        # Romanian
                        'active corporale', 'imobilizări corporale'],
}

# Known country/jurisdiction names for validation
COUNTRY_NAMES = {
    'australia', 'austria', 'belgium', 'brazil', 'canada', 'china', 'czech republic',
    'denmark', 'finland', 'france', 'germany', 'greece', 'hungary', 'india', 'indonesia',
    'ireland', 'italy', 'japan', 'luxembourg', 'malaysia', 'mexico', 'netherlands',
    'new zealand', 'nigeria', 'norway', 'philippines', 'poland', 'portugal', 'romania',
    'russia', 'singapore', 'south africa', 'south korea', 'spain', 'sweden', 'switzerland',
    'taiwan', 'thailand', 'turkey', 'united kingdom', 'uk', 'united states', 'us', 'usa',
    'vietnam', 'argentina', 'chile', 'colombia', 'egypt', 'israel', 'kenya',
    'morocco', 'pakistan', 'peru', 'qatar', 'saudi arabia', 'uae',
    'united arab emirates', 'azerbaijan', 'angola', 'mozambique', 'trinidad',
    'total', 'other', 'rest of world', 'unallocated', 'group total',
    # Romanian names
    'austria', 'belgia', 'cehia', 'cipru', 'croația', 'croatia', 'danemarca',
    'franta', 'franța', 'germania', 'grecia', 'irlanda', 'letonia', 'lituania',
    'luxemburg', 'malta', 'olanda', 'tarile de jos', 'țările de jos', 'polonia',
    'portugalia', 'spania', 'suedia', 'ungaria', 'norvegia', 'elveția', 'elvetia',
    'rusia', 'turcia', 'malaezia', 'iordania', 'botswana',
    # EU non-cooperative list jurisdictions
    'panama', 'costa rica', 'fiji', 'samoa', 'trinidad and tobago',
    'trinidad și tobago', 'trinidad si tobago', 'hong kong',
    'american samoa', 'anguilla', 'antigua and barbuda', 'guam', 'palau',
    'vanuatu', 'seychelles',
    # ISO codes as jurisdiction names
    'at', 'be', 'bg', 'hr', 'cy', 'cz', 'dk', 'ee', 'fi', 'fr', 'de', 'gr',
    'hu', 'ie', 'it', 'lv', 'lt', 'lu', 'mt', 'nl', 'pl', 'pt', 'ro', 'sk',
    'si', 'es', 'se', 'gb', 'us', 'ch', 'no', 'jp', 'kr', 'cn', 'in', 'br',
    'au', 'ca', 'mx', 'za', 'sg', 'hk', 'tw', 'my', 'th', 'id', 'ph', 'vn',
    'tr', 'ru', 'pa', 'cr', 'tt',
}


def classify_column(header_text):
    """Classify a column header into a known field type."""
    if not header_text:
        return None
    h = str(header_text).lower().strip()
    for field, patterns in HEADER_PATTERNS.items():
        for pattern in patterns:
            if pattern in h:
                return field
    return None


def looks_like_country(text):
    """Check if text looks like a country/jurisdiction name. Strict matching only."""
    if not text:
        return False
    t = str(text).lower().strip()
    if len(t) < 2 or len(t) > 40:
        return False
    # Exact match against known names only — no partial matching
    return t in COUNTRY_NAMES


def parse_number(text):
    """Parse a number from text, handling various formats."""
    if not text:
        return None
    s = str(text).strip()
    if s in ('', '-', '–', '—', 'n/a', 'N/A', 'nil', 'Nil'):
        return None

    # Handle parentheses as negative: (123) -> -123
    negative = False
    if s.startswith('(') and s.endswith(')'):
        s = s[1:-1]
        negative = True
    if s.startswith('−') or s.startswith('–') or s.startswith('-'):
        s = s[1:]
        negative = True

    # Remove currency symbols, spaces, thousand separators
    s = re.sub(r'[€$£¥\s]', '', s)
    s = s.replace(',', '').replace('\u00a0', '')

    # Handle "− 289" style
    s = s.strip()

    try:
        val = float(s)
        return -val if negative else val
    except ValueError:
        return None


CBCR_KEYWORDS = [
    'country-by-country', 'country by country', 'cbcr', 'cbc report',
    'income tax information', 'tax jurisdiction',
    'profit before tax', 'tax paid', 'tax accrued',
    'raport privind informatiile referitoare la impozitul pe profit',
    'impozit pe profit', 'raportare pe',
    'informe sobre el impuesto', 'informe país por país',
    'public cbcr', 'public cbyc',
    'eu directive 2021/2101', 'directive 2013/34',
    'tax transparency report', 'tax contribution report',
]


def find_cbcr_tables(pdf_path):
    """Find and extract CbCR tables from a PDF.
    First verifies the PDF contains CbCR content to avoid extracting
    ESG/sustainability tables with country breakdowns.
    """
    import fitz as pymupdf

    # Pre-filter: check if the PDF actually contains CbCR content
    try:
        doc = pymupdf.open(pdf_path)
        # Check first 10 pages for CbCR keywords
        sample_text = ' '.join(doc[i].get_text().lower() for i in range(min(10, len(doc))))
        doc.close()
        if not any(kw in sample_text for kw in CBCR_KEYWORDS):
            return []  # Not a CbCR report
    except Exception:
        pass  # If fitz fails, continue with pdfplumber

    results = []
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                for table_idx, table in enumerate(tables):
                    if len(table) < 3:  # Need at least header + 2 data rows
                        continue

                    # Try to identify CbCR table
                    cbcr_data = try_parse_cbcr_table(table, page_num + 1, table_idx + 1)
                    if cbcr_data:
                        results.extend(cbcr_data)
    except Exception as e:
        print(f'  Error reading {os.path.basename(pdf_path)}: {e}')

    return results


def try_parse_cbcr_table(table, page_num, table_idx):
    """Try to parse a table as CbCR data."""
    if not table or len(table) < 3:
        return None

    # Find the header row(s) — look for rows with column classification matches
    header_row_idx = None
    col_map = {}

    for row_idx in range(min(4, len(table))):  # Check first 4 rows for headers
        row = table[row_idx]
        classifications = {}
        for col_idx, cell in enumerate(row):
            cls = classify_column(cell)
            if cls:
                classifications[col_idx] = cls

        if len(classifications) >= 2:  # At least 2 recognized columns
            header_row_idx = row_idx
            col_map = classifications
            break

    # If no header found, check if first column has country names (headerless table)
    if header_row_idx is None:
        country_count = sum(1 for row in table[1:6] if looks_like_country(row[0] if row else None))
        if country_count >= 2:
            # Assume first column is jurisdiction, try to infer rest from data
            header_row_idx = 0
            col_map = {0: 'jurisdiction'}
            # Guess numeric columns based on position
            if len(table[0]) >= 4:
                col_map[1] = 'revenue'
                col_map[2] = 'profit'
                col_map[3] = 'tax_paid'
            if len(table[0]) >= 5:
                col_map[4] = 'employees'

    if header_row_idx is None:
        return None

    # Also check if any column has country names but wasn't classified
    if 'jurisdiction' not in col_map.values():
        for col_idx in range(len(table[0])):
            if col_idx in col_map:
                continue
            country_hits = sum(1 for row in table[header_row_idx+1:header_row_idx+6]
                             if len(row) > col_idx and looks_like_country(row[col_idx]))
            if country_hits >= 2:
                col_map[col_idx] = 'jurisdiction'
                break

    # MUST have a jurisdiction column with actual country names + at least one financial column
    has_jurisdiction = 'jurisdiction' in col_map.values()
    has_financial = any(v in ('revenue', 'profit', 'tax_paid', 'tax_accrued', 'employees')
                       for v in col_map.values())

    if not has_jurisdiction or not has_financial:
        return None

    # Validate: check that at least 3 data rows have recognizable country names
    jur_col = [k for k, v in col_map.items() if v == 'jurisdiction'][0]
    country_hits = sum(1 for row in table[header_row_idx+1:min(header_row_idx+10, len(table))]
                       if len(row) > jur_col and looks_like_country(row[jur_col]))
    if country_hits < 2:
        return None

    # Extract data rows
    data_rows = []
    for row_idx in range(header_row_idx + 1, len(table)):
        row = table[row_idx]
        if not row or all(not cell for cell in row):
            continue

        record = {'_page': page_num, '_table': table_idx}

        for col_idx, field in col_map.items():
            if col_idx >= len(row):
                continue
            cell = row[col_idx]

            if field == 'jurisdiction':
                record['jurisdiction'] = str(cell).strip() if cell else None
            else:
                record[field] = parse_number(cell)

        # Skip rows that look like headers/totals without data
        if not record.get('jurisdiction') and not any(
            record.get(f) is not None for f in ['revenue', 'profit', 'tax_paid', 'employees']):
            continue

        data_rows.append(record)

    # Validate: at least 2 rows with data
    valid_rows = [r for r in data_rows if any(r.get(f) is not None
                  for f in ['revenue', 'profit', 'tax_paid', 'employees'])]
    if len(valid_rows) < 2:
        return None

    return valid_rows


def extract_year_from_filename(filename):
    """Extract report year from filename."""
    match = re.search(r'(20[12]\d)', filename)
    return int(match.group(1)) if match else None


def extract_company_info(filename):
    """Extract country and company name from filename."""
    # Format: CC_COMPANY_NAME_YEAR_cbcr.pdf
    parts = filename.replace('_cbcr.pdf', '').split('_', 1)
    country = parts[0] if parts else ''
    name_year = parts[1] if len(parts) > 1 else ''
    # Remove year
    name = re.sub(r'_\d{4}$|_unknown$', '', name_year).replace('_', ' ')
    return country, name


def main():
    print('=== Extracting CbCR Data from PDFs ===\n')

    pdf_files = sorted(f for f in os.listdir(REPORTS_DIR) if f.endswith('.pdf'))
    print(f'PDFs to process: {len(pdf_files)}')

    all_extracted = []
    success = 0
    failed = 0

    for f in pdf_files:
        path = os.path.join(REPORTS_DIR, f)
        country, company = extract_company_info(f)
        year = extract_year_from_filename(f)

        data = find_cbcr_tables(path)

        if data:
            for row in data:
                row['company_name'] = company
                row['country_iso'] = country
                row['report_year'] = year
                row['source_file'] = f
            all_extracted.extend(data)
            success += 1
            jur_count = len([r for r in data if r.get('jurisdiction')])
            print(f'  OK   {f[:60]:60} {len(data)} rows, {jur_count} jurisdictions')
        else:
            failed += 1
            print(f'  MISS {f[:60]:60} no CbCR tables found')

    # Save extracted data
    if all_extracted:
        df = pd.DataFrame(all_extracted)

        # Reorder columns
        col_order = ['company_name', 'country_iso', 'report_year', 'jurisdiction',
                     'revenue', 'profit', 'tax_paid', 'tax_accrued', 'employees',
                     'tangible_assets', 'source_file', '_page', '_table']
        cols = [c for c in col_order if c in df.columns]
        cols += [c for c in df.columns if c not in cols]
        df = df[cols]

        csv_path = os.path.join(REPORTS_DIR, 'extracted_data.csv')
        df.to_csv(csv_path, index=False)

        print(f'\n{"="*60}')
        print(f'EXTRACTION SUMMARY')
        print(f'{"="*60}')
        print(f'PDFs processed: {len(pdf_files)}')
        print(f'Data extracted: {success}')
        print(f'No tables found: {failed}')
        print(f'Total rows: {len(df)}')
        print(f'Unique companies: {df["company_name"].nunique()}')
        if 'jurisdiction' in df.columns:
            print(f'Unique jurisdictions: {df["jurisdiction"].nunique()}')
        print(f'\nSaved to: {csv_path}')

        # Show sample
        print(f'\nSample data:')
        print(df.head(10).to_string())
    else:
        print('\nNo data extracted from any PDF.')

    print('\nDone.')


if __name__ == '__main__':
    main()
