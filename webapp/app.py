"""
Public CbCR Database — Web Application

Flask app serving the unified CbCR dataset with:
- Company search and CbCR data explorer
- Compliance dashboard (in-scope firms vs. reports found)
- Jurisdiction-level data browser
- API endpoints for data access
"""

from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, Response
import sqlite3
import pandas as pd
import json
import os
import csv
import io
from datetime import datetime

# Paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, 'data', 'outputs', 'pcbcr_tracker.db')
UNIFIED_CSV = os.path.join(PROJECT_ROOT, 'data', 'outputs', 'cbcr_unified.csv')
SUBMISSIONS_CSV = os.path.join(PROJECT_ROOT, 'data', 'outputs', 'submitted_reports.csv')

app = Flask(__name__)

REGIME_LABELS = {
    'EU_2021_2101': 'EU Directive 2021/2101',
    'CRD_IV': 'CRD IV (banks)',
    'EU_2021_2101|CRD_IV': 'EU Directive 2021/2101 + CRD IV',
    'EU_2021_2101_VIA_SUBSIDIARY': 'EU Directive (via EU subsidiary)',
    'CRD_IV_VIA_SUBSIDIARY': 'CRD IV (via EU subsidiary)',
    'EU_2021_2101_VIA_SUBSIDIARY|CRD_IV_VIA_SUBSIDIARY': 'EU Directive + CRD IV (via EU subsidiary)',
    'EU_2021_2101_CANDIDATE': 'EU Directive (candidate)',
    'CRD_IV_CANDIDATE': 'CRD IV (candidate)',
    'EU_2021_2101_CANDIDATE|CRD_IV_CANDIDATE': 'EU Directive + CRD IV (candidate)',
    'UNKNOWN': 'Classification pending',
}

SOURCE_LABELS = {
    'company_website': 'Company website',
    'taxplorer': 'EU Tax Observatory (TAXPLORER)',
    'tax_observatory_banks': 'EU Tax Observatory (banks)',
    'user_submission': 'User submission',
    'fair_tax_foundation': 'Fair Tax Foundation',
    'eba_pillar3': 'EBA Pillar 3 Disclosures',
    'cro_ireland': 'CRO Ireland',
}

SOURCE_URLS = {
    'company_website': None,
    'taxplorer': 'https://tax-observatory.eu',
    'tax_observatory_banks': 'https://tax-observatory.eu',
    'fair_tax_foundation': 'https://fairtaxmark.net/resources/public-country-by-country-reporting-why-and-how/',
    'eba_pillar3': 'https://www.eba.europa.eu/risk-analysis-and-data/eu-wide-transparency-exercise',
    'cro_ireland': 'https://cro.ie/publications/document-library/',
    'user_submission': None,
}


REGIME_URLS = {
    'EU_2021_2101': 'https://eur-lex.europa.eu/eli/dir/2021/2101/oj',
    'CRD_IV': 'https://eur-lex.europa.eu/eli/dir/2013/36/oj',
}


def nice_regime(raw):
    """Convert a raw regime string to a nice label."""
    return REGIME_LABELS.get(raw, raw.replace('_', ' ').title())


def regime_url(raw):
    """Return the official EUR-Lex URL for a regime, or None."""
    for key, url in REGIME_URLS.items():
        if key in raw:
            return url
    return None


def nice_source(raw):
    """Convert a raw source string to a nice label."""
    return SOURCE_LABELS.get(raw, raw.replace('_', ' ').title())


def source_url(raw):
    """Return the URL for a data source, or None."""
    return SOURCE_URLS.get(raw)


def regime_badge_class(raw):
    """Return the CSS badge class for a regime."""
    if 'VIA_SUBSIDIARY' in raw or 'CANDIDATE' in raw:
        if 'CRD' in raw and 'EU_2021' not in raw:
            return 'badge-blue-light'
        return 'badge-green-light'
    if 'CRD' in raw and 'EU_2021' not in raw:
        return 'badge-blue'
    if 'EU_2021' in raw:
        return 'badge-green'
    return 'badge-gray'


@app.context_processor
def inject_helpers():
    return dict(nice_regime=nice_regime, nice_source=nice_source,
                regime_url=regime_url, regime_badge_class=regime_badge_class,
                source_url=source_url)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# --- Pages ---

@app.route('/')
def index():
    db = get_db()

    # Key stats — only count firms/reports that actually contain CbCR data
    total_firms = db.execute("SELECT COUNT(*) FROM firms").fetchone()[0]
    in_scope = db.execute("""
        SELECT COUNT(*) FROM firms
        WHERE regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND regime_classification NOT LIKE '%CANDIDATE%'
    """).fetchone()[0]
    with_data = db.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports WHERE data_extracted = 1").fetchone()[0]
    total_reports = db.execute("SELECT COUNT(*) FROM reports WHERE data_extracted = 1").fetchone()[0]
    total_data_rows = db.execute("SELECT COUNT(*) FROM report_data").fetchone()[0]
    data_gap = in_scope - with_data

    # Reports by source — only those with extracted CbCR data
    sources = db.execute("""
        SELECT source, COUNT(DISTINCT bvd_id) as firms, COUNT(*) as reports
        FROM reports WHERE data_extracted = 1 GROUP BY source
    """).fetchall()

    # In-scope by regime
    regimes = db.execute("""
        SELECT regime_classification, COUNT(*) as n
        FROM firms
        WHERE regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND regime_classification NOT LIKE '%CANDIDATE%'
        GROUP BY regime_classification
        ORDER BY n DESC
    """).fetchall()

    # Countries by in-scope firms — only count reports with extracted CbCR data
    countries_with = db.execute("""
        SELECT f.country_iso, COUNT(*) as n,
               SUM(CASE WHEN f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as with_report,
               SUM(CASE WHEN f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as with_data
        FROM firms f
        WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND f.regime_classification NOT LIKE '%CANDIDATE%'
        GROUP BY f.country_iso
        HAVING with_report > 0
        ORDER BY with_report DESC
    """).fetchall()
    countries_without = db.execute("""
        SELECT f.country_iso, COUNT(*) as n,
               SUM(CASE WHEN f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as with_report
        FROM firms f
        WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND f.regime_classification NOT LIKE '%CANDIDATE%'
        GROUP BY f.country_iso
        HAVING with_report = 0
        ORDER BY n DESC
    """).fetchall()

    # Summary stats from report data — per-firm ranges
    report_stats = None
    firm_totals = db.execute("""
        SELECT r.report_id,
               SUM(rd.revenue) as rev, SUM(rd.profit_before_tax) as pbt,
               SUM(rd.tax_paid) as tax, SUM(rd.employees) as emp
        FROM reports r JOIN report_data rd ON r.report_id = rd.report_id
        GROUP BY r.report_id
    """).fetchall()
    yr = db.execute("""
        SELECT MIN(r.report_year), MAX(r.report_year)
        FROM reports r JOIN report_data rd ON r.report_id = rd.report_id
    """).fetchone()
    jur_count = db.execute(
        "SELECT COUNT(DISTINCT jurisdiction_code) FROM report_data"
    ).fetchone()[0]

    if firm_totals:
        import statistics

        def fmt_big(val):
            if val is None:
                return 'n/a'
            abs_val = abs(val)
            if abs_val >= 1e12:
                return f'${val/1e12:,.1f}T'
            if abs_val >= 1e9:
                return f'${val/1e9:,.1f}B'
            if abs_val >= 1e6:
                return f'${val/1e6:,.1f}M'
            return f'${val:,.0f}'

        def fmt_emp(val):
            if val is None:
                return 'n/a'
            if val >= 1e6:
                return f'{val/1e6:,.1f}M'
            return f'{val:,.0f}'

        def range_stats(values, fmt_fn):
            clean = [v for v in values if v is not None and v != 0]
            if not clean:
                return {'min': 'n/a', 'median': 'n/a', 'max': 'n/a'}
            return {
                'min': fmt_fn(min(clean)),
                'median': fmt_fn(statistics.median(clean)),
                'max': fmt_fn(max(clean)),
            }

        revs = range_stats([r['rev'] for r in firm_totals], fmt_big)
        pbts = range_stats([r['pbt'] for r in firm_totals], fmt_big)
        taxes = range_stats([r['tax'] for r in firm_totals], fmt_big)
        emps = range_stats([r['emp'] for r in firm_totals], fmt_emp)

        report_stats = {
            'report_count': len(firm_totals),
            'jurisdictions': jur_count,
            'year_min': yr[0] or '?',
            'year_max': yr[1] or '?',
            'rev_min': revs['min'], 'rev_median': revs['median'], 'rev_max': revs['max'],
            'pbt_min': pbts['min'], 'pbt_median': pbts['median'], 'pbt_max': pbts['max'],
            'tax_min': taxes['min'], 'tax_median': taxes['median'], 'tax_max': taxes['max'],
            'emp_min': emps['min'], 'emp_median': emps['median'], 'emp_max': emps['max'],
        }

    # Profit misalignment chart data
    misalignment_data = None
    try:
        df_unified = pd.read_csv(UNIFIED_CSV)

        # Standardize jurisdiction names
        jur_map = {
            'united states of america': 'United States', 'usa': 'United States',
            'us': 'United States', 'united states': 'United States',
            'united kingdom': 'United Kingdom', 'uk': 'United Kingdom',
            'great britain': 'United Kingdom', 'hong kong': 'Hong Kong',
            'hong kong sar': 'Hong Kong', 'korea, republic of': 'South Korea',
            'south korea': 'South Korea', 'korea': 'South Korea',
            'russian federation': 'Russia', 'danmark': 'Denmark',
            'czech republic': 'Czech Republic', 'czechia': 'Czech Republic',
            'uae': 'UAE', 'united arab emirates': 'UAE',
            'türkiye': 'Turkey', 'turkiye': 'Turkey',
        }
        agg_cats = {'total','group total','consolidated total','grand total','other',
                    'others','other countries','other jurisdictions','rest of world',
                    'rest of the world','row','unallocated','not allocated','intercompany',
                    'eliminations','consolidation adjustments','adjustments','international',
                    'worldwide','global','europe','asia','americas','africa','middle east',
                    'asia pacific','asia-pacific','apac','emea','latam','latin america',
                    'north america','sub-saharan africa','other europe','other asia',
                    'other americas','other africa'}

        # Use all rows with profit data, but only the LATEST year per company
        # to avoid inflating jurisdictions with multi-year data
        df_m = df_unified[df_unified['profit_before_tax'].notna()].copy()

        # Keep only the latest report year per company
        latest_year = df_m.groupby('company_name')['report_year'].max().reset_index()
        latest_year.columns = ['company_name', 'latest_year']
        df_m = df_m.merge(latest_year, on='company_name')
        df_m = df_m[df_m['report_year'] == df_m['latest_year']]

        df_m['jur_clean'] = df_m['jurisdiction_name'].apply(
            lambda x: jur_map.get(str(x).strip().lower(), str(x).strip()) if pd.notna(x) else None)
        df_m = df_m[df_m['jur_clean'].notna()]
        df_m = df_m[~df_m['jur_clean'].str.lower().isin(agg_cats)]

        agg = df_m.groupby('jur_clean').agg(
            revenue=('total_revenues', 'sum'),
            profit=('profit_before_tax', 'sum'),
            employees=('employees', 'sum'),
            tangible_assets=('tangible_assets', 'sum'),
            n_firms=('company_name', 'nunique'),
        ).reset_index()
        agg = agg[agg['n_firms'] >= 3]

        # Profitability per employee in USD (data is in EUR, convert at ~1.10)
        EUR_USD = 1.10
        agg['profit_per_employee'] = agg.apply(
            lambda r: round(r['profit'] / r['employees'] * EUR_USD, 0) if r['employees'] > 0 else None, axis=1)
        # Profit per revenue
        agg['profit_per_revenue'] = agg.apply(
            lambda r: round(r['profit'] / r['revenue'] * 100, 1) if r['revenue'] > 0 else None, axis=1)
        # Profitability per asset
        agg['profit_per_asset'] = agg.apply(
            lambda r: round(r['profit'] / r['tangible_assets'], 2) if r['tangible_assets'] > 0 else None, axis=1)

        top = agg.nlargest(20, 'n_firms')
        misalignment_data = {
            'labels': top['jur_clean'].tolist(),
            'profit_per_employee': top['profit_per_employee'].tolist(),
            'profit_per_revenue': top['profit_per_revenue'].tolist(),
            'profit_per_asset': [v if pd.notna(v) else None for v in top['profit_per_asset']],
            'n_firms': top['n_firms'].tolist(),
            'total_firms': int(df_m['company_name'].nunique()),
        }
    except Exception:
        pass

    db.close()
    return render_template('index.html',
                           total_firms=total_firms,
                           in_scope=in_scope,
                           with_data=with_data,
                           data_gap=data_gap,
                           total_reports=total_reports,
                           total_data_rows=total_data_rows,
                           sources=sources,
                           regimes=regimes,
                           countries_with=countries_with,
                           countries_without=countries_without,
                           report_stats=report_stats,
                           misalignment_data=misalignment_data)


@app.route('/companies')
def companies():
    db = get_db()
    q = request.args.get('q', '').strip()
    regime = request.args.get('regime', '')
    country = request.args.get('country', '')
    has_report = request.args.get('has_report', '')
    page = int(request.args.get('page', 1))
    per_page = 50

    where = ["f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'",
             "f.regime_classification NOT LIKE '%CANDIDATE%'"]
    params = []

    if q:
        where.append("f.company_name LIKE ?")
        params.append(f'%{q}%')
    if regime:
        where.append("f.regime_classification LIKE ?")
        params.append(f'%{regime}%')
    if country:
        where.append("f.country_iso = ?")
        params.append(country)
    if has_report == 'yes':
        where.append("f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1)")
    elif has_report == 'no':
        where.append("f.bvd_id NOT IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1)")

    where_sql = ' AND '.join(where)

    total = db.execute(f"SELECT COUNT(*) FROM firms f WHERE {where_sql}", params).fetchone()[0]

    firms = db.execute(f"""
        SELECT f.bvd_id, f.company_name, f.country_iso, f.regime_classification,
               f.bvd_sector, f.website,
               (SELECT COUNT(*) FROM reports r WHERE r.bvd_id = f.bvd_id AND r.data_extracted = 1) as report_count,
               (SELECT GROUP_CONCAT(DISTINCT report_year ORDER BY report_year) FROM reports r WHERE r.bvd_id = f.bvd_id AND r.data_extracted = 1 AND r.report_year > 0) as report_years
        FROM firms f
        WHERE {where_sql}
        ORDER BY (SELECT COUNT(*) FROM reports r WHERE r.bvd_id = f.bvd_id AND r.data_extracted = 1) DESC, f.company_name
        LIMIT ? OFFSET ?
    """, params + [per_page, (page - 1) * per_page]).fetchall()

    # Get unique countries and regimes for filters
    all_countries = db.execute("""
        SELECT DISTINCT country_iso FROM firms
        WHERE regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND regime_classification NOT LIKE '%CANDIDATE%'
        ORDER BY country_iso
    """).fetchall()

    db.close()
    total_pages = (total + per_page - 1) // per_page
    return render_template('companies.html',
                           firms=firms, total=total, page=page,
                           total_pages=total_pages, q=q, regime=regime,
                           country=country, has_report=has_report,
                           all_countries=all_countries)


@app.route('/company/<bvd_id>')
def company_detail(bvd_id):
    db = get_db()

    firm = db.execute("SELECT * FROM firms WHERE bvd_id = ?", (bvd_id,)).fetchone()
    if not firm:
        return "Company not found", 404

    reports = db.execute("""
        SELECT r.*, COUNT(rd.data_id) as data_rows
        FROM reports r
        LEFT JOIN report_data rd ON r.report_id = rd.report_id
        WHERE r.bvd_id = ?
        GROUP BY r.report_id
        ORDER BY r.report_year DESC
    """, (bvd_id,)).fetchall()

    # Get jurisdiction-level data for all reports
    report_data = {}
    for report in reports:
        data = db.execute("""
            SELECT * FROM report_data
            WHERE report_id = ?
            ORDER BY revenue DESC
        """, (report['report_id'],)).fetchall()
        report_data[report['report_id']] = data

    db.close()
    return render_template('company.html', firm=firm, reports=reports,
                           report_data=report_data)


@app.route('/compliance')
def compliance_redirect():
    return redirect(url_for('data_gap'))


@app.route('/data-gap')
def data_gap():
    db = get_db()

    # Gap by country — split into countries with some data vs none
    gap_with_data = db.execute("""
        SELECT f.country_iso,
               COUNT(*) as total_in_scope,
               SUM(CASE WHEN f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as with_report,
               SUM(CASE WHEN f.bvd_id NOT IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as without_report
        FROM firms f
        WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND f.regime_classification NOT LIKE '%CANDIDATE%'
        GROUP BY f.country_iso
        HAVING with_report > 0
        ORDER BY with_report DESC
    """).fetchall()
    gap_no_data = db.execute("""
        SELECT f.country_iso,
               COUNT(*) as total_in_scope,
               0 as with_report,
               COUNT(*) as without_report
        FROM firms f
        WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND f.regime_classification NOT LIKE '%CANDIDATE%'
        GROUP BY f.country_iso
        HAVING SUM(CASE WHEN f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) = 0
        ORDER BY total_in_scope DESC
    """).fetchall()

    # Gap by regime
    gap_by_regime = db.execute("""
        SELECT f.regime_classification,
               COUNT(*) as total,
               SUM(CASE WHEN f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as with_report
        FROM firms f
        WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND f.regime_classification NOT LIKE '%CANDIDATE%'
        GROUP BY f.regime_classification
        ORDER BY total DESC
    """).fetchall()

    db.close()
    return render_template('compliance.html',
                           gap_with_data=gap_with_data,
                           gap_no_data=gap_no_data,
                           gap_by_regime=gap_by_regime)


@app.route('/submit-report', methods=['GET', 'POST'])
def submit_report():
    success = False
    error = None
    prefill_bvd_id = request.args.get('bvd_id', '')
    prefill_company = request.args.get('company', '')

    if request.method == 'POST':
        company_name = request.form.get('company_name', '').strip()
        report_url = request.form.get('report_url', '').strip()
        report_year = request.form.get('report_year', '').strip()
        bvd_id = request.form.get('bvd_id', '').strip()
        notes = request.form.get('notes', '').strip()
        submitter_email = request.form.get('submitter_email', '').strip()

        if not company_name or not report_url:
            error = 'Please provide both a company name and a link to the report.'
        else:
            # Append to submissions CSV
            file_exists = os.path.exists(SUBMISSIONS_CSV)
            with open(SUBMISSIONS_CSV, 'a', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(['submitted_at', 'company_name', 'bvd_id',
                                     'report_url', 'report_year', 'notes',
                                     'submitter_email', 'status'])
                writer.writerow([
                    datetime.now().isoformat(),
                    company_name, bvd_id, report_url, report_year,
                    notes, submitter_email, 'pending'
                ])
            success = True
            prefill_company = ''
            prefill_bvd_id = ''

    return render_template('submit_report.html',
                           success=success, error=error,
                           prefill_company=prefill_company,
                           prefill_bvd_id=prefill_bvd_id)


# --- API endpoints ---

@app.route('/api/stats')
def api_stats():
    db = get_db()
    stats = {
        'total_firms': db.execute("SELECT COUNT(*) FROM firms").fetchone()[0],
        'in_scope': db.execute("SELECT COUNT(*) FROM firms WHERE regime_classification NOT LIKE '%OUT_OF_SCOPE%' AND regime_classification NOT LIKE '%CANDIDATE%'").fetchone()[0],
        'with_data': db.execute("SELECT COUNT(DISTINCT bvd_id) FROM reports WHERE data_extracted = 1").fetchone()[0],
        'total_reports': db.execute("SELECT COUNT(*) FROM reports WHERE data_extracted = 1").fetchone()[0],
        'total_data_rows': db.execute("SELECT COUNT(*) FROM report_data").fetchone()[0],
    }
    stats['data_gap'] = stats['in_scope'] - stats['with_data']
    db.close()
    return jsonify(stats)


@app.route('/api/company/<bvd_id>')
def api_company(bvd_id):
    db = get_db()
    firm = db.execute("SELECT * FROM firms WHERE bvd_id = ?", (bvd_id,)).fetchone()
    if not firm:
        return jsonify({'error': 'Not found'}), 404

    reports = db.execute("""
        SELECT r.report_id, r.report_year, r.source
        FROM reports r WHERE r.bvd_id = ?
        ORDER BY r.report_year
    """, (bvd_id,)).fetchall()

    data = []
    for r in reports:
        rows = db.execute("SELECT * FROM report_data WHERE report_id = ?",
                          (r['report_id'],)).fetchall()
        data.append({
            'year': r['report_year'],
            'source': r['source'],
            'jurisdictions': [dict(row) for row in rows]
        })

    db.close()
    return jsonify({'firm': dict(firm), 'reports': data})


@app.route('/api/download')
def api_download():
    if os.path.exists(UNIFIED_CSV):
        return send_file(UNIFIED_CSV, as_attachment=True,
                         download_name='pcbcr_unified_data.csv')
    return jsonify({'error': 'Data file not found'}), 404


def rows_to_csv_response(rows, filename):
    """Convert sqlite3.Row results to a CSV download response."""
    if not rows:
        return Response('No data', mimetype='text/plain')
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(rows[0].keys())
    for row in rows:
        writer.writerow(tuple(row))
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


@app.route('/api/download/company/<bvd_id>')
def download_company(bvd_id):
    db = get_db()
    firm = db.execute("SELECT * FROM firms WHERE bvd_id = ?", (bvd_id,)).fetchone()
    name = firm['company_name'].replace(' ', '_')[:40] if firm else bvd_id
    rows = db.execute("""
        SELECT f.company_name, f.country_iso, r.report_year, r.source,
               rd.jurisdiction_code, rd.jurisdiction_name,
               rd.revenue, rd.profit_before_tax, rd.tax_paid,
               rd.tax_accrued, rd.employees, rd.tangible_assets,
               rd.stated_capital, rd.accumulated_earnings, rd.currency
        FROM report_data rd
        JOIN reports r ON rd.report_id = r.report_id
        JOIN firms f ON r.bvd_id = f.bvd_id
        WHERE r.bvd_id = ?
        ORDER BY r.report_year, rd.jurisdiction_name
    """, (bvd_id,)).fetchall()
    db.close()
    return rows_to_csv_response(rows, f'{name}_cbcr_data.csv')


@app.route('/api/download/companies')
def download_companies():
    db = get_db()
    q = request.args.get('q', '').strip()
    regime = request.args.get('regime', '')
    country = request.args.get('country', '')
    has_report = request.args.get('has_report', '')

    where = ["f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'",
             "f.regime_classification NOT LIKE '%CANDIDATE%'"]
    params = []
    if q:
        where.append("f.company_name LIKE ?")
        params.append(f'%{q}%')
    if regime:
        where.append("f.regime_classification LIKE ?")
        params.append(f'%{regime}%')
    if country:
        where.append("f.country_iso = ?")
        params.append(country)
    if has_report == 'yes':
        where.append("f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1)")
    elif has_report == 'no':
        where.append("f.bvd_id NOT IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1)")

    where_sql = ' AND '.join(where)
    rows = db.execute(f"""
        SELECT f.bvd_id, f.company_name, f.country_iso, f.regime_classification,
               f.bvd_sector, f.website,
               (SELECT COUNT(*) FROM reports r WHERE r.bvd_id = f.bvd_id AND r.data_extracted = 1) as report_count,
               (SELECT GROUP_CONCAT(DISTINCT report_year ORDER BY report_year) FROM reports r WHERE r.bvd_id = f.bvd_id AND r.data_extracted = 1 AND r.report_year > 0) as report_years
        FROM firms f
        WHERE {where_sql}
        ORDER BY (SELECT COUNT(*) FROM reports r WHERE r.bvd_id = f.bvd_id AND r.data_extracted = 1) DESC, f.company_name
    """, params).fetchall()
    db.close()
    return rows_to_csv_response(rows, 'companies_list.csv')


@app.route('/api/download/data-gap')
def download_data_gap():
    db = get_db()
    rows = db.execute("""
        SELECT f.country_iso,
               COUNT(*) as total_in_scope,
               SUM(CASE WHEN f.bvd_id IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as with_report,
               SUM(CASE WHEN f.bvd_id NOT IN (SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1) THEN 1 ELSE 0 END) as without_report
        FROM firms f
        WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND f.regime_classification NOT LIKE '%CANDIDATE%'
        GROUP BY f.country_iso
        ORDER BY total_in_scope DESC
    """).fetchall()
    db.close()
    return rows_to_csv_response(rows, 'data_gap_by_country.csv')


if __name__ == '__main__':
    print(f'Database: {DB_PATH}')
    print(f'Data: {UNIFIED_CSV}')
    app.run(debug=True, port=5000)
