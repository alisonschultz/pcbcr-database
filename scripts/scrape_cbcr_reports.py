"""
Search company websites for public CbCR / GRI 207-4 reports.

Strategy:
1. For each in-scope firm with a website URL, check common paths
   (e.g., /sustainability, /investors, /tax) for CbCR keywords
2. Search for PDF links containing CbCR-related terms
3. Record findings in a results CSV for manual review

Supports resume — skips firms already checked.

Usage: python scrape_cbcr_reports.py [--limit 100] [--country NL]
"""

import pandas as pd
import requests
import os
import re
import time
import argparse
from urllib.parse import urljoin, urlparse
from paths import OUTPUT_DIR, REGISTERS_DIR

RESULTS_PATH = os.path.join(REGISTERS_DIR, 'website_cbcr_search.csv')

# Keywords that suggest CbCR content (broader set to catch embedded disclosures)
CBCR_KEYWORDS = [
    # Direct CbCR terms
    'country-by-country', 'country by country', 'cbcr', 'cbc report',
    'public reporting directive', '2021/2101',
    # Tax transparency reports
    'tax transparency', 'tax report', 'tax contribution', 'tax strategy',
    'total tax contribution', 'taxes paid by country', 'tax payments by country',
    'tax per country', 'tax footprint',
    # GRI 207
    'gri 207', 'gri207', 'gri-207',
    # Sustainability / annual report sections
    'tax governance', 'approach to tax', 'responsible tax',
    'tax policy', 'our tax approach',
    # French
    'impôt par pays', 'rapport pays par pays', 'transparence fiscale',
    # German
    'länderbezogene berichterstattung', 'steuerliche transparenz',
    'steuertransparenz', 'steuerbericht',
    # Spanish
    'información país por país', 'transparencia fiscal',
    'informe fiscal', 'contribución fiscal',
    # Italian
    'rendicontazione paese per paese', 'trasparenza fiscale',
    # Extractive industries / EITI
    'payment to governments', 'payments to governments', 'eiti report',
    'extractive industries transparency',
    # Common report titles
    'tax contribution report', 'taxes paid report',
    'taxes paid by jurisdiction', 'taxes paid by country',
    'tax by country', 'tax by jurisdiction',
    'economic contribution', 'constituent entities',
    'effective tax rate by jurisdiction', 'effective tax rate by country',
    'profit allocation',
    # Pillar Two / GloBE (emerging)
    'globe information return', 'pillar two', 'jurisdictional effective tax',
    # Dutch
    'land-voor-land', 'fiscale transparantie', 'belastingbeleid',
    # Swedish/Nordic
    'skattepolicy', 'skattetransparens', 'land för land',
]

# Common website paths where tax/sustainability reports live
SEARCH_PATHS = [
    '/',
    '/sustainability',
    '/sustainability/reports',
    '/sustainability/governance',
    '/corporate-responsibility',
    '/csr',
    '/esg',
    '/investors',
    '/investor-relations',
    '/annual-report',
    '/tax',
    '/tax-strategy',
    '/tax-transparency',
    '/governance',
    '/about/governance',
    '/publications',
    '/reports',
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TJN-CbCR-Research/1.0',
    'Accept': 'text/html,application/xhtml+xml',
    'Accept-Language': 'en-US,en;q=0.9',
}


def search_page_for_cbcr(url, timeout=15):
    """Fetch a page and search for CbCR-related keywords and PDF links."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout,
                            allow_redirects=True, verify=False)
        if resp.status_code != 200:
            return None

        content_type = resp.headers.get('content-type', '')
        if 'text/html' not in content_type:
            return None

        text = resp.text.lower()

        # Search for CbCR keywords
        found_keywords = []
        for kw in CBCR_KEYWORDS:
            if kw in text:
                found_keywords.append(kw)

        # Search for PDF links that might contain CbCR data
        pdf_links = []
        PDF_LINK_KEYWORDS = [
            'country-by-country', 'cbcr', 'tax-transparency', 'tax-report',
            'tax-contribution', 'gri-207', 'gri207',
            # Broader: reports that often embed CbCR
            'tax-strategy', 'tax_transparency', 'taxtransparency',
            'sustainability-report', 'sustainability_report',
            'annual-report', 'annual_report', 'integrated-report',
            'esg-report', 'csr-report', 'responsible-tax',
            'total-tax', 'totaltax', 'steuerbericht', 'fiscale',
            # Extractive / payments
            'payment-to-government', 'payments-to-government', 'eiti',
            'taxes-paid', 'tax-data', 'tax-contribution-report',
        ]
        pdf_pattern = re.compile(r'href=["\']([^"\']*\.pdf)["\']', re.IGNORECASE)
        for match in pdf_pattern.finditer(resp.text):
            link = match.group(1)
            link_lower = link.lower()
            if any(kw in link_lower for kw in PDF_LINK_KEYWORDS):
                full_url = urljoin(url, link)
                pdf_links.append(full_url)

        if found_keywords or pdf_links:
            return {
                'keywords': found_keywords,
                'pdf_links': pdf_links,
                'url': url,
            }

    except requests.exceptions.SSLError:
        # Retry without SSL verification (some corporate sites have cert issues)
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout,
                                allow_redirects=True, verify=False)
            if resp.status_code == 200 and 'text/html' in resp.headers.get('content-type', ''):
                text = resp.text.lower()
                found_keywords = [kw for kw in CBCR_KEYWORDS if kw in text]
                if found_keywords:
                    return {'keywords': found_keywords, 'pdf_links': [], 'url': url}
        except Exception:
            pass
    except Exception:
        pass

    return None


LINK_KEYWORDS = ['tax', 'sustainab', 'transparen', 'cbcr', 'country',
                  'esg', 'governance', 'responsib', 'report', 'gri',
                  'annual', 'investor', 'download', 'fiscal', 'steuer',
                  'impot', 'belasting', 'skatt']


def search_company_website(website, company_name):
    """Search a company's website for CbCR content.

    Two-level approach:
    1. Check seed pages (/, /sustainability, /tax) for keywords and relevant links
    2. Follow up to 5 relevant internal links one level deep
    """
    if pd.isna(website) or not website:
        return {'status': 'no_website'}

    website = str(website).strip()
    if not website.startswith('http'):
        website = 'https://' + website

    parsed = urlparse(website)
    base_url = f'{parsed.scheme}://{parsed.netloc}'

    all_keywords = set()
    all_pdfs = []
    pages_checked = 0
    pages_with_hits = []
    relevant_links = set()

    # Level 1: seed pages
    seeds = [base_url, base_url + '/sustainability', base_url + '/tax']
    for url in seeds:
        result = search_page_for_cbcr(url)
        pages_checked += 1
        if result:
            all_keywords.update(result['keywords'])
            all_pdfs.extend(result['pdf_links'])
            pages_with_hits.append(url)

        # Also harvest internal links with relevant keywords
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10,
                                allow_redirects=True, verify=False)
            if resp.status_code == 200 and 'text/html' in resp.headers.get('content-type', ''):
                for match in re.finditer(r'href=["\']([^"\'>#]+)["\']', resp.text):
                    href = match.group(1)
                    if any(k in href.lower() for k in LINK_KEYWORDS):
                        full = urljoin(url, href)
                        if parsed.netloc in full and full not in seeds:
                            relevant_links.add(full)
        except Exception:
            pass
        time.sleep(0.2)

        if all_pdfs:  # Found PDF links, good enough
            break

    # Level 2: follow relevant internal links (max 5)
    if not all_pdfs:
        for url in list(relevant_links)[:5]:
            result = search_page_for_cbcr(url)
            pages_checked += 1
            if result:
                all_keywords.update(result['keywords'])
                all_pdfs.extend(result['pdf_links'])
                pages_with_hits.append(url)
            time.sleep(0.2)
            if all_pdfs:
                break

    if all_keywords or all_pdfs:
        return {
            'status': 'found',
            'keywords': ', '.join(sorted(all_keywords)),
            'pdf_links': ' | '.join(all_pdfs[:5]),
            'pages_with_hits': ' | '.join(pages_with_hits),
            'pages_checked': pages_checked,
        }

    return {
        'status': 'not_found',
        'pages_checked': pages_checked,
    }


def load_results():
    """Load existing results for resume support."""
    if os.path.exists(RESULTS_PATH):
        return pd.read_csv(RESULTS_PATH)
    return pd.DataFrame()


def save_results(df):
    """Save results to CSV."""
    df.to_csv(RESULTS_PATH, index=False)


def main():
    import warnings
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    parser = argparse.ArgumentParser()
    parser.add_argument('--limit', type=int, default=0, help='Max firms to check (0=all)')
    parser.add_argument('--country', type=str, help='Only check this country')
    args = parser.parse_args()

    print('=== CbCR Website Scraper ===\n')

    # Load in-scope firms without reports
    df_master = pd.read_csv(os.path.join(OUTPUT_DIR, 'master_firm_list.csv'),
                            usecols=['bvd_id', 'company_name', 'country_iso', 'website',
                                     'regime_classification'])
    inscope = df_master[
        ~df_master['regime_classification'].str.contains('OUT_OF_SCOPE|CANDIDATE', na=True, regex=True)
    ].copy()

    # Filter to firms with websites
    inscope = inscope[inscope['website'].notna()].copy()

    if args.country:
        inscope = inscope[inscope['country_iso'] == args.country]

    print(f'Firms to search: {len(inscope)}')

    # Load existing results and skip already-checked firms
    df_results = load_results()
    if len(df_results) > 0:
        already_checked = set(df_results['bvd_id'])
        inscope = inscope[~inscope['bvd_id'].isin(already_checked)]
        print(f'Already checked: {len(already_checked)}')
        print(f'Remaining: {len(inscope)}')

    if args.limit > 0:
        inscope = inscope.head(args.limit)
        print(f'Limited to: {len(inscope)}')

    # Search websites
    new_results = []
    found_count = 0

    for i, (_, firm) in enumerate(inscope.iterrows()):
        result = search_company_website(firm['website'], firm['company_name'])

        row = {
            'bvd_id': firm['bvd_id'],
            'company_name': firm['company_name'],
            'country_iso': firm['country_iso'],
            'website': firm['website'],
            'status': result['status'],
            'keywords': result.get('keywords', ''),
            'pdf_links': result.get('pdf_links', ''),
            'pages_with_hits': result.get('pages_with_hits', ''),
            'pages_checked': result.get('pages_checked', 0),
            'search_date': pd.Timestamp.now().strftime('%Y-%m-%d'),
        }
        new_results.append(row)

        if result['status'] == 'found':
            found_count += 1
            print(f'  [{i+1}] FOUND: {firm["company_name"]} ({firm["country_iso"]}) — {result.get("keywords", "")[:60]}')

        # Save every 25 firms
        if len(new_results) % 25 == 0:
            df_batch = pd.DataFrame(new_results)
            df_results = pd.concat([df_results, df_batch], ignore_index=True)
            save_results(df_results)
            new_results = []
            print(f'  Progress: {i+1}/{len(inscope)} checked, {found_count} found')

    # Save final batch
    if new_results:
        df_batch = pd.DataFrame(new_results)
        df_results = pd.concat([df_results, df_batch], ignore_index=True)
        save_results(df_results)

    # Summary
    print(f'\n{"="*60}')
    print(f'WEBSITE SEARCH RESULTS')
    print(f'{"="*60}')
    print(f'Firms checked: {len(inscope)}')
    print(f'CbCR content found: {found_count}')

    if len(df_results) > 0:
        print(f'\nAll results ({len(df_results)} total):')
        print(df_results['status'].value_counts().to_string())

        found = df_results[df_results['status'] == 'found']
        if len(found) > 0:
            print(f'\nFirms with CbCR content ({len(found)}):')
            print(f'By country: {found["country_iso"].value_counts().head(10).to_dict()}')
            print(f'\nTop keyword matches:')
            all_kw = ', '.join(found['keywords'].dropna()).split(', ')
            from collections import Counter
            for kw, count in Counter(all_kw).most_common(10):
                if kw:
                    print(f'  {kw}: {count}')

    print(f'\nResults saved to: {RESULTS_PATH}')
    print('Done.')


if __name__ == '__main__':
    main()
