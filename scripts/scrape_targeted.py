"""
Targeted CbCR search: site-specific Google/DuckDuckGo searches for in-scope firms.

More precise than the broad website scraper — searches for exact CbCR terms
on each company's domain using search engine queries like:
  site:shell.com "country-by-country"

This finds CbCR reports buried deep in company websites that the
page-by-page crawler missed.
"""

import pandas as pd
import requests
import os
import re
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin, quote
from paths import OUTPUT_DIR, REGISTERS_DIR

RESULTS_PATH = os.path.join(REGISTERS_DIR, 'targeted_cbcr_search.csv')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TJN-CbCR-Research/1.0',
    'Accept': 'text/html,application/xhtml+xml',
}

# Highly specific CbCR queries — one of these per domain
SEARCH_TEMPLATES = [
    'site:{domain} "country-by-country" OR "country by country" filetype:pdf',
    'site:{domain} "tax transparency report" filetype:pdf',
    'site:{domain} "GRI 207" OR "gri207" filetype:pdf',
    'site:{domain} "Directive 2021/2101"',
    'site:{domain} "ertragsteuerinformationsbericht" OR "länderbezogen" filetype:pdf',
    'site:{domain} "rapport pays par pays" OR "pays par pays" filetype:pdf',
    'site:{domain} "informe país por país" filetype:pdf',
    'site:{domain} "tax paid" "jurisdiction" filetype:pdf',
]


def search_duckduckgo(query):
    """Search DuckDuckGo and return result URLs."""
    try:
        url = f'https://html.duckduckgo.com/html/?q={quote(query)}'
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        results = []
        for link in soup.select('.result__a'):
            href = link.get('href', '')
            url_match = re.search(r'uddg=([^&]+)', href)
            if url_match:
                actual_url = requests.utils.unquote(url_match.group(1))
                results.append({
                    'url': actual_url,
                    'title': link.get_text(strip=True),
                })
        return results
    except Exception:
        return []


def main():
    import warnings
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')
    import sqlite3

    print('=== Targeted CbCR Search ===\n')

    # Load in-scope firms WITHOUT reports that have extracted data
    db_path = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Get firms that are in-scope but have no extracted data
    firms = conn.execute("""
        SELECT f.bvd_id, f.company_name, f.country_iso, f.website
        FROM firms f
        WHERE f.regime_classification NOT LIKE '%OUT_OF_SCOPE%'
          AND f.regime_classification NOT LIKE '%CANDIDATE%'
          AND f.website IS NOT NULL
          AND f.website != ''
          AND f.bvd_id NOT IN (
              SELECT DISTINCT bvd_id FROM reports WHERE data_extracted = 1
          )
        ORDER BY f.company_name
    """).fetchall()
    conn.close()

    print(f'Firms to search (in-scope, no extracted data, with website): {len(firms)}')

    # Load existing results
    if os.path.exists(RESULTS_PATH):
        df_existing = pd.read_csv(RESULTS_PATH)
        already_searched = set(df_existing['bvd_id'])
        print(f'Already searched: {len(already_searched)}')
    else:
        df_existing = pd.DataFrame()
        already_searched = set()

    firms = [f for f in firms if f['bvd_id'] not in already_searched]
    print(f'Remaining: {len(firms)}')

    new_results = []
    found_count = 0

    for i, firm in enumerate(firms):
        domain = str(firm['website']).strip()
        if not domain:
            continue
        # Clean domain
        domain = re.sub(r'^https?://', '', domain)
        domain = re.sub(r'/.*$', '', domain)

        found_urls = []
        matched_query = None

        for template in SEARCH_TEMPLATES:
            query = template.format(domain=domain)
            results = search_duckduckgo(query)

            pdf_results = [r for r in results if '.pdf' in r['url'].lower()]
            if pdf_results:
                found_urls = pdf_results
                matched_query = query
                break

            if results:
                found_urls = results
                matched_query = query
                # Don't break — keep looking for PDFs

            time.sleep(1.5)  # Rate limit

        row = {
            'bvd_id': firm['bvd_id'],
            'company_name': firm['company_name'],
            'country_iso': firm['country_iso'],
            'website': firm['website'],
            'status': 'found' if found_urls else 'not_found',
            'pdf_urls': ' | '.join(r['url'] for r in found_urls[:5] if '.pdf' in r['url'].lower()),
            'all_urls': ' | '.join(r['url'] for r in found_urls[:5]),
            'titles': ' | '.join(r['title'] for r in found_urls[:3]),
            'matched_query': matched_query or '',
            'search_date': pd.Timestamp.now().strftime('%Y-%m-%d'),
        }
        new_results.append(row)

        if found_urls:
            found_count += 1
            pdf_count = len([r for r in found_urls if '.pdf' in r['url'].lower()])
            print(f'  [{i+1}] FOUND: {firm["company_name"]} ({firm["country_iso"]}) '
                  f'— {pdf_count} PDFs, {len(found_urls)} total links')

        # Save every 20 firms
        if len(new_results) % 20 == 0 and new_results:
            df_batch = pd.DataFrame(new_results)
            df_all = pd.concat([df_existing, df_batch], ignore_index=True)
            df_all.to_csv(RESULTS_PATH, index=False)
            new_results = []
            print(f'  Progress: {i+1}/{len(firms)} searched, {found_count} found')

        # Stop after first batch if testing
        if i >= 500:
            print(f'\n  Stopping after 500 firms (use --all to search all)')
            break

    # Save final batch
    if new_results:
        df_batch = pd.DataFrame(new_results)
        df_all = pd.concat([df_existing, df_batch], ignore_index=True)
        df_all.to_csv(RESULTS_PATH, index=False)

    print(f'\n{"="*60}')
    print(f'SEARCH RESULTS')
    print(f'{"="*60}')
    print(f'Firms searched: {min(i+1, len(firms))}')
    print(f'CbCR content found: {found_count}')
    print(f'Results saved to: {RESULTS_PATH}')
    print('Done.')


if __name__ == '__main__':
    main()
