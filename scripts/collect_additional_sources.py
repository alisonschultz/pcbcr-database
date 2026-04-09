"""
Collect CbCR reports from additional public sources beyond company websites.

Sources:
1. Fair Tax Foundation — analysed voluntary CbCR disclosures
2. GRI Sustainability Disclosure Database — companies reporting under GRI 207 (Tax)
3. EITI — Extractive Industries Transparency Initiative reports
4. PwC EU pCbCR Tracker — tracks early/voluntary filers
5. ECB/EBA — Pillar 3 bank disclosures with CbCR data
6. Google Scholar / targeted web search for CbCR PDFs
"""

import requests
import pandas as pd
import os
import re
import time
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from paths import OUTPUT_DIR, TAXOBS_DIR

RESULTS_DIR = os.path.join(os.path.dirname(OUTPUT_DIR), 'additional_sources')
os.makedirs(RESULTS_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) TJN-CbCR-Research/1.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
}


def fetch(url, timeout=20):
    """Fetch a URL with error handling."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=timeout,
                            allow_redirects=True, verify=False)
        if resp.status_code == 200:
            return resp
    except Exception as e:
        print(f'  Error fetching {url}: {e}')
    return None


# === 1. Fair Tax Foundation ===

def collect_fair_tax():
    """Scrape Fair Tax Foundation for companies with public CbCR analysis."""
    print('\n=== Fair Tax Foundation ===')
    results = []

    urls = [
        'https://fairtaxmark.net/resources/',
        'https://fairtaxmark.net/the-fair-tax-mark/',
        'https://fairtaxmark.net/resources/public-country-by-country-reporting-why-and-how/',
    ]

    for url in urls:
        resp = fetch(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')

        # Look for links to CbCR analyses or company lists
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True).lower()
            if any(kw in text or kw in href.lower() for kw in
                   ['country-by-country', 'cbcr', 'tax transparency', 'country by country']):
                full_url = urljoin(url, href)
                results.append({
                    'source': 'fair_tax_foundation',
                    'url': full_url,
                    'title': link.get_text(strip=True),
                    'found_on': url,
                })

        # Also look for PDF links
        for link in soup.find_all('a', href=re.compile(r'\.pdf$', re.I)):
            href = link['href']
            full_url = urljoin(url, href)
            results.append({
                'source': 'fair_tax_foundation',
                'url': full_url,
                'title': link.get_text(strip=True),
                'found_on': url,
            })

    print(f'  Found {len(results)} links')
    return results


# === 2. GRI Database (GRI 207 Tax Standard) ===

def collect_gri207():
    """Search for companies reporting under GRI 207 Tax standard."""
    print('\n=== GRI 207 Tax Standard ===')
    results = []

    # GRI Sustainability Disclosure Database
    gri_urls = [
        'https://www.globalreporting.org/how-to-use-the-gri-standards/gri-standards-english-language/',
        'https://www.globalreporting.org/standards/standards-development/topic-standard-project-for-tax/',
    ]

    for url in gri_urls:
        resp = fetch(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            text = link.get_text(strip=True).lower()
            href = link['href'].lower()
            if '207' in text or 'tax' in text or '207' in href:
                full_url = urljoin(url, link['href'])
                results.append({
                    'source': 'gri_207',
                    'url': full_url,
                    'title': link.get_text(strip=True),
                    'found_on': url,
                })

    # Also search for GRI 207-4 reports via targeted web queries
    search_queries = [
        'GRI 207-4 "country-by-country" filetype:pdf',
        'GRI 207 "tax paid" "jurisdiction" filetype:pdf',
    ]

    for query in search_queries:
        try:
            search_url = f'https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}'
            resp = fetch(search_url)
            if resp:
                soup = BeautifulSoup(resp.text, 'html.parser')
                for result in soup.select('.result__a'):
                    href = result.get('href', '')
                    # DuckDuckGo wraps URLs
                    url_match = re.search(r'uddg=([^&]+)', href)
                    if url_match:
                        actual_url = requests.utils.unquote(url_match.group(1))
                        results.append({
                            'source': 'gri_207_search',
                            'url': actual_url,
                            'title': result.get_text(strip=True),
                            'found_on': f'duckduckgo: {query}',
                        })
            time.sleep(2)
        except Exception as e:
            print(f'  Search error: {e}')

    print(f'  Found {len(results)} links')
    return results


# === 3. EITI ===

def collect_eiti():
    """Collect data from EITI (Extractive Industries Transparency Initiative)."""
    print('\n=== EITI ===')
    results = []

    # EITI data portal
    eiti_urls = [
        'https://eiti.org/data',
        'https://eiti.org/collections/eiti-reports',
        'https://eiti.org/other/eiti-summary-data',
    ]

    for url in eiti_urls:
        resp = fetch(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True).lower()
            if any(kw in text or kw in href.lower() for kw in
                   ['report', 'data', 'payment', 'download', 'country']):
                full_url = urljoin(url, href)
                if full_url.endswith(('.pdf', '.csv', '.xlsx', '.xls')):
                    results.append({
                        'source': 'eiti',
                        'url': full_url,
                        'title': link.get_text(strip=True),
                        'found_on': url,
                    })

    # EITI API for summary data
    try:
        api_url = 'https://eiti.org/api/v1.0/organisation'
        resp = fetch(api_url)
        if resp:
            data = resp.json()
            if 'data' in data:
                for item in data['data'][:50]:
                    results.append({
                        'source': 'eiti_api',
                        'url': item.get('url', ''),
                        'title': item.get('label', ''),
                        'found_on': api_url,
                    })
    except Exception as e:
        print(f'  EITI API error: {e}')

    print(f'  Found {len(results)} links')
    return results


# === 4. PwC EU pCbCR Tracker ===

def collect_pwc_tracker():
    """Scrape PwC's EU public CbCR reporting tracker."""
    print('\n=== PwC EU pCbCR Tracker ===')
    results = []

    url = 'https://www.pwc.com/gx/en/services/tax/eu-pcbcr-reporting-tracker.html'
    resp = fetch(url)
    if resp:
        soup = BeautifulSoup(resp.text, 'html.parser')

        # Look for company names, data tables, or downloadable resources
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True)
            if any(kw in href.lower() or kw in text.lower() for kw in
                   ['cbcr', 'country-by-country', 'tracker', 'download', 'report', 'pdf']):
                full_url = urljoin(url, href)
                results.append({
                    'source': 'pwc_tracker',
                    'url': full_url,
                    'title': text,
                    'found_on': url,
                })

        # Also check for embedded data in tables
        tables = soup.find_all('table')
        for table in tables:
            rows = table.find_all('tr')
            for row in rows:
                cells = row.find_all(['td', 'th'])
                if len(cells) >= 2:
                    results.append({
                        'source': 'pwc_tracker_table',
                        'url': url,
                        'title': ' | '.join(c.get_text(strip=True) for c in cells[:3]),
                        'found_on': url,
                    })

    print(f'  Found {len(results)} entries')
    return results


# === 5. ECB/EBA Bank Pillar 3 Disclosures ===

def collect_eba_pillar3():
    """Collect CbCR data from EBA Pillar 3 disclosures."""
    print('\n=== EBA Pillar 3 Disclosures ===')
    results = []

    # EBA's transparency exercise data
    eba_urls = [
        'https://www.eba.europa.eu/risk-analysis-and-data/eu-wide-transparency-exercise',
        'https://www.eba.europa.eu/risk-analysis-and-data/eu-wide-transparency-exercise/results',
    ]

    for url in eba_urls:
        resp = fetch(url)
        if not resp:
            continue

        soup = BeautifulSoup(resp.text, 'html.parser')
        for link in soup.find_all('a', href=True):
            href = link['href']
            text = link.get_text(strip=True).lower()
            if any(kw in text or kw in href.lower() for kw in
                   ['download', 'data', 'transparency', 'result', 'pillar', 'csv', 'xlsx']):
                full_url = urljoin(url, href)
                results.append({
                    'source': 'eba_pillar3',
                    'url': full_url,
                    'title': link.get_text(strip=True),
                    'found_on': url,
                })

    print(f'  Found {len(results)} entries')
    return results


# === 6. Targeted web search for CbCR PDFs ===

def collect_web_search():
    """Run targeted web searches for CbCR reports."""
    print('\n=== Targeted web searches ===')
    results = []

    queries = [
        '"country-by-country report" "tax paid" filetype:pdf',
        '"public country-by-country reporting" filetype:pdf 2024',
        '"Directive 2021/2101" "country-by-country" filetype:pdf',
        '"tax transparency report" "jurisdiction" "revenue" filetype:pdf',
        '"GRI 207-4" "tax jurisdiction" filetype:pdf',
        '"ertragsteuerinformationsbericht" filetype:pdf',
        '"rapport pays par pays" "impôt" filetype:pdf',
        '"informe país por país" filetype:pdf',
        '"Pillar 3" "country-by-country" filetype:pdf',
        '"income tax paid" "by jurisdiction" "revenue" filetype:pdf',
        '"total tax contribution" "by country" filetype:pdf 2024',
        '"country by country" "profit before tax" "employees" filetype:pdf',
    ]

    for query in queries:
        try:
            search_url = f'https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}'
            resp = fetch(search_url)
            if resp:
                soup = BeautifulSoup(resp.text, 'html.parser')
                for result_el in soup.select('.result__a'):
                    href = result_el.get('href', '')
                    url_match = re.search(r'uddg=([^&]+)', href)
                    if url_match:
                        actual_url = requests.utils.unquote(url_match.group(1))
                        results.append({
                            'source': 'web_search',
                            'url': actual_url,
                            'title': result_el.get_text(strip=True),
                            'found_on': f'search: {query[:60]}',
                        })
                print(f'  Query "{query[:50]}..." → {len(soup.select(".result__a"))} results')
            time.sleep(3)  # Be polite
        except Exception as e:
            print(f'  Search error: {e}')

    print(f'  Total: {len(results)} links')
    return results


# === 7. National business registries (BRIS) ===

def collect_bris():
    """Check European Business Registry Interconnection System."""
    print('\n=== BRIS / e-Justice Portal ===')
    results = []

    url = 'https://e-justice.europa.eu/489/EN/business_registers__search_for_a_company_in_the_eu'
    resp = fetch(url)
    if resp:
        soup = BeautifulSoup(resp.text, 'html.parser')
        # Look for any links to national registries
        for link in soup.find_all('a', href=True):
            text = link.get_text(strip=True).lower()
            href = link['href']
            if any(kw in text for kw in ['register', 'registry', 'company']):
                full_url = urljoin(url, href)
                results.append({
                    'source': 'bris',
                    'url': full_url,
                    'title': link.get_text(strip=True),
                    'found_on': url,
                })

    print(f'  Found {len(results)} registry links')
    return results


def main():
    import warnings
    warnings.filterwarnings('ignore', message='Unverified HTTPS request')

    print('='*60)
    print('COLLECTING CbCR REPORTS FROM ADDITIONAL SOURCES')
    print('='*60)

    all_results = []

    # Run all collectors
    all_results.extend(collect_fair_tax())
    all_results.extend(collect_gri207())
    all_results.extend(collect_eiti())
    all_results.extend(collect_pwc_tracker())
    all_results.extend(collect_eba_pillar3())
    all_results.extend(collect_web_search())
    all_results.extend(collect_bris())

    # Deduplicate by URL
    seen_urls = set()
    unique_results = []
    for r in all_results:
        url = r.get('url', '')
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(r)

    # Save results
    df = pd.DataFrame(unique_results)
    output_path = os.path.join(RESULTS_DIR, 'additional_sources.csv')
    df.to_csv(output_path, index=False)

    print(f'\n{"="*60}')
    print(f'SUMMARY')
    print(f'{"="*60}')
    print(f'Total links found: {len(unique_results)}')
    if len(df) > 0:
        print(f'\nBy source:')
        print(df['source'].value_counts().to_string())
        pdf_links = df[df['url'].str.contains('.pdf', case=False, na=False)]
        print(f'\nPDF links: {len(pdf_links)}')
    print(f'\nSaved to: {output_path}')
    print('Done.')


if __name__ == '__main__':
    main()
