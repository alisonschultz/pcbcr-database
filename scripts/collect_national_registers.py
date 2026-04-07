"""
Collect public CbCR filings from EU national business registers.

IMPORTANT TIMELINE:
- Directive 2021/2101 applies to fiscal years starting on/after 22 June 2024
- First filings due by 31 December 2026 (for calendar-year FY2025 companies)
- iXBRL format mandatory from FY starting 1 Jan 2025
- As of March 2026, most registers may not yet have CbCR filings

This script provides a framework for checking registers as filings become available.
It currently focuses on countries with the best API access.
"""

import pandas as pd
import requests
import sqlite3
import os
import json
import time
from datetime import date

from paths import OUTPUT_DIR, DB_PATH, REGISTERS_DIR

# --- Registry configuration ---

REGISTRIES = {
    'DE': {
        'name': 'Bundesanzeiger / Unternehmensregister',
        'search_url': 'https://www.unternehmensregister.de',
        'api': False,
        'notes': 'No public API. Manual search or web scraping needed. €1-5 per document.',
        'status': 'manual_only',
    },
    'FR': {
        'name': 'Infogreffe / RNE',
        'search_url': 'https://www.infogreffe.fr/',
        'api': False,
        'notes': 'No public CbCR API yet. Search via web interface.',
        'status': 'manual_only',
    },
    'NL': {
        'name': 'KVK (Kamer van Koophandel)',
        'search_url': 'https://www.kvk.nl/en/',
        'api': True,
        'api_base': 'https://api.kvk.nl/api/v1',
        'notes': 'API requires contract/key. Basic search free.',
        'status': 'api_available',
    },
    'SE': {
        'name': 'Bolagsverket',
        'search_url': 'https://bolagsverket.se/',
        'api': True,
        'api_base': 'https://bolagsverket.se/api',
        'notes': 'Free API (60 queries/min). Paid API for company details.',
        'status': 'api_available',
    },
    'IT': {
        'name': 'Registro Imprese / InfoCamere',
        'search_url': 'https://www.registroimprese.it/',
        'api': True,
        'api_base': 'https://accessoallebanchedati.registroimprese.it/abdo/api',
        'notes': 'API requires service agreement.',
        'status': 'api_available',
    },
    'ES': {
        'name': 'Registro Mercantil',
        'search_url': 'https://opendata.registradores.org/en/',
        'api': False,
        'notes': 'Open data portal available. XBRL filing supported.',
        'status': 'open_data',
    },
    'BE': {
        'name': 'BCE/KBO (Banque-Carrefour des Entreprises)',
        'search_url': 'https://economie.fgov.be/en/themes/enterprises/crossroads-bank-enterprises',
        'api': True,
        'notes': 'API available (€50 per 2000 requests). Bulk CSV downloads free.',
        'status': 'api_available',
    },
    'DK': {
        'name': 'CVR (Erhvervsstyrelsen)',
        'search_url': 'https://virk.dk/',
        'api': True,
        'notes': 'API available since Sep 2025. Requires service account.',
        'status': 'api_available',
    },
    'FI': {
        'name': 'PRH (Finnish Patent and Registration Office)',
        'search_url': 'https://avoindata.prh.fi/en',
        'api': True,
        'api_base': 'https://avoindata.prh.fi/opendata-ytj-api.html',
        'notes': 'Free open data API. JSON, updated daily. No registration needed.',
        'status': 'free_api',
    },
    'AT': {
        'name': 'Firmenbuch / OpenFirmenbuch',
        'search_url': 'https://openfirmenbuch.at/',
        'api': True,
        'notes': 'OpenFirmenbuch offers free API, no registration.',
        'status': 'free_api',
    },
}

# --- 1. Load in-scope firms by country ---

print('Loading in-scope firms...')
df_firms = pd.read_csv(os.path.join(OUTPUT_DIR, 'firms_in_scope.csv'))
print(f'  {len(df_firms)} in-scope firms')

country_counts = df_firms['country_iso'].value_counts()
print(f'\n  Top 10 countries by in-scope firm count:')
for country, count in country_counts.head(10).items():
    reg = REGISTRIES.get(country, {})
    name = reg.get('name', 'Unknown register')
    status = reg.get('status', 'not_configured')
    print(f'    {country}: {count} firms — {name} [{status}]')

# --- 2. Finland (PRH) - Free open data API ---

def check_finland_prh(df_fi_firms):
    """Check Finnish PRH open data for company filings."""
    print('\n=== Finland: PRH Open Data ===')
    if len(df_fi_firms) == 0:
        print('  No Finnish firms in scope.')
        return []

    print(f'  {len(df_fi_firms)} Finnish firms to check')
    results = []

    # PRH open data API - search by company name
    base_url = 'https://avoindata.prh.fi/opendata-ytj-api/v3/companies'

    for _, firm in df_fi_firms.head(20).iterrows():  # Start with first 20 as test
        name = firm['company_name']
        try:
            resp = requests.get(
                base_url,
                params={'name': name, 'maxResults': 5},
                timeout=30,
                headers={'Accept': 'application/json'}
            )
            if resp.status_code == 200:
                data = resp.json()
                companies = data.get('results', data.get('companies', []))
                if companies:
                    for co in companies[:1]:  # Best match
                        results.append({
                            'bvd_id': firm['bvd_id'],
                            'company_name': name,
                            'prh_name': co.get('name', ''),
                            'business_id': co.get('businessId', ''),
                            'status': 'found',
                        })
                else:
                    results.append({
                        'bvd_id': firm['bvd_id'],
                        'company_name': name,
                        'status': 'not_found',
                    })
            elif resp.status_code == 429:
                print(f'  Rate limited, pausing...')
                time.sleep(5)
            else:
                results.append({
                    'bvd_id': firm['bvd_id'],
                    'company_name': name,
                    'status': f'error_{resp.status_code}',
                })
            time.sleep(0.5)  # Rate limiting
        except Exception as e:
            results.append({
                'bvd_id': firm['bvd_id'],
                'company_name': name,
                'status': f'error: {str(e)[:50]}',
            })

    df_results = pd.DataFrame(results)
    found = len(df_results[df_results['status'] == 'found'])
    print(f'  Checked {len(df_results)} firms: {found} found in PRH')

    if len(df_results) > 0:
        csv_path = os.path.join(REGISTERS_DIR, 'finland_prh_lookup.csv')
        df_results.to_csv(csv_path, index=False)
        print(f'  Saved to {csv_path}')

    return results

# --- 3. Denmark (CVR/VIRK) - has API ---

def check_denmark_cvr(df_dk_firms):
    """Check Danish CVR for company information."""
    print('\n=== Denmark: CVR ===')
    if len(df_dk_firms) == 0:
        print('  No Danish firms in scope.')
        return []

    print(f'  {len(df_dk_firms)} Danish firms to check')

    # CVR ElasticSearch API (publicly queryable for basic data)
    base_url = 'https://cvrapi.dk/api'

    results = []
    for _, firm in df_dk_firms.head(20).iterrows():
        name = firm['company_name']
        try:
            resp = requests.get(
                base_url,
                params={'search': name, 'country': 'dk'},
                headers={'User-Agent': 'TJN-CbCR-Research/1.0'},
                timeout=30,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get('vat'):
                    results.append({
                        'bvd_id': firm['bvd_id'],
                        'company_name': name,
                        'cvr_name': data.get('name', ''),
                        'cvr_number': data.get('vat', ''),
                        'status': 'found',
                    })
                else:
                    results.append({
                        'bvd_id': firm['bvd_id'],
                        'company_name': name,
                        'status': 'not_found',
                    })
            else:
                results.append({
                    'bvd_id': firm['bvd_id'],
                    'company_name': name,
                    'status': f'error_{resp.status_code}',
                })
            time.sleep(1)
        except Exception as e:
            results.append({
                'bvd_id': firm['bvd_id'],
                'company_name': name,
                'status': f'error: {str(e)[:50]}',
            })

    df_results = pd.DataFrame(results)
    found = len(df_results[df_results['status'] == 'found'])
    print(f'  Checked {len(df_results)} firms: {found} found in CVR')

    if len(df_results) > 0:
        csv_path = os.path.join(REGISTERS_DIR, 'denmark_cvr_lookup.csv')
        df_results.to_csv(csv_path, index=False)
        print(f'  Saved to {csv_path}')

    return results

# --- 4. Run available register checks ---

print('\n' + '='*60)
print('CHECKING AVAILABLE NATIONAL REGISTERS')
print('='*60)

# Finland
df_fi = df_firms[df_firms['country_iso'] == 'FI']
check_finland_prh(df_fi)

# Denmark
df_dk = df_firms[df_firms['country_iso'] == 'DK']
check_denmark_cvr(df_dk)

# --- 5. Summary and next steps ---

print('\n' + '='*60)
print('NATIONAL REGISTER STATUS SUMMARY')
print('='*60)

print(f"""
Register availability for CbCR collection:

  FREE API (can query now):
    FI - PRH Open Data       — {len(df_firms[df_firms['country_iso']=='FI'])} firms
    AT - OpenFirmenbuch       — {len(df_firms[df_firms['country_iso']=='AT'])} firms

  API AVAILABLE (needs credentials/contract):
    NL - KVK                  — {len(df_firms[df_firms['country_iso']=='NL'])} firms
    SE - Bolagsverket         — {len(df_firms[df_firms['country_iso']=='SE'])} firms
    IT - Registro Imprese     — {len(df_firms[df_firms['country_iso']=='IT'])} firms
    BE - BCE/KBO              — {len(df_firms[df_firms['country_iso']=='BE'])} firms
    DK - CVR                  — {len(df_firms[df_firms['country_iso']=='DK'])} firms

  MANUAL ONLY (no API):
    DE - Bundesanzeiger       — {len(df_firms[df_firms['country_iso']=='DE'])} firms
    FR - Infogreffe           — {len(df_firms[df_firms['country_iso']=='FR'])} firms
    ES - Registro Mercantil   — {len(df_firms[df_firms['country_iso']=='ES'])} firms

IMPORTANT: First CbCR filings are due by 31 December 2026.
Registers are unlikely to have CbCR filings before that date.

This script should be re-run periodically starting Q4 2026 to
check for newly filed CbCR reports.

Current focus: Build registry ID crosswalk (national ID <-> BvD ID)
so we can efficiently query registers when filings become available.
""")

# Save registry status
status_data = []
for country_iso, reg in REGISTRIES.items():
    n_firms = len(df_firms[df_firms['country_iso'] == country_iso])
    status_data.append({
        'country_iso': country_iso,
        'register_name': reg['name'],
        'api_status': reg['status'],
        'firms_in_scope': n_firms,
        'search_url': reg['search_url'],
        'notes': reg['notes'],
    })

df_status = pd.DataFrame(status_data)
df_status.to_csv(os.path.join(REGISTERS_DIR, 'registry_status.csv'), index=False)
print(f'Saved registry status to {os.path.join(REGISTERS_DIR, "registry_status.csv")}')
print('Done.')
