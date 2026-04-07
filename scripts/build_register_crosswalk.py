"""
Build a crosswalk mapping BvD IDs to national business register IDs.

Queries free APIs (Finland PRH, Denmark CVR, Austria OpenFirmenbuch)
for all in-scope firms. Supports resume — skips firms already looked up.

For countries without free APIs, extracts national IDs from BvD ID prefixes
where possible (many BvD IDs encode national register numbers).

Run with: python build_register_crosswalk.py [--country FI] [--limit 50]
"""

import pandas as pd
import requests
import os
import sys
import time
import re
import argparse
from paths import OUTPUT_DIR, REGISTERS_DIR

CROSSWALK_PATH = os.path.join(REGISTERS_DIR, 'register_crosswalk.csv')

# --- BvD ID parsing ---
# BvD IDs often encode national register numbers:
#   DE2150006872 -> DE + Handelsregister number
#   FR542051180 -> FR + SIREN number
#   NL24311036 -> NL + KVK number
#   SE5560362262 -> SE + Organisationsnummer
#   DK36213728 -> DK + CVR number
#   FI01120389 -> FI + Y-tunnus (without dash)
#   AT*110782 -> AT + Firmenbuch number
#   BE0401574852 -> BE + BCE/KBO number (without dots)
#   IT01040930303 -> IT + Codice Fiscale
#   ESAXXXXXXXX -> ES + CIF

BVD_PATTERNS = {
    'DE': {'prefix': 'DE', 'name': 'Handelsregister', 'extract': lambda s: s[2:]},
    'FR': {'prefix': 'FR', 'name': 'SIREN', 'extract': lambda s: s[2:11] if len(s) >= 11 else s[2:]},
    'NL': {'prefix': 'NL', 'name': 'KVK', 'extract': lambda s: s[2:]},
    'SE': {'prefix': 'SE', 'name': 'Organisationsnummer', 'extract': lambda s: s[2:]},
    'DK': {'prefix': 'DK', 'name': 'CVR', 'extract': lambda s: s[2:]},
    'FI': {'prefix': 'FI', 'name': 'Y-tunnus', 'extract': lambda s: s[2:9] + '-' + s[9] if len(s) >= 10 else s[2:]},
    'BE': {'prefix': 'BE', 'name': 'BCE/KBO', 'extract': lambda s: s[2:]},
    'IT': {'prefix': 'IT', 'name': 'Codice Fiscale', 'extract': lambda s: s[2:]},
    'ES': {'prefix': 'ES', 'name': 'CIF/NIF', 'extract': lambda s: s[2:]},
    'AT': {'prefix': 'AT', 'name': 'Firmenbuch', 'extract': lambda s: s[2:].lstrip('*')},
    'NO': {'prefix': 'NO', 'name': 'Organisasjonsnummer', 'extract': lambda s: s[2:]},
    'PL': {'prefix': 'PL', 'name': 'KRS/REGON', 'extract': lambda s: s[2:]},
    'IE': {'prefix': 'IE', 'name': 'CRO', 'extract': lambda s: s[2:]},
    'LU': {'prefix': 'LU', 'name': 'RCS', 'extract': lambda s: s[2:]},
    'PT': {'prefix': 'PT', 'name': 'NIPC', 'extract': lambda s: s[2:]},
    'GR': {'prefix': 'GR', 'name': 'GEMI', 'extract': lambda s: s[2:]},
}


def extract_national_id(bvd_id, country_iso):
    """Extract national register ID from BvD ID."""
    if pd.isna(bvd_id):
        return None
    bvd_id = str(bvd_id).strip()
    pattern = BVD_PATTERNS.get(country_iso)
    if not pattern:
        return None
    prefix = pattern['prefix']
    if bvd_id.startswith(prefix):
        raw = pattern['extract'](bvd_id)
        # Clean: remove non-alphanumeric trailing chars
        raw = raw.rstrip('L').rstrip('*')
        return raw if raw else None
    return None


def load_crosswalk():
    """Load existing crosswalk for resume support."""
    if os.path.exists(CROSSWALK_PATH):
        return pd.read_csv(CROSSWALK_PATH)
    return pd.DataFrame(columns=[
        'bvd_id', 'company_name', 'country_iso',
        'national_id', 'national_id_type', 'register_name',
        'api_verified', 'api_match_name', 'api_match_id',
        'status', 'lookup_date'
    ])


def save_crosswalk(df):
    """Save crosswalk to CSV."""
    df.to_csv(CROSSWALK_PATH, index=False)


# --- API lookup functions ---

def lookup_finland_prh(name, bvd_id, national_id):
    """Look up a Finnish company in PRH open data."""
    base_url = 'https://avoindata.prh.fi/opendata-ytj-api/v3/companies'

    # Try by business ID first if we have one
    if national_id:
        try:
            resp = requests.get(base_url, params={'businessId': national_id},
                                timeout=30, headers={'Accept': 'application/json'})
            if resp.status_code == 200:
                data = resp.json()
                results = data.get('results', data.get('companies', []))
                if results:
                    co = results[0]
                    return {
                        'api_verified': True,
                        'api_match_name': co.get('name', ''),
                        'api_match_id': co.get('businessId', ''),
                        'status': 'verified_by_id',
                    }
        except Exception:
            pass

    # Fall back to name search
    try:
        resp = requests.get(base_url, params={'name': name, 'maxResults': 3},
                            timeout=30, headers={'Accept': 'application/json'})
        if resp.status_code == 200:
            data = resp.json()
            results = data.get('results', data.get('companies', []))
            if results:
                co = results[0]
                return {
                    'api_verified': True,
                    'api_match_name': co.get('name', ''),
                    'api_match_id': co.get('businessId', ''),
                    'status': 'found_by_name',
                }
        elif resp.status_code == 429:
            time.sleep(5)
            return {'status': 'rate_limited'}
    except Exception as e:
        return {'status': f'error: {str(e)[:50]}'}

    return {'status': 'not_found'}


def lookup_denmark_cvr(name, bvd_id, national_id):
    """Look up a Danish company in CVR."""
    base_url = 'https://cvrapi.dk/api'

    # Try by CVR number first
    if national_id:
        try:
            resp = requests.get(base_url, params={'vat': national_id, 'country': 'dk'},
                                headers={'User-Agent': 'TJN-CbCR-Research/1.0'}, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                if data.get('vat'):
                    return {
                        'api_verified': True,
                        'api_match_name': data.get('name', ''),
                        'api_match_id': str(data.get('vat', '')),
                        'status': 'verified_by_id',
                    }
        except Exception:
            pass

    # Fall back to name search
    try:
        resp = requests.get(base_url, params={'search': name, 'country': 'dk'},
                            headers={'User-Agent': 'TJN-CbCR-Research/1.0'}, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if data.get('vat'):
                return {
                    'api_verified': True,
                    'api_match_name': data.get('name', ''),
                    'api_match_id': str(data.get('vat', '')),
                    'status': 'found_by_name',
                }
    except Exception as e:
        return {'status': f'error: {str(e)[:50]}'}

    return {'status': 'not_found'}


API_FUNCTIONS = {
    'FI': ('PRH', lookup_finland_prh, 0.5),   # (register_name, function, delay_seconds)
    'DK': ('CVR', lookup_denmark_cvr, 1.0),
}


# --- Main ---

def main():
    parser = argparse.ArgumentParser(description='Build national register crosswalk')
    parser.add_argument('--country', type=str, help='Only process this country (ISO2)')
    parser.add_argument('--limit', type=int, default=0, help='Max firms to look up per country (0=all)')
    parser.add_argument('--api-only', action='store_true', help='Only run API lookups, skip BvD parsing')
    args = parser.parse_args()

    print('=== Building National Register Crosswalk ===\n')

    # Load firms
    df_firms = pd.read_csv(os.path.join(OUTPUT_DIR, 'firms_in_scope.csv'),
                           usecols=['bvd_id', 'company_name', 'country_iso'])
    # Also include VIA_SUBSIDIARY firms
    df_all = pd.read_csv(os.path.join(OUTPUT_DIR, 'master_firm_list.csv'),
                         usecols=['bvd_id', 'company_name', 'country_iso', 'regime_classification'])
    df_via_sub = df_all[df_all['regime_classification'].str.contains('VIA_SUBSIDIARY', na=False)]
    df_firms = pd.concat([df_firms, df_via_sub[['bvd_id', 'company_name', 'country_iso']]], ignore_index=True)
    df_firms = df_firms.drop_duplicates('bvd_id')
    print(f'Total firms to process: {len(df_firms)}')

    if args.country:
        df_firms = df_firms[df_firms['country_iso'] == args.country]
        print(f'Filtered to {args.country}: {len(df_firms)} firms')

    # Load existing crosswalk
    df_xwalk = load_crosswalk()
    existing_bvds = set(df_xwalk['bvd_id'])
    print(f'Existing crosswalk entries: {len(df_xwalk)}')

    # --- Step 1: Extract national IDs from BvD IDs ---
    if not args.api_only:
        print('\n--- Extracting national IDs from BvD IDs ---')
        new_rows = []
        for _, firm in df_firms.iterrows():
            if firm['bvd_id'] in existing_bvds:
                continue
            nat_id = extract_national_id(firm['bvd_id'], firm['country_iso'])
            pattern = BVD_PATTERNS.get(firm['country_iso'], {})
            new_rows.append({
                'bvd_id': firm['bvd_id'],
                'company_name': firm['company_name'],
                'country_iso': firm['country_iso'],
                'national_id': nat_id,
                'national_id_type': pattern.get('name', ''),
                'register_name': pattern.get('name', 'unknown'),
                'api_verified': False,
                'api_match_name': None,
                'api_match_id': None,
                'status': 'parsed' if nat_id else 'no_pattern',
                'lookup_date': pd.Timestamp.now().strftime('%Y-%m-%d'),
            })

        if new_rows:
            df_new = pd.DataFrame(new_rows)
            df_xwalk = pd.concat([df_xwalk, df_new], ignore_index=True)
            save_crosswalk(df_xwalk)
            parsed = sum(1 for r in new_rows if r['status'] == 'parsed')
            print(f'  Added {len(new_rows)} entries ({parsed} with national IDs)')

            # Summary by country
            df_new_df = pd.DataFrame(new_rows)
            for country, group in df_new_df.groupby('country_iso'):
                n_parsed = (group['status'] == 'parsed').sum()
                pattern = BVD_PATTERNS.get(country, {})
                print(f'  {country}: {len(group)} firms, {n_parsed} IDs extracted ({pattern.get("name", "?")})')

    # --- Step 2: API verification for countries with free APIs ---
    print('\n--- API verification ---')

    for country_code, (reg_name, lookup_fn, delay) in API_FUNCTIONS.items():
        if args.country and args.country != country_code:
            continue

        # Get firms for this country that haven't been API-verified
        mask = (df_xwalk['country_iso'] == country_code) & (~df_xwalk['api_verified'].astype(bool))
        to_verify = df_xwalk[mask]

        if args.limit > 0:
            to_verify = to_verify.head(args.limit)

        if len(to_verify) == 0:
            print(f'\n  {country_code} ({reg_name}): all firms already verified')
            continue

        print(f'\n  {country_code} ({reg_name}): verifying {len(to_verify)} firms...')
        verified = 0
        found = 0

        for idx, row in to_verify.iterrows():
            result = lookup_fn(row['company_name'], row['bvd_id'], row.get('national_id'))

            df_xwalk.at[idx, 'status'] = result.get('status', 'error')
            if result.get('api_verified'):
                df_xwalk.at[idx, 'api_verified'] = True
                df_xwalk.at[idx, 'api_match_name'] = result.get('api_match_name', '')
                df_xwalk.at[idx, 'api_match_id'] = result.get('api_match_id', '')
                # Update national_id if we got a better one from the API
                if result.get('api_match_id'):
                    df_xwalk.at[idx, 'national_id'] = result['api_match_id']
                found += 1
            verified += 1

            # Save every 50 lookups for resume safety
            if verified % 50 == 0:
                save_crosswalk(df_xwalk)
                print(f'    Progress: {verified}/{len(to_verify)} ({found} found)')

            if result.get('status') == 'rate_limited':
                print(f'    Rate limited at {verified}, saving and pausing...')
                save_crosswalk(df_xwalk)
                time.sleep(10)

            time.sleep(delay)

        save_crosswalk(df_xwalk)
        print(f'  {country_code}: Verified {verified}, found {found}')

    # --- Summary ---
    print('\n' + '='*60)
    print('CROSSWALK SUMMARY')
    print('='*60)
    print(f'Total entries: {len(df_xwalk)}')
    print(f'With national ID: {df_xwalk["national_id"].notna().sum()}')
    print(f'API verified: {df_xwalk["api_verified"].astype(bool).sum()}')

    print(f'\nBy country:')
    for country, group in df_xwalk.groupby('country_iso'):
        n = len(group)
        n_id = group['national_id'].notna().sum()
        n_api = group['api_verified'].astype(bool).sum()
        print(f'  {country}: {n} firms, {n_id} national IDs, {n_api} API-verified')

    print(f'\nSaved to: {CROSSWALK_PATH}')
    print('Done.')


if __name__ == '__main__':
    main()
