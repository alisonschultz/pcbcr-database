"""Shared path definitions for all scripts."""
import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Input data
ORBIS_DIR = os.path.join(PROJECT_ROOT, 'data', 'orbis_exports')

# Outputs
OUTPUT_DIR = os.path.join(PROJECT_ROOT, 'data', 'outputs')
DB_PATH = os.path.join(OUTPUT_DIR, 'pcbcr_tracker.db')
WRDS_DIR = os.path.join(PROJECT_ROOT, 'data', 'wrds')
TAXOBS_DIR = os.path.join(PROJECT_ROOT, 'data', 'tax_observatory')
REGISTERS_DIR = os.path.join(PROJECT_ROOT, 'data', 'national_registers')

# Ensure directories exist
for d in [OUTPUT_DIR, WRDS_DIR, TAXOBS_DIR, REGISTERS_DIR]:
    os.makedirs(d, exist_ok=True)
