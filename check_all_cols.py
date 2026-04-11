"""Run this from your project folder to dump all CSV column names."""
import duckdb
from pathlib import Path

DATA_DIR = Path("data/raw")

files = [
    "pluto.csv",
    "registrations.csv",
    "registration_contacts.csv",
    "dob_violations.csv",
    "dob_safety_violations.csv",
    "dob_ecb_violations.csv",
    "hpd_violations.csv",
    "hpd_complaints.csv",
    "fire_prevention_inspections.csv",
    "fire_incidents.csv",
    "fire_company_incidents.csv",
    "ems_incidents.csv",
    "311_requests.csv",
    "fire_hydrants.csv",
    "hospitals.csv",
    "elevators.csv",
]

con = duckdb.connect()
for fname in files:
    fp = DATA_DIR / fname
    if not fp.exists():
        print(f"\n=== {fname} === MISSING")
        continue
    fp_str = str(fp).replace("\\", "/")
    try:
        desc = con.execute(
            f"SELECT * FROM read_csv_auto('{fp_str}', header=true, ignore_errors=true) LIMIT 0"
        ).description
        cols = [c[0] for c in desc]
        print(f"\n=== {fname} ===")
        for c in cols:
            print(f"  {c}")
    except Exception as e:
        print(f"\n=== {fname} === ERROR: {e}")
con.close()
