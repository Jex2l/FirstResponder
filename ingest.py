"""
FirstResponder Copilot — Data Ingestion Pipeline
Downloads NYC Open Data datasets and loads them into a local DuckDB database.
Designed to run on NVIDIA GB10 / DGX Spark.

Usage:
    python ingest.py --download    # Download all CSVs
    python ingest.py --load        # Load CSVs into DuckDB
    python ingest.py --all         # Download + Load + Compute risk scores
"""

import os
import sys
import argparse
import requests
import duckdb
import math
from pathlib import Path
from datetime import datetime, timedelta

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

DATA_DIR = Path("data/raw")
DB_PATH = "data/responder.duckdb"
SCHEMA_PATH = "schema.sql"

# Socrata API base
SOCRATA_BASE = "https://data.cityofnewyork.us/resource"

# Dataset registry: (name, socrata_id, row_limit, filter_clause)
# socrata_id is the 4x4 identifier from the dataset URL
DATASETS = {
    # Building profiles
    "pluto": {
        "id": "64uk-42ks",
        "limit": 900000,
        "filter": None,
        "filename": "pluto.csv"
    },
    # DOB Violations
    "dob_violations": {
        "id": "3h2n-5cm9",
        "limit": 2000000,
        "filter": None,
        "filename": "dob_violations.csv"
    },
    # DOB Safety Violations
    "dob_safety_violations": {
        "id": "855j-jady",
        "limit": 500000,
        "filter": None,
        "filename": "dob_safety_violations.csv"
    },
    # HPD Violations
    "hpd_violations": {
        "id": "wvxf-dwi5",
        "limit": 3000000,
        "filter": None,
        "filename": "hpd_violations.csv"
    },
    # HPD Complaints & Problems
    "hpd_complaints": {
        "id": "ygpa-z7cr",
        "limit": 3000000,
        "filter": None,
        "filename": "hpd_complaints.csv"
    },
    # 311 (filtered for safety-relevant complaints)
    "311_requests": {
        "id": "erm2-nwe9",
        "limit": 500000,
        "filter": "complaint_type in('HEATING','PLUMBING','GENERAL CONSTRUCTION','Heat/Hot Water','ELECTRIC','Gas Leak','STRUCTURAL','SAFETY','Hazardous Materials','Illegal Conversion','Building/Use')",
        "filename": "311_requests.csv"
    },
    # Fire Incident Dispatch
    "fire_incidents": {
        "id": "8m42-w767",
        "limit": 2000000,
        "filter": None,
        "filename": "fire_incidents.csv"
    },
    # Fire Company Incidents
    "fire_company_incidents": {
        "id": "tm6d-hbzd",
        "limit": 2000000,
        "filter": None,
        "filename": "fire_company_incidents.csv"
    },
    # EMS Dispatch
    "ems_incidents": {
        "id": "76xm-jjuj",
        "limit": 2000000,
        "filter": None,
        "filename": "ems_incidents.csv"
    },
    # NYPD Complaints
    "nypd_complaints": {
        "id": "5uac-w243",
        "limit": 500000,
        "filter": None,
        "filename": "nypd_complaints.csv"
    },
    # Fire Hydrants
    "fire_hydrants": {
        "id": "23d2-ttdp",
        "limit": 200000,
        "filter": None,
        "filename": "fire_hydrants.csv"
    },
    # HHC Facilities (hospitals)
    "hospitals": {
        "id": "kxmf-j285",
        "limit": 1000,
        "filter": None,
        "filename": "hospitals.csv"
    },
    # City Facilities Database
    "facilities": {
        "id": "ji82-xba5",
        "limit": 50000,
        "filter": None,
        "filename": "facilities.csv"
    },
    # Building Registrations
    "registrations": {
        "id": "tesw-yqqr",
        "limit": 500000,
        "filter": None,
        "filename": "registrations.csv"
    },
    # Registration Contacts (owners)
    "registration_contacts": {
        "id": "feu5-w2e2",
        "limit": 1000000,
        "filter": None,
        "filename": "registration_contacts.csv"
    },
    # Elevator Device Details
    "elevators": {
        "id": "juyv-2jek",
        "limit": 500000,
        "filter": None,
        "filename": "elevators.csv"
    },
}


# ─────────────────────────────────────────────
# Download Functions
# ─────────────────────────────────────────────

def download_dataset(name: str, config: dict):
    """Download a single dataset from NYC Open Data Socrata API."""
    filepath = DATA_DIR / config["filename"]
    
    if filepath.exists():
        size_mb = filepath.stat().st_size / (1024 * 1024)
        print(f"  ⏭️  {name}: already exists ({size_mb:.1f} MB), skipping. Delete to re-download.")
        return True
    
    base_url = f"{SOCRATA_BASE}/{config['id']}.csv"
    params = {"$limit": config["limit"]}
    
    if config.get("filter"):
        params["$where"] = config["filter"]
    
    print(f"  ⬇️  {name}: downloading from Socrata ({config['id']})...")
    print(f"      URL: {base_url}")
    
    try:
        # Stream download for large files
        response = requests.get(base_url, params=params, stream=True, timeout=300)
        response.raise_for_status()
        
        total_size = 0
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192 * 16):
                f.write(chunk)
                total_size += len(chunk)
        
        size_mb = total_size / (1024 * 1024)
        print(f"  ✅ {name}: downloaded ({size_mb:.1f} MB)")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"  ❌ {name}: download failed - {e}")
        # Try the direct CSV export URL as fallback
        fallback_url = f"https://data.cityofnewyork.us/api/views/{config['id']}/rows.csv?accessType=DOWNLOAD"
        print(f"      Trying fallback URL...")
        try:
            response = requests.get(fallback_url, stream=True, timeout=600)
            response.raise_for_status()
            total_size = 0
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192 * 16):
                    f.write(chunk)
                    total_size += len(chunk)
            size_mb = total_size / (1024 * 1024)
            print(f"  ✅ {name}: downloaded via fallback ({size_mb:.1f} MB)")
            return True
        except Exception as e2:
            print(f"  ❌ {name}: fallback also failed - {e2}")
            return False


def download_all():
    """Download all datasets."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    print("\n" + "=" * 60)
    print("📥 DOWNLOADING NYC OPEN DATA DATASETS")
    print("=" * 60 + "\n")
    
    success = 0
    failed = 0
    
    for name, config in DATASETS.items():
        result = download_dataset(name, config)
        if result:
            success += 1
        else:
            failed += 1
    
    print(f"\n📊 Download Summary: {success} succeeded, {failed} failed")
    return failed == 0


# ─────────────────────────────────────────────
# Load Functions
# ─────────────────────────────────────────────

def init_db(con: duckdb.DuckDBPyConnection):
    """Initialize the database with schema."""
    if os.path.exists(SCHEMA_PATH):
        with open(SCHEMA_PATH, 'r') as f:
            schema_sql = f.read()
        # Execute each statement separately
        for statement in schema_sql.split(';'):
            statement = statement.strip()
            if statement and not statement.startswith('--'):
                try:
                    con.execute(statement)
                except Exception as e:
                    print(f"  ⚠️  Schema warning: {e}")
    print("  ✅ Database schema initialized")


def load_pluto(con: duckdb.DuckDBPyConnection):
    """Load PLUTO building data."""
    filepath = DATA_DIR / "pluto.csv"
    if not filepath.exists():
        print("  ⏭️  PLUTO: file not found, skipping")
        return
    
    print("  📦 Loading PLUTO (building profiles)...")
    con.execute("""
        INSERT OR REPLACE INTO buildings 
        SELECT 
            CAST("BIN" AS VARCHAR) as bin,
            "Borough" as borough,
            CAST("Block" AS VARCHAR) as block,
            CAST("Lot" AS VARCHAR) as lot,
            "Address" as address,
            CAST("ZipCode" AS VARCHAR) as zipcode,
            CAST("BoroCode" AS INTEGER) as borough_code,
            TRY_CAST("NumFloors" AS INTEGER) as num_floors,
            TRY_CAST("YearBuilt" AS INTEGER) as year_built,
            "BldgClass" as building_class,
            "LandUse" as land_use,
            TRY_CAST("UnitsRes" AS INTEGER) as residential_units,
            TRY_CAST("UnitsTotal" AS INTEGER) as total_units,
            TRY_CAST("LotArea" AS DOUBLE) as lot_area,
            TRY_CAST("BldgArea" AS DOUBLE) as building_area,
            NULL as construction_type,
            "OwnerName" as owner_name,
            TRY_CAST("Latitude" AS DOUBLE) as latitude,
            TRY_CAST("Longitude" AS DOUBLE) as longitude,
            0.0 as risk_score,
            CURRENT_TIMESTAMP as last_updated
        FROM read_csv_auto('{}', header=true, ignore_errors=true)
        WHERE "BIN" IS NOT NULL AND "BIN" != '0' AND "BIN" != ''
    """.format(filepath))
    
    count = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    print(f"  ✅ PLUTO: loaded {count:,} buildings")


def load_dob_violations(con: duckdb.DuckDBPyConnection):
    """Load DOB violations."""
    filepath = DATA_DIR / "dob_violations.csv"
    if not filepath.exists():
        print("  ⏭️  DOB Violations: file not found, skipping")
        return
    
    print("  📦 Loading DOB Violations...")
    con.execute("""
        INSERT INTO dob_violations 
        SELECT 
            ROW_NUMBER() OVER () as id,
            CAST("BIN" AS VARCHAR) as bin,
            CAST("BLOCK" AS VARCHAR) as block,
            CAST("LOT" AS VARCHAR) as lot,
            "VIOLATION_TYPE" as violation_type,
            "VIOLATION_NUMBER" as violation_number,
            "VIOLATION_CATEGORY" as violation_category,
            "DESCRIPTION" as description,
            TRY_CAST("DISPOSITION_DATE" AS DATE) as disposition_date,
            "DISPOSITION_COMMENTS" as disposition_comments,
            TRY_CAST("ISSUE_DATE" AS DATE) as issue_date,
            CASE 
                WHEN "VIOLATION_TYPE" IN ('LL6291', 'AEUHAZ', 'IMEGNCY', 'LL1081') THEN 'CRITICAL'
                WHEN "VIOLATION_TYPE" IN ('ACC1', 'HBLVIO', 'P*CLSS1') THEN 'HIGH'
                WHEN "VIOLATION_TYPE" IN ('UB', 'COMPBLD') THEN 'MEDIUM'
                ELSE 'LOW'
            END as severity,
            CASE WHEN "DISPOSITION_DATE" IS NULL THEN TRUE ELSE FALSE END as is_active
        FROM read_csv_auto('{}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """.format(filepath))
    
    count = con.execute("SELECT COUNT(*) FROM dob_violations").fetchone()[0]
    active = con.execute("SELECT COUNT(*) FROM dob_violations WHERE is_active = true").fetchone()[0]
    print(f"  ✅ DOB Violations: loaded {count:,} ({active:,} active)")


def load_hpd_violations(con: duckdb.DuckDBPyConnection):
    """Load HPD housing violations."""
    filepath = DATA_DIR / "hpd_violations.csv"
    if not filepath.exists():
        print("  ⏭️  HPD Violations: file not found, skipping")
        return
    
    print("  📦 Loading HPD Violations...")
    con.execute("""
        INSERT INTO hpd_violations 
        SELECT 
            TRY_CAST("ViolationID" AS INTEGER) as violation_id,
            CAST("BIN" AS VARCHAR) as bin,
            TRY_CAST("BuildingID" AS INTEGER) as building_id,
            "BoroID" as borough_id,
            "Block" as block,
            "Lot" as lot,
            "Apartment" as apartment,
            "Story" as story,
            "Class" as violation_class,
            TRY_CAST("InspectionDate" AS DATE) as inspection_date,
            TRY_CAST("ApprovedDate" AS DATE) as approved_date,
            TRY_CAST("OriginalCertifyByDate" AS DATE) as original_certify_by_date,
            TRY_CAST("OriginalCorrectByDate" AS DATE) as original_correct_by_date,
            TRY_CAST("NewCertifyByDate" AS DATE) as new_certify_by_date,
            TRY_CAST("NewCorrectByDate" AS DATE) as new_correct_by_date,
            TRY_CAST("CertifiedDismissedDatetime" AS TIMESTAMP) as certified_dismissed_datetime,
            "OrderNumber" as order_number,
            "NOVID" as nov_id,
            "NOVDescription" as nov_description,
            TRY_CAST("NOVIssuedDate" AS DATE) as nov_issueddate,
            "CurrentStatus" as current_status,
            TRY_CAST("CurrentStatusDate" AS DATE) as current_status_date
        FROM read_csv_auto('{}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """.format(filepath))
    
    count = con.execute("SELECT COUNT(*) FROM hpd_violations").fetchone()[0]
    class_c = con.execute("SELECT COUNT(*) FROM hpd_violations WHERE violation_class = 'C' AND current_status != 'CLOSE'").fetchone()[0]
    print(f"  ✅ HPD Violations: loaded {count:,} ({class_c:,} open Class C - immediately hazardous)")


def load_311(con: duckdb.DuckDBPyConnection):
    """Load 311 service requests."""
    filepath = DATA_DIR / "311_requests.csv"
    if not filepath.exists():
        print("  ⏭️  311 Requests: file not found, skipping")
        return
    
    print("  📦 Loading 311 Service Requests (safety-filtered)...")
    con.execute("""
        INSERT INTO service_requests_311
        SELECT
            "unique_key" as unique_key,
            TRY_CAST("created_date" AS TIMESTAMP) as created_date,
            TRY_CAST("closed_date" AS TIMESTAMP) as closed_date,
            "agency" as agency,
            "agency_name" as agency_name,
            "complaint_type" as complaint_type,
            "descriptor" as descriptor,
            "location_type" as location_type,
            "incident_zip" as incident_zip,
            "incident_address" as incident_address,
            "city" as city,
            "borough" as borough,
            TRY_CAST("latitude" AS DOUBLE) as latitude,
            TRY_CAST("longitude" AS DOUBLE) as longitude,
            NULL as bin,
            "status" as status,
            "resolution_description" as resolution_description
        FROM read_csv_auto('{}', header=true, ignore_errors=true, all_varchar=true)
    """.format(filepath))
    
    count = con.execute("SELECT COUNT(*) FROM service_requests_311").fetchone()[0]
    print(f"  ✅ 311 Requests: loaded {count:,}")


def load_fire_incidents(con: duckdb.DuckDBPyConnection):
    """Load fire incident dispatch data."""
    filepath = DATA_DIR / "fire_incidents.csv"
    if not filepath.exists():
        print("  ⏭️  Fire Incidents: file not found, skipping")
        return
    
    print("  📦 Loading Fire Incident Dispatch Data...")
    con.execute("""
        INSERT INTO fire_incidents
        SELECT
            "STARFIRE_INCIDENT_ID" as incident_id,
            TRY_CAST("INCIDENT_DATETIME" AS TIMESTAMP) as incident_datetime,
            "INCIDENT_TYPE_DESC" as incident_type_desc,
            "INCIDENT_BOROUGH" as incident_borough,
            "ZIPCODE" as zipcode,
            "POLICEPRECINCT" as policeprecinct,
            "INCIDENT_CLASSIFICATION" as incident_classification,
            "INCIDENT_CLASSIFICATION_GROUP" as incident_classification_group,
            TRY_CAST("DISPATCH_RESPONSE_SECONDS_QY" AS INTEGER) as dispatch_response_seconds,
            TRY_CAST("INCIDENT_RESPONSE_SECONDS_QY" AS INTEGER) as incident_response_seconds,
            TRY_CAST("INCIDENT_TRAVEL_TM_SECONDS_QY" AS INTEGER) as incident_travel_seconds,
            TRY_CAST("ENGINES_ASSIGNED_QUANTITY" AS INTEGER) as engines_assigned,
            TRY_CAST("LADDERS_ASSIGNED_QUANTITY" AS INTEGER) as ladders_assigned,
            TRY_CAST("OTHER_UNITS_ASSIGNED_QUANTITY" AS INTEGER) as other_units_assigned,
            TRY_CAST("LATITUDE" AS DOUBLE) as latitude,
            TRY_CAST("LONGITUDE" AS DOUBLE) as longitude,
            NULL as bin
        FROM read_csv_auto('{}', header=true, ignore_errors=true, all_varchar=true)
    """.format(filepath))
    
    count = con.execute("SELECT COUNT(*) FROM fire_incidents").fetchone()[0]
    print(f"  ✅ Fire Incidents: loaded {count:,}")


def load_hydrants(con: duckdb.DuckDBPyConnection):
    """Load fire hydrant locations."""
    filepath = DATA_DIR / "fire_hydrants.csv"
    if not filepath.exists():
        print("  ⏭️  Fire Hydrants: file not found, skipping")
        return
    
    print("  📦 Loading Fire Hydrants...")
    con.execute("""
        INSERT INTO fire_hydrants
        SELECT
            ROW_NUMBER() OVER () as id,
            TRY_CAST("LATITUDE" AS DOUBLE) as latitude,
            TRY_CAST("LONGITUDE" AS DOUBLE) as longitude,
            "UNITID" as unitid,
            "BOROUGH" as borough
        FROM read_csv_auto('{}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "LATITUDE" IS NOT NULL AND "LONGITUDE" IS NOT NULL
    """.format(filepath))
    
    count = con.execute("SELECT COUNT(*) FROM fire_hydrants").fetchone()[0]
    print(f"  ✅ Fire Hydrants: loaded {count:,}")


def load_hospitals(con: duckdb.DuckDBPyConnection):
    """Load hospital/facility locations."""
    filepath = DATA_DIR / "hospitals.csv"
    if not filepath.exists():
        print("  ⏭️  Hospitals: file not found, skipping")
        return
    
    print("  📦 Loading Hospitals...")
    con.execute("""
        INSERT INTO hospitals
        SELECT
            "Facility Name" as facility_name,
            "Facility Type" as facility_type,
            "Borough" as borough,
            "Location 1" as address,
            "Phone Number" as phone,
            TRY_CAST("Latitude" AS DOUBLE) as latitude,
            TRY_CAST("Longitude" AS DOUBLE) as longitude
        FROM read_csv_auto('{}', header=true, ignore_errors=true, all_varchar=true)
    """.format(filepath))
    
    count = con.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    print(f"  ✅ Hospitals: loaded {count:,}")


def load_all(con: duckdb.DuckDBPyConnection):
    """Load all datasets into DuckDB."""
    print("\n" + "=" * 60)
    print("📦 LOADING DATA INTO DUCKDB")
    print("=" * 60 + "\n")
    
    init_db(con)
    
    # Load in dependency order
    load_pluto(con)
    load_dob_violations(con)
    load_hpd_violations(con)
    load_hpd_complaints(con) if (DATA_DIR / "hpd_complaints.csv").exists() else None
    load_311(con)
    load_fire_incidents(con)
    load_hydrants(con)
    load_hospitals(con)
    
    # Print summary
    print("\n" + "=" * 60)
    print("📊 DATABASE SUMMARY")
    print("=" * 60)
    
    tables = con.execute("SHOW TABLES").fetchall()
    for (table,) in tables:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table}: {count:,} rows")
        except:
            print(f"  {table}: (empty)")
    
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    print(f"\n  💾 Database size: {db_size:.1f} MB")


# ─────────────────────────────────────────────
# Risk Score Computation
# ─────────────────────────────────────────────

def compute_risk_scores(con: duckdb.DuckDBPyConnection):
    """Compute composite risk scores for every building."""
    print("\n" + "=" * 60)
    print("🧮 COMPUTING BUILDING RISK SCORES")
    print("=" * 60 + "\n")
    
    # Count active DOB violations per building
    print("  Computing DOB violation scores...")
    con.execute("""
        INSERT OR REPLACE INTO building_risk_scores (bin, active_dob_violations, overall_risk_score)
        SELECT 
            bin,
            COUNT(*) as active_dob_violations,
            0.0 as overall_risk_score
        FROM dob_violations
        WHERE is_active = true
        GROUP BY bin
    """)
    
    # Update with HPD Class C (immediately hazardous) counts
    print("  Computing HPD hazardous violation scores...")
    con.execute("""
        UPDATE building_risk_scores 
        SET active_hpd_class_c = sub.cnt
        FROM (
            SELECT bin, COUNT(*) as cnt 
            FROM hpd_violations 
            WHERE violation_class = 'C' AND current_status != 'CLOSE'
            GROUP BY bin
        ) as sub
        WHERE building_risk_scores.bin = sub.bin
    """)
    
    con.execute("""
        UPDATE building_risk_scores 
        SET active_hpd_class_b = sub.cnt
        FROM (
            SELECT bin, COUNT(*) as cnt 
            FROM hpd_violations 
            WHERE violation_class = 'B' AND current_status != 'CLOSE'
            GROUP BY bin
        ) as sub
        WHERE building_risk_scores.bin = sub.bin
    """)
    
    # Compute overall risk score (weighted formula)
    print("  Computing composite risk scores...")
    con.execute("""
        UPDATE building_risk_scores SET overall_risk_score = 
            COALESCE(active_dob_violations, 0) * 2.0 +
            COALESCE(active_hpd_class_c, 0) * 5.0 +
            COALESCE(active_hpd_class_b, 0) * 2.0 +
            COALESCE(prior_fire_incidents, 0) * 3.0 +
            COALESCE(complaint_velocity_30d, 0) * 1.5
    """)
    
    scored = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score > 0").fetchone()[0]
    high_risk = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score > 20").fetchone()[0]
    print(f"  ✅ Risk scores computed for {scored:,} buildings ({high_risk:,} high-risk)")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FirstResponder Copilot - Data Ingestion")
    parser.add_argument("--download", action="store_true", help="Download datasets from NYC Open Data")
    parser.add_argument("--load", action="store_true", help="Load CSVs into DuckDB")
    parser.add_argument("--risk", action="store_true", help="Compute risk scores")
    parser.add_argument("--all", action="store_true", help="Download + Load + Risk scores")
    
    args = parser.parse_args()
    
    if not any([args.download, args.load, args.risk, args.all]):
        parser.print_help()
        sys.exit(1)
    
    if args.download or args.all:
        download_all()
    
    if args.load or args.all:
        os.makedirs(os.path.dirname(DB_PATH) or '.', exist_ok=True)
        con = duckdb.connect(DB_PATH)
        load_all(con)
        con.close()
    
    if args.risk or args.all:
        con = duckdb.connect(DB_PATH)
        compute_risk_scores(con)
        con.close()
    
    print("\n🎉 Done! Database ready at:", DB_PATH)


if __name__ == "__main__":
    main()
