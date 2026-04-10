"""
FirstResponder Copilot — Data Ingestion Pipeline
Downloads NYC Open Data datasets and loads them into a local DuckDB database.

16 datasets across 5 layers:
  - Core building profile (PLUTO, Registrations, Registration Contacts)
  - Hazard layer (DOB Violations, DOB Safety, DOB ECB, HPD Violations, HPD Complaints, Fire Prevention)
  - Incident layer (Fire Dispatch, Fire Company, EMS Dispatch)
  - 311 complaints
  - Infrastructure (Fire Hydrants, Hospitals, Elevators)

Usage:
    python ingest.py --download    # Download all CSVs to data/raw/
    python ingest.py --load        # Load CSVs into DuckDB
    python ingest.py --risk        # Compute building risk scores
    python ingest.py --portfolio   # Compute owner portfolio table
    python ingest.py --all         # Everything in sequence
"""

import os
import sys
import argparse
import requests
import duckdb
from pathlib import Path
from datetime import datetime

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

DATA_DIR = Path("data/raw")
DB_PATH = "data/responder.duckdb"
SCHEMA_PATH = "schema.sql"

SOCRATA_BASE = "https://data.cityofnewyork.us/resource"

# ─────────────────────────────────────────────
# Dataset Registry
# 16 datasets, purposefully chosen for tactical
# first-responder briefs. Each has a Socrata 4x4
# ID, row limit matching expected dataset size,
# and optional $where filter for relevance.
# ─────────────────────────────────────────────

DATASETS = {

    # ── CORE: Building Profile ──────────────────────────────────────────
    # Foundation of every brief — floors, age, construction type, owner

    "pluto": {
        "id": "64uk-42ks",
        "limit": 900_000,
        "filter": None,
        "filename": "pluto.csv",
        "description": "Building profiles (floors, year built, construction type)"
    },
    "registrations": {
        "id": "tesw-yqqr",
        "limit": 500_000,
        "filter": None,
        "filename": "registrations.csv",
        "description": "Multiple dwelling registrations (owner/agent per building)"
    },
    "registration_contacts": {
        "id": "feu5-w2e2",
        "limit": 1_000_000,
        "filter": None,
        "filename": "registration_contacts.csv",
        "description": "Owner names and management companies — enables portfolio analysis"
    },

    # ── HAZARD LAYER: Violations & Inspections ──────────────────────────
    # What violations exist, what fines are unpaid, is suppression working?

    "dob_violations": {
        "id": "3h2n-5cm9",
        "limit": 2_000_000,
        "filter": None,
        "filename": "dob_violations.csv",
        "description": "DOB violations — structural, construction, elevator issues"
    },
    "dob_safety_violations": {
        "id": "855j-jady",
        "limit": 500_000,
        "filter": None,
        "filename": "dob_safety_violations.csv",
        "description": "DOB safety-specific violations — critical subset for first responders"
    },
    "dob_ecb_violations": {
        "id": "6bgk-3dad",
        "limit": 1_000_000,
        "filter": None,
        "filename": "dob_ecb_violations.csv",
        "description": "ECB violations — penalty amounts, unpaid balances = neglect signal"
    },
    "hpd_violations": {
        "id": "wvxf-dwi5",
        "limit": 3_000_000,
        "filter": None,
        "filename": "hpd_violations.csv",
        "description": "HPD violations — Class A/B/C (C = immediately hazardous)"
    },
    "hpd_complaints": {
        "id": "ygpa-z7cr",
        "limit": 3_000_000,
        "filter": None,
        "filename": "hpd_complaints.csv",
        "description": "HPD complaints — gas leaks, electrical hazards, heat failures"
    },
    "fire_prevention_inspections": {
        "id": "ssq6-fkht",
        "limit": 500_000,
        "filter": None,
        "filename": "fire_prevention_inspections.csv",
        "description": "FDNY Bureau of Fire Prevention — sprinkler/standpipe/suppression status"
    },

    # ── INCIDENT LAYER: Historical Response Data ────────────────────────
    # What has happened here before? Response times, fire spread, EMS volume

    "fire_incidents": {
        "id": "8m42-w767",
        "limit": 2_000_000,
        "filter": None,
        "filename": "fire_incidents.csv",
        "description": "Fire incident dispatch — incident type, response times, unit counts"
    },
    "fire_company_incidents": {
        "id": "tm6d-hbzd",
        "limit": 2_000_000,
        "filter": None,
        "filename": "fire_company_incidents.csv",
        "description": "Fire company incidents — floor of origin, fire spread, AES presence"
    },
    "ems_incidents": {
        "id": "76xm-jjuj",
        "limit": 2_000_000,
        "filter": None,
        "filename": "ems_incidents.csv",
        "description": "EMS dispatch — call types, response times, severity levels"
    },

    # ── 311 COMPLAINTS ──────────────────────────────────────────────────
    # Real-time building intelligence — complaint velocity = risk signal

    "311_requests": {
        "id": "erm2-nwe9",
        "limit": 500_000,
        "filter": "complaint_type in('HEATING','PLUMBING','GENERAL CONSTRUCTION',"
                  "'Heat/Hot Water','ELECTRIC','Gas Leak','STRUCTURAL','SAFETY',"
                  "'Hazardous Materials','Illegal Conversion','Building/Use')",
        "filename": "311_requests.csv",
        "description": "311 safety-relevant complaints — gas, structural, electrical, heat"
    },

    # ── INFRASTRUCTURE: Nearby Resources ────────────────────────────────
    # What can first responders use on arrival?

    "fire_hydrants": {
        "id": "23d2-ttdp",
        "limit": 200_000,
        "filter": None,
        "filename": "fire_hydrants.csv",
        "description": "Fire hydrant locations — nearest hydrant to building"
    },
    "hospitals": {
        "id": "kxmf-j285",
        "limit": 1_000,
        "filter": None,
        "filename": "hospitals.csv",
        "description": "NYC hospital/trauma center locations"
    },
    "elevators": {
        "id": "juyv-2jek",
        "limit": 500_000,
        "filter": None,
        "filename": "elevators.csv",
        "description": "Elevator device details — count, type, status per building"
    },
}


# ─────────────────────────────────────────────
# Download Functions
# ─────────────────────────────────────────────

def download_dataset(name: str, config: dict) -> bool:
    """Download a single dataset from NYC Open Data Socrata API."""
    filepath = DATA_DIR / config["filename"]

    if filepath.exists():
        size_mb = filepath.stat().st_size / (1024 * 1024)
        print(f"  ⏭️  {name}: already exists ({size_mb:.1f} MB) — delete to re-download")
        return True

    url = f"{SOCRATA_BASE}/{config['id']}.csv"
    params = {"$limit": config["limit"]}
    if config.get("filter"):
        params["$where"] = config["filter"]

    print(f"  ⬇️  {name}: {config['description']}")
    print(f"      {url}")

    try:
        resp = requests.get(url, params=params, stream=True, timeout=300)
        resp.raise_for_status()
        total = 0
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=131_072):
                f.write(chunk)
                total += len(chunk)
        print(f"  ✅ {name}: {total / 1024 / 1024:.1f} MB")
        return True

    except requests.exceptions.RequestException as e:
        print(f"  ⚠️  {name}: primary URL failed ({e}) — trying fallback...")
        fallback = f"https://data.cityofnewyork.us/api/views/{config['id']}/rows.csv?accessType=DOWNLOAD"
        try:
            resp = requests.get(fallback, stream=True, timeout=600)
            resp.raise_for_status()
            total = 0
            with open(filepath, "wb") as f:
                for chunk in resp.iter_content(chunk_size=131_072):
                    f.write(chunk)
                    total += len(chunk)
            print(f"  ✅ {name}: {total / 1024 / 1024:.1f} MB (via fallback)")
            return True
        except Exception as e2:
            print(f"  ❌ {name}: both URLs failed — {e2}")
            return False


def download_all() -> bool:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("\n" + "=" * 60)
    print("📥  DOWNLOADING NYC OPEN DATA — 16 DATASETS")
    print("=" * 60)

    ok, fail = 0, 0
    for name, config in DATASETS.items():
        result = download_dataset(name, config)
        ok += result
        fail += not result

    print(f"\n📊  Download complete: {ok} succeeded, {fail} failed")
    if fail:
        print("     Re-run --download to retry failed datasets (existing files are skipped)")
    return fail == 0


# ─────────────────────────────────────────────
# DB Initialisation
# ─────────────────────────────────────────────

def init_db(con: duckdb.DuckDBPyConnection):
    """Create all tables from schema.sql."""
    if not os.path.exists(SCHEMA_PATH):
        print(f"  ❌ {SCHEMA_PATH} not found — cannot initialise DB")
        sys.exit(1)

    with open(SCHEMA_PATH) as f:
        sql = f.read()

    for stmt in sql.split(";"):
        stmt = stmt.strip()
        if stmt and not stmt.startswith("--"):
            try:
                con.execute(stmt)
            except Exception as e:
                # Warn but don't stop — index conflicts are harmless
                if "already exists" not in str(e).lower():
                    print(f"  ⚠️  Schema: {e}")

    print("  ✅ Database schema ready")


# ─────────────────────────────────────────────
# Load Functions — one per dataset
# ─────────────────────────────────────────────

def load_pluto(con):
    fp = DATA_DIR / "pluto.csv"
    if not fp.exists():
        print("  ⏭️  PLUTO: not found, skipping")
        return

    print("  📦 Loading PLUTO — building profiles...")
    con.execute(f"""
        INSERT OR REPLACE INTO buildings
        SELECT
            CAST("BIN" AS VARCHAR)              AS bin,
            "Borough"                           AS borough,
            CAST("Block" AS VARCHAR)            AS block,
            CAST("Lot" AS VARCHAR)              AS lot,
            "Address"                           AS address,
            CAST("ZipCode" AS VARCHAR)          AS zipcode,
            TRY_CAST("BoroCode" AS INTEGER)     AS borough_code,
            TRY_CAST("NumFloors" AS INTEGER)    AS num_floors,
            TRY_CAST("YearBuilt" AS INTEGER)    AS year_built,
            "BldgClass"                         AS building_class,
            "LandUse"                           AS land_use,
            TRY_CAST("UnitsRes" AS INTEGER)     AS residential_units,
            TRY_CAST("UnitsTotal" AS INTEGER)   AS total_units,
            TRY_CAST("LotArea" AS DOUBLE)       AS lot_area,
            TRY_CAST("BldgArea" AS DOUBLE)      AS building_area,
            NULL                                AS construction_type,
            "OwnerName"                         AS owner_name,
            TRY_CAST("Latitude" AS DOUBLE)      AS latitude,
            TRY_CAST("Longitude" AS DOUBLE)     AS longitude,
            0.0                                 AS risk_score,
            CURRENT_TIMESTAMP                   AS last_updated
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true)
        WHERE "BIN" IS NOT NULL
          AND "BIN" != '0'
          AND "BIN" != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    print(f"  ✅ PLUTO: {n:,} buildings")


def load_registrations(con):
    fp = DATA_DIR / "registrations.csv"
    if not fp.exists():
        print("  ⏭️  Registrations: not found, skipping")
        return

    print("  📦 Loading Multiple Dwelling Registrations...")
    # Store into owner_portfolio seed (will be recomputed in --portfolio)
    # Also enriches buildings with registration status
    con.execute(f"""
        INSERT OR IGNORE INTO owner_portfolio (owner_name, total_buildings,
            total_open_violations, total_class_c_violations,
            total_ecb_balance_due, avg_violations_per_building, bins)
        SELECT
            "ContactDescription"    AS owner_name,
            COUNT(DISTINCT "RegistrationID") AS total_buildings,
            0, 0, 0.0, 0.0,
            STRING_AGG(CAST("BIN" AS VARCHAR), ',')
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "ContactDescription" IS NOT NULL
          AND "BIN" IS NOT NULL
        GROUP BY "ContactDescription"
    """)
    n = con.execute("SELECT COUNT(*) FROM owner_portfolio").fetchone()[0]
    print(f"  ✅ Registrations: {n:,} unique owners seeded")


def load_registration_contacts(con):
    fp = DATA_DIR / "registration_contacts.csv"
    if not fp.exists():
        print("  ⏭️  Registration Contacts: not found, skipping")
        return

    print("  📦 Loading Registration Contacts — owner/management names...")
    # Used downstream in portfolio analysis and brief generation
    # Merge into owner_portfolio by name
    con.execute(f"""
        INSERT OR IGNORE INTO owner_portfolio (owner_name, total_buildings,
            total_open_violations, total_class_c_violations,
            total_ecb_balance_due, avg_violations_per_building, bins)
        SELECT
            COALESCE("CorporationName", "FirstName" || ' ' || "LastName") AS owner_name,
            COUNT(DISTINCT "RegistrationID") AS total_buildings,
            0, 0, 0.0, 0.0,
            STRING_AGG(CAST("RegistrationID" AS VARCHAR), ',')
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "Type" IN ('HeadOfficer', 'IndividualOwner', 'CorporateOwner')
          AND COALESCE("CorporationName", "FirstName") IS NOT NULL
        GROUP BY owner_name
    """)
    n = con.execute("SELECT COUNT(*) FROM owner_portfolio").fetchone()[0]
    print(f"  ✅ Registration Contacts: owner_portfolio now {n:,} records")


def load_dob_violations(con):
    fp = DATA_DIR / "dob_violations.csv"
    if not fp.exists():
        print("  ⏭️  DOB Violations: not found, skipping")
        return

    print("  📦 Loading DOB Violations...")
    con.execute(f"""
        INSERT OR IGNORE INTO dob_violations
        SELECT
            ROW_NUMBER() OVER ()                            AS id,
            CAST("BIN" AS VARCHAR)                          AS bin,
            CAST("BLOCK" AS VARCHAR)                        AS block,
            CAST("LOT" AS VARCHAR)                          AS lot,
            "VIOLATION_TYPE"                                AS violation_type,
            "VIOLATION_NUMBER"                              AS violation_number,
            "VIOLATION_CATEGORY"                            AS violation_category,
            "DESCRIPTION"                                   AS description,
            TRY_CAST("DISPOSITION_DATE" AS DATE)            AS disposition_date,
            "DISPOSITION_COMMENTS"                          AS disposition_comments,
            TRY_CAST("ISSUE_DATE" AS DATE)                  AS issue_date,
            CASE
                WHEN "VIOLATION_TYPE" IN ('LL6291','AEUHAZ','IMEGNCY','LL1081') THEN 'CRITICAL'
                WHEN "VIOLATION_TYPE" IN ('ACC1','HBLVIO','P*CLSS1')           THEN 'HIGH'
                WHEN "VIOLATION_TYPE" IN ('UB','COMPBLD')                      THEN 'MEDIUM'
                ELSE 'LOW'
            END                                             AS severity,
            CASE WHEN "DISPOSITION_DATE" IS NULL THEN TRUE ELSE FALSE END AS is_active
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """)
    total  = con.execute("SELECT COUNT(*) FROM dob_violations").fetchone()[0]
    active = con.execute("SELECT COUNT(*) FROM dob_violations WHERE is_active").fetchone()[0]
    print(f"  ✅ DOB Violations: {total:,} total, {active:,} active")


def load_dob_safety_violations(con):
    fp = DATA_DIR / "dob_safety_violations.csv"
    if not fp.exists():
        print("  ⏭️  DOB Safety Violations: not found, skipping")
        return

    print("  📦 Loading DOB Safety Violations...")
    con.execute(f"""
        INSERT OR IGNORE INTO dob_safety_violations
        SELECT
            ROW_NUMBER() OVER ()                    AS id,
            CAST("BIN" AS VARCHAR)                  AS bin,
            "VIOLATION_NUMBER"                      AS violation_number,
            "VIOLATION_TYPE"                        AS violation_type,
            "VIOLATION_DESCRIPTION"                 AS violation_description,
            TRY_CAST("ISSUE_DATE" AS DATE)          AS issue_date,
            COALESCE("STATUS", 'OPEN')              AS status
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM dob_safety_violations").fetchone()[0]
    print(f"  ✅ DOB Safety Violations: {n:,} records")


def load_dob_ecb_violations(con):
    fp = DATA_DIR / "dob_ecb_violations.csv"
    if not fp.exists():
        print("  ⏭️  DOB ECB Violations: not found, skipping")
        return

    print("  📦 Loading DOB ECB Violations — penalty/neglect data...")
    con.execute(f"""
        INSERT OR IGNORE INTO dob_ecb_violations
        SELECT
            "ECB_VIOLATION_NUMBER"                          AS ecb_violation_number,
            CAST("BIN" AS VARCHAR)                          AS bin,
            CAST("BLOCK" AS VARCHAR)                        AS block,
            CAST("LOT" AS VARCHAR)                          AS lot,
            "VIOLATION_TYPE"                                AS violation_type,
            "VIOLATION_DESCRIPTION"                         AS violation_description,
            "INFRACTION_CODE"                               AS infraction_code,
            "SECTION_LAW_DESCRIPTION"                       AS section_law_description,
            TRY_CAST("PENALTY_IMPOSED" AS DOUBLE)           AS penalty_imposed,
            TRY_CAST("AMOUNT_PAID" AS DOUBLE)               AS amount_paid,
            TRY_CAST("BALANCE_DUE" AS DOUBLE)               AS balance_due,
            TRY_CAST("HEARING_DATE" AS DATE)                AS hearing_date,
            "HEARING_STATUS"                                AS hearing_status,
            TRY_CAST("SERVED_DATE" AS DATE)                 AS served_date,
            TRY_CAST("ISSUE_DATE" AS DATE)                  AS issue_date,
            CASE
                WHEN TRY_CAST("PENALTY_IMPOSED" AS DOUBLE) > 10000 THEN 'CRITICAL'
                WHEN TRY_CAST("PENALTY_IMPOSED" AS DOUBLE) > 2500  THEN 'HIGH'
                WHEN TRY_CAST("PENALTY_IMPOSED" AS DOUBLE) > 500   THEN 'MEDIUM'
                ELSE 'LOW'
            END                                             AS severity,
            CASE WHEN TRY_CAST("BALANCE_DUE" AS DOUBLE) > 0
                 THEN TRUE ELSE FALSE END                   AS is_active
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """)
    total   = con.execute("SELECT COUNT(*) FROM dob_ecb_violations").fetchone()[0]
    unpaid  = con.execute("SELECT COUNT(*) FROM dob_ecb_violations WHERE is_active").fetchone()[0]
    bal     = con.execute("SELECT COALESCE(SUM(balance_due),0) FROM dob_ecb_violations").fetchone()[0]
    print(f"  ✅ DOB ECB Violations: {total:,} total, {unpaid:,} with unpaid balance (${bal:,.0f} owed)")


def load_hpd_violations(con):
    fp = DATA_DIR / "hpd_violations.csv"
    if not fp.exists():
        print("  ⏭️  HPD Violations: not found, skipping")
        return

    print("  📦 Loading HPD Housing Violations...")
    con.execute(f"""
        INSERT OR IGNORE INTO hpd_violations
        SELECT
            TRY_CAST("ViolationID" AS INTEGER)              AS violation_id,
            CAST("BIN" AS VARCHAR)                          AS bin,
            TRY_CAST("BuildingID" AS INTEGER)               AS building_id,
            "BoroID"                                        AS borough_id,
            "Block"                                         AS block,
            "Lot"                                           AS lot,
            "Apartment"                                     AS apartment,
            "Story"                                         AS story,
            "Class"                                         AS violation_class,
            TRY_CAST("InspectionDate" AS DATE)              AS inspection_date,
            TRY_CAST("ApprovedDate" AS DATE)                AS approved_date,
            TRY_CAST("OriginalCertifyByDate" AS DATE)       AS original_certify_by_date,
            TRY_CAST("OriginalCorrectByDate" AS DATE)       AS original_correct_by_date,
            TRY_CAST("NewCertifyByDate" AS DATE)            AS new_certify_by_date,
            TRY_CAST("NewCorrectByDate" AS DATE)            AS new_correct_by_date,
            TRY_CAST("CertifiedDismissedDatetime" AS TIMESTAMP) AS certified_dismissed_datetime,
            "OrderNumber"                                   AS order_number,
            "NOVID"                                         AS nov_id,
            "NOVDescription"                                AS nov_description,
            TRY_CAST("NOVIssuedDate" AS DATE)               AS nov_issueddate,
            "CurrentStatus"                                 AS current_status,
            TRY_CAST("CurrentStatusDate" AS DATE)           AS current_status_date
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """)
    total   = con.execute("SELECT COUNT(*) FROM hpd_violations").fetchone()[0]
    class_c = con.execute("""
        SELECT COUNT(*) FROM hpd_violations
        WHERE violation_class = 'C' AND current_status != 'CLOSE'
    """).fetchone()[0]
    print(f"  ✅ HPD Violations: {total:,} total, {class_c:,} open Class C (immediately hazardous)")


def load_hpd_complaints(con):
    fp = DATA_DIR / "hpd_complaints.csv"
    if not fp.exists():
        print("  ⏭️  HPD Complaints: not found, skipping")
        return

    print("  📦 Loading HPD Complaints & Problems...")
    con.execute(f"""
        INSERT OR IGNORE INTO hpd_complaints
        SELECT
            TRY_CAST("ComplaintID" AS INTEGER)              AS complaint_id,
            CAST("BIN" AS VARCHAR)                          AS bin,
            TRY_CAST("BuildingID" AS INTEGER)               AS building_id,
            "BoroID"                                        AS borough_id,
            "Block"                                         AS block,
            "Lot"                                           AS lot,
            "Apartment"                                     AS apartment,
            "Status"                                        AS status,
            TRY_CAST("StatusDate" AS DATE)                  AS status_date,
            "Type"                                          AS complaint_type,
            "MajorCategoryID"                               AS major_category,
            "MinorCategoryID"                               AS minor_category,
            "CodeID"                                        AS code,
            "StatusDescription"                             AS problem_description,
            "StatusDescription"                             AS status_description,
            TRY_CAST("ReceivedDate" AS DATE)                AS received_date
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM hpd_complaints").fetchone()[0]
    print(f"  ✅ HPD Complaints: {n:,} records")


def load_fire_prevention_inspections(con):
    fp = DATA_DIR / "fire_prevention_inspections.csv"
    if not fp.exists():
        print("  ⏭️  Fire Prevention Inspections: not found, skipping")
        return

    print("  📦 Loading Bureau of Fire Prevention Inspections...")
    print("      (sprinkler / standpipe / suppression system status)")
    con.execute(f"""
        INSERT OR IGNORE INTO fire_prevention_inspections
        SELECT
            ROW_NUMBER() OVER ()                            AS id,
            CAST("BIN" AS VARCHAR)                          AS bin,
            "PREMISESADDRESS"                               AS address,
            "BOROUGH"                                       AS borough,
            TRY_CAST("INSPECTIONDATE" AS DATE)              AS inspection_date,
            "INSPECTIONTYPE"                                AS inspection_type,
            "RESULT"                                        AS result,
            "VIOLATIONDESCRIPTION"                          AS violation_description,
            "CERTIFICATENUMBER"                             AS certificate_number,
            TRY_CAST("EXPIRATIONDATE" AS DATE)              AS expiration_date,
            CASE WHEN UPPER("RESULT") = 'PASSED' THEN TRUE ELSE FALSE END AS is_compliant
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """)
    total    = con.execute("SELECT COUNT(*) FROM fire_prevention_inspections").fetchone()[0]
    failed   = con.execute("SELECT COUNT(*) FROM fire_prevention_inspections WHERE NOT is_compliant").fetchone()[0]
    print(f"  ✅ Fire Prevention: {total:,} inspections, {failed:,} non-compliant")


def load_fire_incidents(con):
    fp = DATA_DIR / "fire_incidents.csv"
    if not fp.exists():
        print("  ⏭️  Fire Incidents: not found, skipping")
        return

    print("  📦 Loading Fire Incident Dispatch Data...")
    con.execute(f"""
        INSERT OR IGNORE INTO fire_incidents
        SELECT
            "STARFIRE_INCIDENT_ID"                              AS incident_id,
            TRY_CAST("INCIDENT_DATETIME" AS TIMESTAMP)          AS incident_datetime,
            "INCIDENT_TYPE_DESC"                                AS incident_type_desc,
            "INCIDENT_BOROUGH"                                  AS incident_borough,
            "ZIPCODE"                                           AS zipcode,
            "POLICEPRECINCT"                                    AS policeprecinct,
            "INCIDENT_CLASSIFICATION"                           AS incident_classification,
            "INCIDENT_CLASSIFICATION_GROUP"                     AS incident_classification_group,
            TRY_CAST("DISPATCH_RESPONSE_SECONDS_QY" AS INTEGER) AS dispatch_response_seconds,
            TRY_CAST("INCIDENT_RESPONSE_SECONDS_QY" AS INTEGER) AS incident_response_seconds,
            TRY_CAST("INCIDENT_TRAVEL_TM_SECONDS_QY" AS INTEGER) AS incident_travel_seconds,
            TRY_CAST("ENGINES_ASSIGNED_QUANTITY" AS INTEGER)    AS engines_assigned,
            TRY_CAST("LADDERS_ASSIGNED_QUANTITY" AS INTEGER)    AS ladders_assigned,
            TRY_CAST("OTHER_UNITS_ASSIGNED_QUANTITY" AS INTEGER) AS other_units_assigned,
            TRY_CAST("LATITUDE" AS DOUBLE)                      AS latitude,
            TRY_CAST("LONGITUDE" AS DOUBLE)                     AS longitude,
            NULL                                                AS bin
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_incidents").fetchone()[0]
    print(f"  ✅ Fire Incidents: {n:,} dispatch records")


def load_fire_company_incidents(con):
    fp = DATA_DIR / "fire_company_incidents.csv"
    if not fp.exists():
        print("  ⏭️  Fire Company Incidents: not found, skipping")
        return

    print("  📦 Loading Fire Company Incidents — tactical detail layer...")
    con.execute(f"""
        INSERT OR IGNORE INTO fire_company_incidents
        SELECT
            ROW_NUMBER() OVER ()                                AS id,
            "IM_INCIDENT_KEY"                                   AS im_incident_key,
            "INCIDENT_TYPE_DESC"                                AS incident_type_desc,
            TRY_CAST("INCIDENT_DATE_TIME" AS TIMESTAMP)         AS incident_date_time,
            TRY_CAST("ARRIVAL_DATE_TIME" AS TIMESTAMP)          AS arrival_date_time,
            TRY_CAST("LAST_UNIT_CLEARED_DATE_TIME" AS TIMESTAMP) AS last_unit_cleared_date_time,
            "HIGHEST_ALARM_LEVEL"                               AS highest_alarm_level,
            TRY_CAST("TOTAL_INCIDENT_DURATION" AS INTEGER)      AS total_incident_duration,
            "ACTION_TAKEN1_DESC"                                AS action_taken_primary,
            "ACTION_TAKEN2_DESC"                                AS action_taken_secondary,
            "PROPERTY_USE_DESC"                                 AS property_use_desc,
            "STREET_HIGHWAY"                                    AS street_highway,
            "ZIP_CODE"                                          AS zip_code,
            "BOROUGH_DESC"                                      AS borough_desc,
            "FLOOR_OF_FIRE_ORIGIN"                              AS floor_of_fire_origin,
            CASE WHEN "FIRE_ORIGIN_BELOW_GRADE" = 'Y'
                 THEN TRUE ELSE FALSE END                       AS fire_origin_below_grade,
            "FIRE_SPREAD_DESC"                                  AS fire_spread_desc,
            "DETECTOR_PRESENCE_DESC"                            AS detector_presence_desc,
            "AES_PRESENCE_DESC"                                 AS aes_presence_desc,
            "STANDPIPE_SYS_PRESENT_DESC"                        AS standpipe_system_type_desc,
            TRY_CAST("LATITUDE" AS DOUBLE)                      AS latitude,
            TRY_CAST("LONGITUDE" AS DOUBLE)                     AS longitude
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_company_incidents").fetchone()[0]
    print(f"  ✅ Fire Company Incidents: {n:,} records")


def load_ems_incidents(con):
    fp = DATA_DIR / "ems_incidents.csv"
    if not fp.exists():
        print("  ⏭️  EMS Incidents: not found, skipping")
        return

    print("  📦 Loading EMS Incident Dispatch Data...")
    con.execute(f"""
        INSERT OR IGNORE INTO ems_incidents
        SELECT
            "CAD_INCIDENT_ID"                                       AS cad_incident_id,
            TRY_CAST("INCIDENT_DATETIME" AS TIMESTAMP)              AS incident_datetime,
            "INITIAL_CALL_TYPE"                                     AS initial_call_type,
            "FINAL_CALL_TYPE"                                       AS final_call_type,
            "INITIAL_SEVERITY_LEVEL_CODE"                           AS initial_severity_level,
            "FINAL_SEVERITY_LEVEL_CODE"                             AS final_severity_level,
            "INCIDENT_DISPOSITION_CODE"                             AS incident_disposition,
            "BOROUGH"                                               AS borough,
            "ZIPCODE"                                               AS zipcode,
            "POLICEPRECINCT"                                        AS policeprecinct,
            "CITYCOUNCILDISTRICT"                                   AS citycouncildistrict,
            "COMMUNITYDISTRICT"                                     AS communitydistrict,
            TRY_CAST("DISPATCH_RESPONSE_SECONDS_QY" AS INTEGER)     AS dispatch_response_seconds,
            TRY_CAST("INCIDENT_RESPONSE_SECONDS_QY" AS INTEGER)     AS incident_response_seconds,
            TRY_CAST("INCIDENT_TRAVEL_TM_SECONDS_QY" AS INTEGER)    AS incident_travel_seconds,
            TRY_CAST("LATITUDE" AS DOUBLE)                          AS latitude,
            TRY_CAST("LONGITUDE" AS DOUBLE)                         AS longitude
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM ems_incidents").fetchone()[0]
    print(f"  ✅ EMS Incidents: {n:,} dispatch records")


def load_311(con):
    fp = DATA_DIR / "311_requests.csv"
    if not fp.exists():
        print("  ⏭️  311 Requests: not found, skipping")
        return

    print("  📦 Loading 311 Safety Complaints...")
    con.execute(f"""
        INSERT OR IGNORE INTO service_requests_311
        SELECT
            "unique_key"                                    AS unique_key,
            TRY_CAST("created_date" AS TIMESTAMP)           AS created_date,
            TRY_CAST("closed_date" AS TIMESTAMP)            AS closed_date,
            "agency"                                        AS agency,
            "agency_name"                                   AS agency_name,
            "complaint_type"                                AS complaint_type,
            "descriptor"                                    AS descriptor,
            "location_type"                                 AS location_type,
            "incident_zip"                                  AS incident_zip,
            "incident_address"                              AS incident_address,
            "city"                                          AS city,
            "borough"                                       AS borough,
            TRY_CAST("latitude" AS DOUBLE)                  AS latitude,
            TRY_CAST("longitude" AS DOUBLE)                 AS longitude,
            NULL                                            AS bin,
            "status"                                        AS status,
            "resolution_description"                        AS resolution_description
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM service_requests_311").fetchone()[0]
    print(f"  ✅ 311 Requests: {n:,} safety-relevant complaints")


def load_fire_hydrants(con):
    fp = DATA_DIR / "fire_hydrants.csv"
    if not fp.exists():
        print("  ⏭️  Fire Hydrants: not found, skipping")
        return

    print("  📦 Loading Fire Hydrant Locations...")
    con.execute(f"""
        INSERT OR IGNORE INTO fire_hydrants
        SELECT
            ROW_NUMBER() OVER ()                AS id,
            TRY_CAST("LATITUDE" AS DOUBLE)      AS latitude,
            TRY_CAST("LONGITUDE" AS DOUBLE)     AS longitude,
            "UNITID"                            AS unitid,
            "BOROUGH"                           AS borough
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "LATITUDE" IS NOT NULL AND "LONGITUDE" IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_hydrants").fetchone()[0]
    print(f"  ✅ Fire Hydrants: {n:,} locations")


def load_hospitals(con):
    fp = DATA_DIR / "hospitals.csv"
    if not fp.exists():
        print("  ⏭️  Hospitals: not found, skipping")
        return

    print("  📦 Loading Hospital / Trauma Center Locations...")
    con.execute(f"""
        INSERT OR IGNORE INTO hospitals
        SELECT
            "Facility Name"                     AS facility_name,
            "Facility Type"                     AS facility_type,
            "Borough"                           AS borough,
            "Location 1"                        AS address,
            "Phone Number"                      AS phone,
            TRY_CAST("Latitude" AS DOUBLE)      AS latitude,
            TRY_CAST("Longitude" AS DOUBLE)     AS longitude
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    print(f"  ✅ Hospitals: {n:,} facilities")


def load_elevators(con):
    fp = DATA_DIR / "elevators.csv"
    if not fp.exists():
        print("  ⏭️  Elevators: not found, skipping")
        return

    print("  📦 Loading Elevator Device Details...")
    con.execute(f"""
        INSERT OR IGNORE INTO elevators
        SELECT
            ROW_NUMBER() OVER ()                    AS id,
            CAST("BIN" AS VARCHAR)                  AS bin,
            "DEVICENUMBER"                          AS device_number,
            "DEVICETYPE"                            AS device_type,
            "FLOORFROM"                             AS floor_from,
            "FLOORTO"                               AS floor_to,
            "SPEED"                                 AS speed,
            "CAPACITY"                              AS capacity,
            TRY_CAST("APPROVALDATE" AS DATE)        AS approval_date,
            COALESCE("DEVICESTATUS", 'UNKNOWN')     AS status
        FROM read_csv_auto('{fp}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "BIN" IS NOT NULL
    """)
    total = con.execute("SELECT COUNT(*) FROM elevators").fetchone()[0]
    oos   = con.execute("SELECT COUNT(*) FROM elevators WHERE status != 'ACTIVE'").fetchone()[0]
    print(f"  ✅ Elevators: {total:,} devices, {oos:,} not active")


def load_all(con):
    print("\n" + "=" * 60)
    print("📦  LOADING ALL DATASETS INTO DUCKDB")
    print("=" * 60)

    init_db(con)

    # CORE
    load_pluto(con)
    load_registrations(con)
    load_registration_contacts(con)

    # HAZARD
    load_dob_violations(con)
    load_dob_safety_violations(con)
    load_dob_ecb_violations(con)
    load_hpd_violations(con)
    load_hpd_complaints(con)
    load_fire_prevention_inspections(con)

    # INCIDENTS
    load_fire_incidents(con)
    load_fire_company_incidents(con)
    load_ems_incidents(con)

    # 311
    load_311(con)

    # INFRASTRUCTURE
    load_fire_hydrants(con)
    load_hospitals(con)
    load_elevators(con)

    # Summary
    print("\n" + "=" * 60)
    print("📊  DATABASE SUMMARY")
    print("=" * 60)
    for (table,) in con.execute("SHOW TABLES").fetchall():
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"  {table:<35} {n:>12,} rows")
        except Exception:
            print(f"  {table:<35} {'(empty)':>12}")

    db_mb = os.path.getsize(DB_PATH) / 1024 / 1024
    print(f"\n  💾 Database size: {db_mb:.1f} MB")


# ─────────────────────────────────────────────
# Owner Portfolio Computation
# ─────────────────────────────────────────────

def compute_owner_portfolio(con):
    """
    Rebuild owner_portfolio from violations data.
    Identifies landlords with high violation counts across
    multiple buildings — a key neglect signal in the brief.
    """
    print("\n" + "=" * 60)
    print("🏢  COMPUTING OWNER PORTFOLIO ANALYSIS")
    print("=" * 60)

    con.execute("DELETE FROM owner_portfolio")

    con.execute("""
        INSERT INTO owner_portfolio
        SELECT
            b.owner_name,
            COUNT(DISTINCT b.bin)                               AS total_buildings,
            COUNT(DISTINCT dv.id) FILTER (WHERE dv.is_active)   AS total_open_violations,
            COUNT(DISTINCT hv.violation_id)
                FILTER (WHERE hv.violation_class = 'C'
                          AND hv.current_status != 'CLOSE')     AS total_class_c_violations,
            COALESCE(SUM(ecb.balance_due), 0)                   AS total_ecb_balance_due,
            COALESCE(
                COUNT(DISTINCT dv.id) FILTER (WHERE dv.is_active)
                    / NULLIF(COUNT(DISTINCT b.bin), 0)
            , 0)                                                AS avg_violations_per_building,
            STRING_AGG(DISTINCT b.bin, ',')                     AS bins
        FROM buildings b
        LEFT JOIN dob_violations dv      ON b.bin = dv.bin
        LEFT JOIN hpd_violations hv      ON b.bin = hv.bin
        LEFT JOIN dob_ecb_violations ecb ON b.bin = ecb.bin AND ecb.is_active
        WHERE b.owner_name IS NOT NULL
        GROUP BY b.owner_name
        HAVING COUNT(DISTINCT b.bin) > 0
    """)

    total   = con.execute("SELECT COUNT(*) FROM owner_portfolio").fetchone()[0]
    neglect = con.execute("""
        SELECT COUNT(*) FROM owner_portfolio
        WHERE total_class_c_violations > 5 OR total_ecb_balance_due > 50000
    """).fetchone()[0]
    print(f"  ✅ Portfolio: {total:,} owners, {neglect:,} flagged as high-neglect")


# ─────────────────────────────────────────────
# Risk Score Computation
# ─────────────────────────────────────────────

def compute_risk_scores(con):
    """
    Composite risk score per building across all data layers.

    Weights are tuned for first-responder tactical value:
    - Class C HPD violations (immediately hazardous) = highest weight
    - Prior fire incidents = strong structural risk signal
    - ECB unpaid balance = owner neglect / deferred maintenance
    - Fire prevention non-compliance = direct suppression risk
    - DOB violations = general structural/safety risk
    - 311 complaint velocity = real-time community signal
    """
    print("\n" + "=" * 60)
    print("🧮  COMPUTING BUILDING RISK SCORES")
    print("=" * 60)

    # Seed: one row per building that has any violation
    print("  Seeding risk score table...")
    con.execute("""
        INSERT OR REPLACE INTO building_risk_scores (bin, overall_risk_score)
        SELECT bin, 0.0 FROM buildings
    """)

    # Active DOB violations
    print("  → DOB violations...")
    con.execute("""
        UPDATE building_risk_scores
        SET active_dob_violations = sub.cnt
        FROM (
            SELECT bin, COUNT(*) AS cnt
            FROM dob_violations WHERE is_active
            GROUP BY bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Active ECB violations (unpaid)
    print("  → ECB violations...")
    con.execute("""
        UPDATE building_risk_scores
        SET active_ecb_violations = sub.cnt
        FROM (
            SELECT bin, COUNT(*) AS cnt
            FROM dob_ecb_violations WHERE is_active
            GROUP BY bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # HPD Class C (immediately hazardous)
    print("  → HPD Class C/B violations...")
    con.execute("""
        UPDATE building_risk_scores
        SET active_hpd_class_c = sub.cnt
        FROM (
            SELECT bin, COUNT(*) AS cnt
            FROM hpd_violations
            WHERE violation_class = 'C' AND current_status != 'CLOSE'
            GROUP BY bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    con.execute("""
        UPDATE building_risk_scores
        SET active_hpd_class_b = sub.cnt
        FROM (
            SELECT bin, COUNT(*) AS cnt
            FROM hpd_violations
            WHERE violation_class = 'B' AND current_status != 'CLOSE'
            GROUP BY bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Prior fire incidents
    print("  → Fire incident history...")
    con.execute("""
        UPDATE building_risk_scores
        SET prior_fire_incidents = sub.cnt
        FROM (
            SELECT bin, COUNT(*) AS cnt
            FROM fire_incidents WHERE bin IS NOT NULL
            GROUP BY bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Prior EMS incidents (geo-matched — within ~50m, no BIN on EMS data)
    print("  → EMS incident history (geo-proximity)...")
    con.execute("""
        UPDATE building_risk_scores
        SET prior_ems_incidents = sub.cnt
        FROM (
            SELECT b.bin, COUNT(*) AS cnt
            FROM buildings b
            JOIN ems_incidents e ON (
                ABS(b.latitude  - e.latitude)  < 0.0005
            AND ABS(b.longitude - e.longitude) < 0.0005
            )
            GROUP BY b.bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # 311 complaint velocity — 30 / 90 / 365 days
    print("  → 311 complaint velocity...")
    con.execute("""
        UPDATE building_risk_scores
        SET complaint_velocity_30d = sub.cnt
        FROM (
            SELECT b.bin, COUNT(*) AS cnt
            FROM buildings b
            JOIN service_requests_311 s
              ON LOWER(b.address) = LOWER(s.incident_address)
            WHERE s.created_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY b.bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    con.execute("""
        UPDATE building_risk_scores
        SET complaint_velocity_90d = sub.cnt
        FROM (
            SELECT b.bin, COUNT(*) AS cnt
            FROM buildings b
            JOIN service_requests_311 s
              ON LOWER(b.address) = LOWER(s.incident_address)
            WHERE s.created_date >= CURRENT_DATE - INTERVAL '90 days'
            GROUP BY b.bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Fire prevention compliance flag
    print("  → Fire suppression compliance...")
    con.execute("""
        UPDATE building_risk_scores
        SET last_fdny_inspection_pass = sub.latest_pass
        FROM (
            SELECT bin,
                   BOOL_OR(is_compliant) AS latest_pass
            FROM fire_prevention_inspections
            GROUP BY bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Elevator counts
    print("  → Elevator status...")
    con.execute("""
        UPDATE building_risk_scores
        SET
            elevator_count          = sub.total,
            elevator_out_of_service = sub.oos
        FROM (
            SELECT bin,
                   COUNT(*)                                             AS total,
                   COUNT(*) FILTER (WHERE status != 'ACTIVE')          AS oos
            FROM elevators GROUP BY bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Nearest hydrant (approximate — Euclidean degrees, good enough for brief)
    print("  → Nearest hydrant distance...")
    con.execute("""
        UPDATE building_risk_scores
        SET nearest_hydrant_ft = sub.dist_ft
        FROM (
            SELECT b.bin,
                   MIN(SQRT(
                       POW((b.latitude  - h.latitude)  * 364000, 2) +
                       POW((b.longitude - h.longitude) * 288200, 2)
                   )) AS dist_ft
            FROM buildings b, fire_hydrants h
            WHERE ABS(b.latitude  - h.latitude)  < 0.005
              AND ABS(b.longitude - h.longitude) < 0.005
            GROUP BY b.bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Nearest hospital
    print("  → Nearest hospital...")
    con.execute("""
        UPDATE building_risk_scores
        SET
            nearest_hospital    = sub.name,
            nearest_hospital_mi = sub.dist_mi
        FROM (
            SELECT b.bin,
                   FIRST(h.facility_name ORDER BY dist_mi) AS name,
                   MIN(SQRT(
                       POW((b.latitude  - h.latitude)  * 69.0, 2) +
                       POW((b.longitude - h.longitude) * 54.6, 2)
                   )) AS dist_mi
            FROM buildings b, hospitals h
            GROUP BY b.bin
        ) sub
        WHERE building_risk_scores.bin = sub.bin
    """)

    # Final composite score
    print("  → Computing composite risk scores...")
    con.execute("""
        UPDATE building_risk_scores SET overall_risk_score =
            COALESCE(active_hpd_class_c,    0) * 6.0  +  -- immediately hazardous
            COALESCE(prior_fire_incidents,  0) * 4.0  +  -- proven fire history
            COALESCE(active_ecb_violations, 0) * 3.0  +  -- unpaid fines = neglect
            COALESCE(active_dob_violations, 0) * 2.5  +  -- structural issues
            COALESCE(active_hpd_class_b,    0) * 2.0  +  -- hazardous violations
            COALESCE(complaint_velocity_30d,0) * 2.0  +  -- recent complaint surge
            COALESCE(prior_ems_incidents,   0) * 1.5  +  -- EMS call history
            COALESCE(complaint_velocity_90d,0) * 1.0  +
            CASE WHEN last_fdny_inspection_pass = FALSE THEN 15.0 ELSE 0.0 END +
            CASE WHEN elevator_out_of_service > 0       THEN 5.0  ELSE 0.0 END
    """)

    # Update buildings table with rolled-up score
    con.execute("""
        UPDATE buildings SET risk_score = brs.overall_risk_score
        FROM building_risk_scores brs
        WHERE buildings.bin = brs.bin
    """)

    scored    = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score > 0").fetchone()[0]
    high_risk = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score > 30").fetchone()[0]
    critical  = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score > 60").fetchone()[0]
    print(f"  ✅ Risk scores: {scored:,} buildings scored")
    print(f"     🟡 High risk (>30):     {high_risk:,}")
    print(f"     🔴 Critical (>60):      {critical:,}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="FirstResponder Copilot — Data Ingestion Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python ingest.py --all               # Full pipeline (recommended first run)
  python ingest.py --download          # Only download CSVs
  python ingest.py --load              # Only load into DuckDB (CSVs must exist)
  python ingest.py --risk              # Only recompute risk scores
  python ingest.py --portfolio         # Only recompute owner portfolios
        """
    )
    parser.add_argument("--download",  action="store_true", help="Download all datasets from NYC Open Data")
    parser.add_argument("--load",      action="store_true", help="Load CSVs into DuckDB")
    parser.add_argument("--risk",      action="store_true", help="Compute building risk scores")
    parser.add_argument("--portfolio", action="store_true", help="Compute owner portfolio analysis")
    parser.add_argument("--all",       action="store_true", help="Full pipeline: download + load + risk + portfolio")

    args = parser.parse_args()

    if not any(vars(args).values()):
        parser.print_help()
        sys.exit(1)

    start = datetime.now()

    if args.download or args.all:
        download_all()

    if args.load or args.all:
        os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
        con = duckdb.connect(DB_PATH)
        load_all(con)
        con.close()

    if args.portfolio or args.all:
        con = duckdb.connect(DB_PATH)
        compute_owner_portfolio(con)
        con.close()

    if args.risk or args.all:
        con = duckdb.connect(DB_PATH)
        compute_risk_scores(con)
        con.close()

    elapsed = datetime.now() - start
    print(f"\n🎉  Done in {elapsed}. Database ready at: {DB_PATH}")


if __name__ == "__main__":
    main()
