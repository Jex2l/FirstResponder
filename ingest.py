"""
FirstResponder Copilot — Data Ingestion Pipeline
Fixed: uses DELETE+INSERT (no INSERT OR IGNORE/REPLACE conflicts),
dynamic column detection handles any case/naming variant.

Usage:
    python ingest.py --download
    python ingest.py --load
    python ingest.py --portfolio
    python ingest.py --risk
    python ingest.py --all
"""

import os
import sys
import argparse
import requests
import duckdb
from pathlib import Path
from datetime import datetime

DATA_DIR     = Path("data/raw")
DB_PATH      = "data/responder.duckdb"
SCHEMA_PATH  = "schema.sql"
SOCRATA_BASE = "https://data.cityofnewyork.us/resource"

DATASETS = {
    "pluto": {
        "id": "64uk-42ks", "limit": 900_000, "filter": None,
        "filename": "pluto.csv",
        "description": "Building profiles"
    },
    "registrations": {
        "id": "tesw-yqqr", "limit": 500_000, "filter": None,
        "filename": "registrations.csv",
        "description": "Multiple dwelling registrations"
    },
    "registration_contacts": {
        "id": "feu5-w2e2", "limit": 1_000_000, "filter": None,
        "filename": "registration_contacts.csv",
        "description": "Owner names and management companies"
    },
    "dob_violations": {
        "id": "3h2n-5cm9", "limit": 2_000_000, "filter": None,
        "filename": "dob_violations.csv",
        "description": "DOB violations"
    },
    "dob_safety_violations": {
        "id": "855j-jady", "limit": 500_000, "filter": None,
        "filename": "dob_safety_violations.csv",
        "description": "DOB safety violations"
    },
    "dob_ecb_violations": {
        "id": "6bgk-3dad", "limit": 1_000_000, "filter": None,
        "filename": "dob_ecb_violations.csv",
        "description": "ECB violations"
    },
    "hpd_violations": {
        "id": "wvxf-dwi5", "limit": 3_000_000, "filter": None,
        "filename": "hpd_violations.csv",
        "description": "HPD violations"
    },
    "hpd_complaints": {
        "id": "ygpa-z7cr", "limit": 3_000_000, "filter": None,
        "filename": "hpd_complaints.csv",
        "description": "HPD complaints"
    },
    "fire_prevention_inspections": {
        "id": "ssq6-fkht", "limit": 500_000, "filter": None,
        "filename": "fire_prevention_inspections.csv",
        "description": "FDNY Bureau of Fire Prevention"
    },
    "fire_incidents": {
        "id": "8m42-w767", "limit": 2_000_000, "filter": None,
        "filename": "fire_incidents.csv",
        "description": "Fire incident dispatch"
    },
    "fire_company_incidents": {
        "id": "tm6d-hbzd", "limit": 2_000_000, "filter": None,
        "filename": "fire_company_incidents.csv",
        "description": "Fire company incidents"
    },
    "ems_incidents": {
        "id": "76xm-jjuj", "limit": 2_000_000, "filter": None,
        "filename": "ems_incidents.csv",
        "description": "EMS dispatch"
    },
    "311_requests": {
        "id": "erm2-nwe9", "limit": 500_000,
        "filter": ("complaint_type in('HEATING','PLUMBING','GENERAL CONSTRUCTION',"
                   "'Heat/Hot Water','ELECTRIC','Gas Leak','STRUCTURAL','SAFETY',"
                   "'Hazardous Materials','Illegal Conversion','Building/Use')"),
        "filename": "311_requests.csv",
        "description": "311 safety complaints"
    },
    "fire_hydrants": {
        "id": "23d2-ttdp", "limit": 200_000, "filter": None,
        "filename": "fire_hydrants.csv",
        "description": "Fire hydrant locations"
    },
    "hospitals": {
        "id": "ji82-xba5", "limit": 10000,
        "filter": "facsubgrp='HOSPITALS AND CLINICS'",
        "filename": "hospitals.csv",
        "description": "NYC hospitals and health facilities"
    },
    "elevators": {
        "id": "juyv-2jek", "limit": 500_000, "filter": None,
        "filename": "elevators.csv",
        "description": "Elevator device details"
    },
}


# ─────────────────────────────────────────────
# Download
# ─────────────────────────────────────────────

def download_dataset(name, config):
    filepath = DATA_DIR / config["filename"]
    if filepath.exists():
        size_mb = filepath.stat().st_size / (1024 * 1024)
        print(f"  ⏭️  {name}: already exists ({size_mb:.1f} MB)")
        return True
    url = f"{SOCRATA_BASE}/{config['id']}.csv"
    params = {"$limit": config["limit"]}
    if config.get("filter"):
        params["$where"] = config["filter"]
    print(f"  ⬇️  {name}: {config['description']}")
    try:
        resp = requests.get(url, params=params, stream=True, timeout=300)
        resp.raise_for_status()
        total = 0
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=131_072):
                f.write(chunk)
                total += len(chunk)
        print(f"  ✅ {name}: {total/1024/1024:.1f} MB")
        return True
    except Exception as e:
        print(f"  ❌ {name}: {e}")
        return False


def download_all():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("\n" + "=" * 60)
    print("📥  DOWNLOADING NYC OPEN DATA — 16 DATASETS")
    print("=" * 60)
    ok, fail = 0, 0
    for name, config in DATASETS.items():
        result = download_dataset(name, config)
        ok += result
        fail += not result
    print(f"\n📊  {ok} succeeded, {fail} failed")
    return fail == 0


# ─────────────────────────────────────────────
# DB Init
# ─────────────────────────────────────────────

def init_db(con):
    if not os.path.exists(SCHEMA_PATH):
        print(f"  ❌ {SCHEMA_PATH} not found")
        sys.exit(1)
    with open(SCHEMA_PATH) as f:
        sql = f.read()
    try:
        con.execute(sql)
    except Exception:
        for stmt in sql.split(";"):
            stmt = stmt.strip()
            if stmt and not stmt.startswith("--"):
                try:
                    con.execute(stmt)
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        print(f"  ⚠️  Schema: {e}")
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "buildings" not in tables:
        print(f"  ❌ Schema failed! Tables: {tables}")
        sys.exit(1)
    print("  ✅ Database schema ready")


# ─────────────────────────────────────────────
# Column detection helpers
# ─────────────────────────────────────────────

def fp_str(fp):
    """Convert path to forward-slash string safe for DuckDB SQL."""
    return str(fp).replace("\\", "/")


def get_csv_columns(con, fp):
    """Return lowercase set of column names in a CSV."""
    try:
        desc = con.execute(
            f"SELECT * FROM read_csv_auto('{fp_str(fp)}', "
            f"header=true, ignore_errors=true) LIMIT 0"
        ).description
        return {c[0].lower() for c in desc}
    except Exception as e:
        print(f"  ⚠️  Cannot read columns from {fp}: {e}")
        return set()


def col(available, *candidates):
    """Return first candidate column found (quoted), else NULL."""
    for c in candidates:
        if c.lower() in available:
            return f'"{c}"'
    return "NULL"


# ─────────────────────────────────────────────
# Load functions — all use DELETE then INSERT
# ─────────────────────────────────────────────

def load_pluto(con):
    fp = DATA_DIR / "pluto.csv"
    if not fp.exists():
        print("  ⏭️  PLUTO: not found, skipping")
        return
    print("  📦 Loading PLUTO — building profiles...")
    # PLUTO has no BIN column — uses BBL (confirmed lowercase)
    con.execute("DELETE FROM buildings")
    con.execute(f"""
        INSERT INTO buildings
        SELECT
            CAST("bbl" AS VARCHAR)              AS bin,
            "borough"                           AS borough,
            CAST("block" AS VARCHAR)            AS block,
            CAST("lot" AS VARCHAR)              AS lot,
            "address"                           AS address,
            CAST("zipcode" AS VARCHAR)          AS zipcode,
            TRY_CAST("borocode" AS INTEGER)     AS borough_code,
            TRY_CAST("numfloors" AS INTEGER)    AS num_floors,
            TRY_CAST("yearbuilt" AS INTEGER)    AS year_built,
            "bldgclass"                         AS building_class,
            "landuse"                           AS land_use,
            TRY_CAST("unitsres" AS INTEGER)     AS residential_units,
            TRY_CAST("unitstotal" AS INTEGER)   AS total_units,
            TRY_CAST("lotarea" AS DOUBLE)       AS lot_area,
            TRY_CAST("bldgarea" AS DOUBLE)      AS building_area,
            NULL                                AS construction_type,
            "ownername"                         AS owner_name,
            TRY_CAST("latitude" AS DOUBLE)      AS latitude,
            TRY_CAST("longitude" AS DOUBLE)     AS longitude,
            0.0                                 AS risk_score,
            CURRENT_TIMESTAMP                   AS last_updated
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "bbl" IS NOT NULL
          AND "bbl" != '0'
          AND "bbl" != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    print(f"  ✅ PLUTO: {n:,} buildings")


def load_registrations(con):
    fp = DATA_DIR / "registrations.csv"
    if not fp.exists():
        print("  ⏭️  Registrations: not found, skipping")
        return
    print("  📦 Loading Multiple Dwelling Registrations...")
    cols = get_csv_columns(con, fp)
    c_contact = col(cols, "ContactDescription", "contactdescription",
                    "OwnersBusinessName", "ownername")
    c_regid   = col(cols, "RegistrationID", "registrationid")
    c_bin     = col(cols, "BIN", "bin")
    con.execute("DELETE FROM owner_portfolio")
    con.execute(f"""
        INSERT INTO owner_portfolio (owner_name, total_buildings,
            total_open_violations, total_class_c_violations,
            total_ecb_balance_due, avg_violations_per_building, bins)
        SELECT
            {c_contact}                                     AS owner_name,
            COUNT(DISTINCT {c_regid})                       AS total_buildings,
            0, 0, 0.0, 0.0,
            STRING_AGG(CAST({c_bin} AS VARCHAR), ',')
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_contact} IS NOT NULL
          AND {c_bin} IS NOT NULL
        GROUP BY {c_contact}
    """)
    n = con.execute("SELECT COUNT(*) FROM owner_portfolio").fetchone()[0]
    print(f"  ✅ Registrations: {n:,} unique owners")


def load_registration_contacts(con):
    fp = DATA_DIR / "registration_contacts.csv"
    if not fp.exists():
        print("  ⏭️  Registration Contacts: not found, skipping")
        return
    print("  📦 Loading Registration Contacts...")
    cols = get_csv_columns(con, fp)
    c_corp  = col(cols, "CorporationName", "corporationname")
    c_first = col(cols, "FirstName", "firstname")
    c_last  = col(cols, "LastName", "lastname")
    c_reg   = col(cols, "RegistrationID", "registrationid")
    c_type  = col(cols, "Type", "type", "ContactType", "contacttype")
    con.execute(f"""
        INSERT INTO owner_portfolio (owner_name, total_buildings,
            total_open_violations, total_class_c_violations,
            total_ecb_balance_due, avg_violations_per_building, bins)
        SELECT
            COALESCE({c_corp},
                TRIM(CONCAT(COALESCE({c_first},''), ' ', COALESCE({c_last},''))))
                                                        AS owner_name,
            COUNT(DISTINCT {c_reg})                     AS total_buildings,
            0, 0, 0.0, 0.0,
            STRING_AGG(CAST({c_reg} AS VARCHAR), ',')
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_type} IN ('HeadOfficer','IndividualOwner','CorporateOwner')
          AND COALESCE({c_corp}, {c_first}) IS NOT NULL
        GROUP BY owner_name
        HAVING owner_name NOT IN (
            SELECT owner_name FROM owner_portfolio WHERE owner_name IS NOT NULL
        )
    """)
    n = con.execute("SELECT COUNT(*) FROM owner_portfolio").fetchone()[0]
    print(f"  ✅ Registration Contacts: owner_portfolio now {n:,} records")


def load_dob_violations(con):
    fp = DATA_DIR / "dob_violations.csv"
    if not fp.exists():
        print("  ⏭️  DOB Violations: not found, skipping")
        return
    print("  📦 Loading DOB Violations...")
    cols = get_csv_columns(con, fp)
    c_bin     = col(cols, "BIN", "bin")
    c_blk     = col(cols, "BLOCK", "block", "Block")
    c_lot     = col(cols, "LOT", "lot", "Lot")
    c_vtype   = col(cols, "VIOLATION_TYPE", "violation_type", "ViolationType")
    c_vnum    = col(cols, "VIOLATION_NUMBER", "violation_number", "ViolationNumber")
    c_vcat    = col(cols, "VIOLATION_CATEGORY", "violation_category", "ViolationCategory")
    c_desc    = col(cols, "DESCRIPTION", "description", "ViolationDescription")
    c_disp_dt = col(cols, "DISPOSITION_DATE", "disposition_date", "DispositionDate")
    c_disp_cm = col(cols, "DISPOSITION_COMMENTS", "disposition_comments")
    c_iss_dt  = col(cols, "ISSUE_DATE", "issue_date", "IssueDate")
    con.execute("DELETE FROM dob_violations")
    con.execute(f"""
        INSERT INTO dob_violations
        SELECT
            ROW_NUMBER() OVER ()                        AS id,
            CAST({c_bin} AS VARCHAR)                    AS bin,
            CAST({c_blk} AS VARCHAR)                    AS block,
            CAST({c_lot} AS VARCHAR)                    AS lot,
            {c_vtype}                                   AS violation_type,
            {c_vnum}                                    AS violation_number,
            {c_vcat}                                    AS violation_category,
            {c_desc}                                    AS description,
            TRY_CAST({c_disp_dt} AS DATE)               AS disposition_date,
            {c_disp_cm}                                 AS disposition_comments,
            TRY_CAST({c_iss_dt} AS DATE)                AS issue_date,
            CASE
                WHEN {c_vtype} IN ('LL6291','AEUHAZ','IMEGNCY','LL1081') THEN 'CRITICAL'
                WHEN {c_vtype} IN ('ACC1','HBLVIO','P*CLSS1')            THEN 'HIGH'
                WHEN {c_vtype} IN ('UB','COMPBLD')                       THEN 'MEDIUM'
                ELSE 'LOW'
            END                                         AS severity,
            CASE WHEN {c_disp_dt} IS NULL
                 THEN TRUE ELSE FALSE END               AS is_active
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_bin} IS NOT NULL
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
    cols = get_csv_columns(con, fp)
    c_bin    = col(cols, "BIN", "bin")
    c_vnum   = col(cols, "VIOLATION_NUMBER", "violation_number", "ViolationNumber")
    c_vtype  = col(cols, "VIOLATION_TYPE", "violation_type", "ViolationType")
    c_vdesc  = col(cols, "VIOLATION_DESCRIPTION", "violation_description",
                   "DESCRIPTION", "description")
    c_iss_dt = col(cols, "ISSUE_DATE", "issue_date", "IssueDate")
    c_status = col(cols, "STATUS", "status", "Status")
    con.execute("DELETE FROM dob_safety_violations")
    con.execute(f"""
        INSERT INTO dob_safety_violations
        SELECT
            ROW_NUMBER() OVER ()            AS id,
            CAST({c_bin} AS VARCHAR)        AS bin,
            {c_vnum}                        AS violation_number,
            {c_vtype}                       AS violation_type,
            {c_vdesc}                       AS violation_description,
            TRY_CAST({c_iss_dt} AS DATE)    AS issue_date,
            COALESCE({c_status}, 'OPEN')    AS status
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_bin} IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM dob_safety_violations").fetchone()[0]
    print(f"  ✅ DOB Safety Violations: {n:,} records")


def load_dob_ecb_violations(con):
    fp = DATA_DIR / "dob_ecb_violations.csv"
    if not fp.exists():
        print("  ⏭️  DOB ECB Violations: not found, skipping")
        return
    print("  📦 Loading DOB ECB Violations...")
    cols = get_csv_columns(con, fp)
    c_bin     = col(cols, "BIN", "bin")
    c_ecb_num = col(cols, "ECB_VIOLATION_NUMBER", "ecb_violation_number")
    c_blk     = col(cols, "BLOCK", "block", "Block")
    c_lot     = col(cols, "LOT", "lot", "Lot")
    c_vtype   = col(cols, "VIOLATION_TYPE", "violation_type")
    c_vdesc   = col(cols, "VIOLATION_DESCRIPTION", "violation_description")
    c_infr    = col(cols, "INFRACTION_CODE", "infraction_code")
    c_seclaw  = col(cols, "SECTION_LAW_DESCRIPTION", "section_law_description")
    c_pen     = col(cols, "PENALTY_IMPOSED", "penalty_imposed")
    c_paid    = col(cols, "AMOUNT_PAID", "amount_paid")
    c_bal     = col(cols, "BALANCE_DUE", "balance_due")
    c_hear_dt = col(cols, "HEARING_DATE", "hearing_date")
    c_hear_st = col(cols, "HEARING_STATUS", "hearing_status")
    c_serv_dt = col(cols, "SERVED_DATE", "served_date")
    c_iss_dt  = col(cols, "ISSUE_DATE", "issue_date")
    con.execute("DELETE FROM dob_ecb_violations")
    con.execute(f"""
        INSERT INTO dob_ecb_violations
        SELECT
            {c_ecb_num}                                 AS ecb_violation_number,
            CAST({c_bin} AS VARCHAR)                    AS bin,
            CAST({c_blk} AS VARCHAR)                    AS block,
            CAST({c_lot} AS VARCHAR)                    AS lot,
            {c_vtype}                                   AS violation_type,
            {c_vdesc}                                   AS violation_description,
            {c_infr}                                    AS infraction_code,
            {c_seclaw}                                  AS section_law_description,
            TRY_CAST({c_pen} AS DOUBLE)                 AS penalty_imposed,
            TRY_CAST({c_paid} AS DOUBLE)                AS amount_paid,
            TRY_CAST({c_bal} AS DOUBLE)                 AS balance_due,
            TRY_CAST({c_hear_dt} AS DATE)               AS hearing_date,
            {c_hear_st}                                 AS hearing_status,
            TRY_CAST({c_serv_dt} AS DATE)               AS served_date,
            TRY_CAST({c_iss_dt} AS DATE)                AS issue_date,
            CASE
                WHEN TRY_CAST({c_pen} AS DOUBLE) > 10000 THEN 'CRITICAL'
                WHEN TRY_CAST({c_pen} AS DOUBLE) > 2500  THEN 'HIGH'
                WHEN TRY_CAST({c_pen} AS DOUBLE) > 500   THEN 'MEDIUM'
                ELSE 'LOW'
            END                                         AS severity,
            CASE WHEN TRY_CAST({c_bal} AS DOUBLE) > 0
                 THEN TRUE ELSE FALSE END               AS is_active
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_bin} IS NOT NULL
    """)
    total  = con.execute("SELECT COUNT(*) FROM dob_ecb_violations").fetchone()[0]
    unpaid = con.execute("SELECT COUNT(*) FROM dob_ecb_violations WHERE is_active").fetchone()[0]
    bal    = con.execute("SELECT COALESCE(SUM(balance_due),0) FROM dob_ecb_violations").fetchone()[0]
    print(f"  ✅ DOB ECB: {total:,} total, {unpaid:,} unpaid (${bal:,.0f} owed)")


def load_hpd_violations(con):
    fp = DATA_DIR / "hpd_violations.csv"
    if not fp.exists():
        print("  ⏭️  HPD Violations: not found, skipping")
        return
    print("  📦 Loading HPD Housing Violations...")
    cols = get_csv_columns(con, fp)
    c_vid     = col(cols, "ViolationID", "violationid")
    c_bin     = col(cols, "BIN", "bin")
    c_bldgid  = col(cols, "BuildingID", "buildingid")
    c_boroid  = col(cols, "BoroID", "boroid")
    c_blk     = col(cols, "Block", "block")
    c_lot     = col(cols, "Lot", "lot")
    c_apt     = col(cols, "Apartment", "apartment")
    c_story   = col(cols, "Story", "story")
    c_cls     = col(cols, "Class", "class", "ViolationClass", "violationclass")
    c_insp_dt = col(cols, "InspectionDate", "inspectiondate")
    c_appr_dt = col(cols, "ApprovedDate", "approveddate")
    c_ocbd    = col(cols, "OriginalCertifyByDate", "originalcertifybydate")
    c_ocrd    = col(cols, "OriginalCorrectByDate", "originalcorrectbydate")
    c_ncbd    = col(cols, "NewCertifyByDate", "newcertifybydate")
    c_ncrd    = col(cols, "NewCorrectByDate", "newcorrectbydate")
    c_cdd     = col(cols, "CertifiedDismissedDatetime", "certifieddismisseddatetime")
    c_order   = col(cols, "OrderNumber", "ordernumber")
    c_novid   = col(cols, "NOVID", "novid")
    c_novdesc = col(cols, "NOVDescription", "novdescription")
    c_noviss  = col(cols, "NOVIssuedDate", "novissueddate")
    c_curst   = col(cols, "CurrentStatus", "currentstatus")
    c_curstd  = col(cols, "CurrentStatusDate", "currentstatusdate")
    con.execute("DELETE FROM hpd_violations")
    con.execute(f"""
        INSERT INTO hpd_violations
        SELECT
            TRY_CAST({c_vid} AS INTEGER)                AS violation_id,
            CAST({c_bin} AS VARCHAR)                    AS bin,
            TRY_CAST({c_bldgid} AS INTEGER)             AS building_id,
            {c_boroid}                                  AS borough_id,
            {c_blk}                                     AS block,
            {c_lot}                                     AS lot,
            {c_apt}                                     AS apartment,
            {c_story}                                   AS story,
            {c_cls}                                     AS violation_class,
            TRY_CAST({c_insp_dt} AS DATE)               AS inspection_date,
            TRY_CAST({c_appr_dt} AS DATE)               AS approved_date,
            TRY_CAST({c_ocbd} AS DATE)                  AS original_certify_by_date,
            TRY_CAST({c_ocrd} AS DATE)                  AS original_correct_by_date,
            TRY_CAST({c_ncbd} AS DATE)                  AS new_certify_by_date,
            TRY_CAST({c_ncrd} AS DATE)                  AS new_correct_by_date,
            TRY_CAST({c_cdd} AS TIMESTAMP)              AS certified_dismissed_datetime,
            {c_order}                                   AS order_number,
            {c_novid}                                   AS nov_id,
            {c_novdesc}                                 AS nov_description,
            TRY_CAST({c_noviss} AS DATE)                AS nov_issueddate,
            {c_curst}                                   AS current_status,
            TRY_CAST({c_curstd} AS DATE)                AS current_status_date
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_bin} IS NOT NULL
    """)
    total   = con.execute("SELECT COUNT(*) FROM hpd_violations").fetchone()[0]
    class_c = con.execute("""
        SELECT COUNT(*) FROM hpd_violations
        WHERE violation_class = 'C' AND current_status != 'CLOSE'
    """).fetchone()[0]
    print(f"  ✅ HPD Violations: {total:,} total, {class_c:,} open Class C")


def load_hpd_complaints(con):
    fp = DATA_DIR / "hpd_complaints.csv"
    if not fp.exists():
        print("  ⏭️  HPD Complaints: not found, skipping")
        return
    print("  📦 Loading HPD Complaints...")
    cols = get_csv_columns(con, fp)
    c_cid     = col(cols, "ComplaintID", "complaintid")
    c_bin     = col(cols, "BIN", "bin")
    c_bldgid  = col(cols, "BuildingID", "buildingid")
    c_boroid  = col(cols, "BoroID", "boroid")
    c_blk     = col(cols, "Block", "block")
    c_lot     = col(cols, "Lot", "lot")
    c_apt     = col(cols, "Apartment", "apartment")
    c_status  = col(cols, "Status", "status")
    c_statdt  = col(cols, "StatusDate", "statusdate")
    c_ctype   = col(cols, "Type", "type", "ComplaintType", "complainttype")
    c_majcat  = col(cols, "MajorCategoryID", "majorcategoryid",
                    "MajorCategory", "majorcategory")
    c_mincat  = col(cols, "MinorCategoryID", "minorcategoryid",
                    "MinorCategory", "minorcategory")
    c_code    = col(cols, "CodeID", "codeid", "Code", "code")
    c_stdesc  = col(cols, "StatusDescription", "statusdescription")
    c_recvdt  = col(cols, "ReceivedDate", "receiveddate")
    con.execute("DELETE FROM hpd_complaints")
    con.execute(f"""
        INSERT INTO hpd_complaints
        SELECT
            TRY_CAST({c_cid} AS INTEGER)        AS complaint_id,
            CAST({c_bin} AS VARCHAR)            AS bin,
            TRY_CAST({c_bldgid} AS INTEGER)     AS building_id,
            {c_boroid}                          AS borough_id,
            {c_blk}                             AS block,
            {c_lot}                             AS lot,
            {c_apt}                             AS apartment,
            {c_status}                          AS status,
            TRY_CAST({c_statdt} AS DATE)        AS status_date,
            {c_ctype}                           AS complaint_type,
            {c_majcat}                          AS major_category,
            {c_mincat}                          AS minor_category,
            {c_code}                            AS code,
            {c_stdesc}                          AS problem_description,
            {c_stdesc}                          AS status_description,
            TRY_CAST({c_recvdt} AS DATE)        AS received_date
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_bin} IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM hpd_complaints").fetchone()[0]
    print(f"  ✅ HPD Complaints: {n:,} records")


def load_fire_prevention_inspections(con):
    fp = DATA_DIR / "fire_prevention_inspections.csv"
    if not fp.exists():
        print("  ⏭️  Fire Prevention: not found, skipping")
        return
    print("  📦 Loading Fire Prevention Inspections...")
    cols = get_csv_columns(con, fp)
    c_bin     = col(cols, "BIN", "bin")
    c_addr    = col(cols, "PREMISESADDRESS", "premisesaddress", "Address", "address")
    c_boro    = col(cols, "BOROUGH", "borough", "Borough")
    c_insp_dt = col(cols, "INSPECTIONDATE", "inspectiondate", "InspectionDate")
    c_insp_tp = col(cols, "INSPECTIONTYPE", "inspectiontype", "InspectionType")
    c_result  = col(cols, "RESULT", "result", "Result")
    c_vdesc   = col(cols, "VIOLATIONDESCRIPTION", "violationdescription",
                    "ViolationDescription", "violation_description")
    c_certnum = col(cols, "CERTIFICATENUMBER", "certificatenumber")
    c_exp_dt  = col(cols, "EXPIRATIONDATE", "expirationdate", "ExpirationDate")
    con.execute("DELETE FROM fire_prevention_inspections")
    con.execute(f"""
        INSERT INTO fire_prevention_inspections
        SELECT
            ROW_NUMBER() OVER ()                        AS id,
            CAST({c_bin} AS VARCHAR)                    AS bin,
            {c_addr}                                    AS address,
            {c_boro}                                    AS borough,
            TRY_CAST({c_insp_dt} AS DATE)               AS inspection_date,
            {c_insp_tp}                                 AS inspection_type,
            {c_result}                                  AS result,
            {c_vdesc}                                   AS violation_description,
            {c_certnum}                                 AS certificate_number,
            TRY_CAST({c_exp_dt} AS DATE)                AS expiration_date,
            CASE WHEN UPPER({c_result}) = 'PASSED'
                 THEN TRUE ELSE FALSE END               AS is_compliant
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_bin} IS NOT NULL
    """)
    total  = con.execute("SELECT COUNT(*) FROM fire_prevention_inspections").fetchone()[0]
    failed = con.execute(
        "SELECT COUNT(*) FROM fire_prevention_inspections WHERE NOT is_compliant"
    ).fetchone()[0]
    print(f"  ✅ Fire Prevention: {total:,} inspections, {failed:,} non-compliant")


def load_fire_incidents(con):
    fp = DATA_DIR / "fire_incidents.csv"
    if not fp.exists():
        print("  ⏭️  Fire Incidents: not found, skipping")
        return
    print("  📦 Loading Fire Incidents...")
    cols = get_csv_columns(con, fp)
    c_id      = col(cols, "STARFIRE_INCIDENT_ID", "starfire_incident_id")
    c_dt      = col(cols, "INCIDENT_DATETIME", "incident_datetime")
    c_type    = col(cols, "INCIDENT_TYPE_DESC", "incident_type_desc")
    c_boro    = col(cols, "INCIDENT_BOROUGH", "incident_borough")
    c_zip     = col(cols, "ZIPCODE", "zipcode", "ZIP_CODE", "zip_code")
    c_prec    = col(cols, "POLICEPRECINCT", "policeprecinct")
    c_cls     = col(cols, "INCIDENT_CLASSIFICATION", "incident_classification")
    c_clsgrp  = col(cols, "INCIDENT_CLASSIFICATION_GROUP", "incident_classification_group")
    c_disp    = col(cols, "DISPATCH_RESPONSE_SECONDS_QY", "dispatch_response_seconds_qy")
    c_resp    = col(cols, "INCIDENT_RESPONSE_SECONDS_QY", "incident_response_seconds_qy")
    c_trav    = col(cols, "INCIDENT_TRAVEL_TM_SECONDS_QY", "incident_travel_tm_seconds_qy")
    c_eng     = col(cols, "ENGINES_ASSIGNED_QUANTITY", "engines_assigned_quantity")
    c_lad     = col(cols, "LADDERS_ASSIGNED_QUANTITY", "ladders_assigned_quantity")
    c_oth     = col(cols, "OTHER_UNITS_ASSIGNED_QUANTITY", "other_units_assigned_quantity")
    c_lat     = col(cols, "LATITUDE", "latitude")
    c_lon     = col(cols, "LONGITUDE", "longitude")
    con.execute("DELETE FROM fire_incidents")
    con.execute(f"""
        INSERT INTO fire_incidents
        SELECT
            {c_id}                                  AS incident_id,
            TRY_CAST({c_dt} AS TIMESTAMP)           AS incident_datetime,
            {c_type}                                AS incident_type_desc,
            {c_boro}                                AS incident_borough,
            {c_zip}                                 AS zipcode,
            {c_prec}                                AS policeprecinct,
            {c_cls}                                 AS incident_classification,
            {c_clsgrp}                              AS incident_classification_group,
            TRY_CAST({c_disp} AS INTEGER)           AS dispatch_response_seconds,
            TRY_CAST({c_resp} AS INTEGER)           AS incident_response_seconds,
            TRY_CAST({c_trav} AS INTEGER)           AS incident_travel_seconds,
            TRY_CAST({c_eng} AS INTEGER)            AS engines_assigned,
            TRY_CAST({c_lad} AS INTEGER)            AS ladders_assigned,
            TRY_CAST({c_oth} AS INTEGER)            AS other_units_assigned,
            TRY_CAST({c_lat} AS DOUBLE)             AS latitude,
            TRY_CAST({c_lon} AS DOUBLE)             AS longitude,
            NULL                                    AS bin
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_incidents").fetchone()[0]
    print(f"  ✅ Fire Incidents: {n:,} records")


def load_fire_company_incidents(con):
    fp = DATA_DIR / "fire_company_incidents.csv"
    if not fp.exists():
        print("  ⏭️  Fire Company Incidents: not found, skipping")
        return
    print("  📦 Loading Fire Company Incidents...")
    cols = get_csv_columns(con, fp)
    c_imkey   = col(cols, "IM_INCIDENT_KEY", "im_incident_key")
    c_type    = col(cols, "INCIDENT_TYPE_DESC", "incident_type_desc")
    c_dt      = col(cols, "INCIDENT_DATE_TIME", "incident_date_time")
    c_arr     = col(cols, "ARRIVAL_DATE_TIME", "arrival_date_time")
    c_clr     = col(cols, "LAST_UNIT_CLEARED_DATE_TIME", "last_unit_cleared_date_time")
    c_alarm   = col(cols, "HIGHEST_ALARM_LEVEL", "highest_alarm_level")
    c_dur     = col(cols, "TOTAL_INCIDENT_DURATION", "total_incident_duration")
    c_act1    = col(cols, "ACTION_TAKEN1_DESC", "action_taken1_desc")
    c_act2    = col(cols, "ACTION_TAKEN2_DESC", "action_taken2_desc")
    c_prop    = col(cols, "PROPERTY_USE_DESC", "property_use_desc")
    c_street  = col(cols, "STREET_HIGHWAY", "street_highway")
    c_zip     = col(cols, "ZIP_CODE", "zip_code", "ZIPCODE", "zipcode")
    c_boro    = col(cols, "BOROUGH_DESC", "borough_desc")
    c_floor   = col(cols, "FLOOR_OF_FIRE_ORIGIN", "floor_of_fire_origin")
    c_below   = col(cols, "FIRE_ORIGIN_BELOW_GRADE", "fire_origin_below_grade")
    c_spread  = col(cols, "FIRE_SPREAD_DESC", "fire_spread_desc")
    c_det     = col(cols, "DETECTOR_PRESENCE_DESC", "detector_presence_desc")
    c_aes     = col(cols, "AES_PRESENCE_DESC", "aes_presence_desc")
    c_stand   = col(cols, "STANDPIPE_SYS_PRESENT_DESC", "standpipe_sys_present_desc")
    c_lat     = col(cols, "LATITUDE", "latitude")
    c_lon     = col(cols, "LONGITUDE", "longitude")
    con.execute("DELETE FROM fire_company_incidents")
    con.execute(f"""
        INSERT INTO fire_company_incidents
        SELECT
            ROW_NUMBER() OVER ()                        AS id,
            {c_imkey}                                   AS im_incident_key,
            {c_type}                                    AS incident_type_desc,
            TRY_CAST({c_dt} AS TIMESTAMP)               AS incident_date_time,
            TRY_CAST({c_arr} AS TIMESTAMP)              AS arrival_date_time,
            TRY_CAST({c_clr} AS TIMESTAMP)              AS last_unit_cleared_date_time,
            {c_alarm}                                   AS highest_alarm_level,
            TRY_CAST({c_dur} AS INTEGER)                AS total_incident_duration,
            {c_act1}                                    AS action_taken_primary,
            {c_act2}                                    AS action_taken_secondary,
            {c_prop}                                    AS property_use_desc,
            {c_street}                                  AS street_highway,
            {c_zip}                                     AS zip_code,
            {c_boro}                                    AS borough_desc,
            {c_floor}                                   AS floor_of_fire_origin,
            CASE WHEN {c_below} = 'Y'
                 THEN TRUE ELSE FALSE END               AS fire_origin_below_grade,
            {c_spread}                                  AS fire_spread_desc,
            {c_det}                                     AS detector_presence_desc,
            {c_aes}                                     AS aes_presence_desc,
            {c_stand}                                   AS standpipe_system_type_desc,
            TRY_CAST({c_lat} AS DOUBLE)                 AS latitude,
            TRY_CAST({c_lon} AS DOUBLE)                 AS longitude
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_company_incidents").fetchone()[0]
    print(f"  ✅ Fire Company Incidents: {n:,} records")


def load_ems_incidents(con):
    fp = DATA_DIR / "ems_incidents.csv"
    if not fp.exists():
        print("  ⏭️  EMS Incidents: not found, skipping")
        return
    print("  📦 Loading EMS Incidents...")
    cols = get_csv_columns(con, fp)
    c_cad     = col(cols, "CAD_INCIDENT_ID", "cad_incident_id")
    c_dt      = col(cols, "INCIDENT_DATETIME", "incident_datetime")
    c_inct    = col(cols, "INITIAL_CALL_TYPE", "initial_call_type")
    c_finct   = col(cols, "FINAL_CALL_TYPE", "final_call_type")
    c_isev    = col(cols, "INITIAL_SEVERITY_LEVEL_CODE", "initial_severity_level_code")
    c_fsev    = col(cols, "FINAL_SEVERITY_LEVEL_CODE", "final_severity_level_code")
    c_disp    = col(cols, "INCIDENT_DISPOSITION_CODE", "incident_disposition_code")
    c_boro    = col(cols, "BOROUGH", "borough")
    c_zip     = col(cols, "ZIPCODE", "zipcode")
    c_prec    = col(cols, "POLICEPRECINCT", "policeprecinct")
    c_council = col(cols, "CITYCOUNCILDISTRICT", "citycouncildistrict")
    c_comm    = col(cols, "COMMUNITYDISTRICT", "communitydistrict")
    c_dresp   = col(cols, "DISPATCH_RESPONSE_SECONDS_QY", "dispatch_response_seconds_qy")
    c_iresp   = col(cols, "INCIDENT_RESPONSE_SECONDS_QY", "incident_response_seconds_qy")
    c_trav    = col(cols, "INCIDENT_TRAVEL_TM_SECONDS_QY", "incident_travel_tm_seconds_qy")
    c_lat     = col(cols, "LATITUDE", "latitude")
    c_lon     = col(cols, "LONGITUDE", "longitude")
    con.execute("DELETE FROM ems_incidents")
    con.execute(f"""
        INSERT INTO ems_incidents
        SELECT
            {c_cad}                                 AS cad_incident_id,
            TRY_CAST({c_dt} AS TIMESTAMP)           AS incident_datetime,
            {c_inct}                                AS initial_call_type,
            {c_finct}                               AS final_call_type,
            {c_isev}                                AS initial_severity_level,
            {c_fsev}                                AS final_severity_level,
            {c_disp}                                AS incident_disposition,
            {c_boro}                                AS borough,
            {c_zip}                                 AS zipcode,
            {c_prec}                                AS policeprecinct,
            {c_council}                             AS citycouncildistrict,
            {c_comm}                                AS communitydistrict,
            TRY_CAST({c_dresp} AS INTEGER)          AS dispatch_response_seconds,
            TRY_CAST({c_iresp} AS INTEGER)          AS incident_response_seconds,
            TRY_CAST({c_trav} AS INTEGER)           AS incident_travel_seconds,
            TRY_CAST({c_lat} AS DOUBLE)             AS latitude,
            TRY_CAST({c_lon} AS DOUBLE)             AS longitude
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM ems_incidents").fetchone()[0]
    print(f"  ✅ EMS Incidents: {n:,} records")


def load_311(con):
    fp = DATA_DIR / "311_requests.csv"
    if not fp.exists():
        print("  ⏭️  311 Requests: not found, skipping")
        return
    print("  📦 Loading 311 Safety Complaints...")
    cols = get_csv_columns(con, fp)
    c_ukey   = col(cols, "unique_key", "UniqueKey")
    c_crdt   = col(cols, "created_date", "CreatedDate")
    c_cldt   = col(cols, "closed_date", "ClosedDate")
    c_agency = col(cols, "agency", "Agency")
    c_agnam  = col(cols, "agency_name", "AgencyName")
    c_ctype  = col(cols, "complaint_type", "ComplaintType")
    c_desc   = col(cols, "descriptor", "Descriptor")
    c_loctyp = col(cols, "location_type", "LocationType")
    c_zip    = col(cols, "incident_zip", "IncidentZip")
    c_addr   = col(cols, "incident_address", "IncidentAddress")
    c_city   = col(cols, "city", "City")
    c_boro   = col(cols, "borough", "Borough")
    c_lat    = col(cols, "latitude", "Latitude")
    c_lon    = col(cols, "longitude", "Longitude")
    c_status = col(cols, "status", "Status")
    c_res    = col(cols, "resolution_description", "ResolutionDescription")
    con.execute("DELETE FROM service_requests_311")
    con.execute(f"""
        INSERT INTO service_requests_311
        SELECT
            {c_ukey}                            AS unique_key,
            TRY_CAST({c_crdt} AS TIMESTAMP)     AS created_date,
            TRY_CAST({c_cldt} AS TIMESTAMP)     AS closed_date,
            {c_agency}                          AS agency,
            {c_agnam}                           AS agency_name,
            {c_ctype}                           AS complaint_type,
            {c_desc}                            AS descriptor,
            {c_loctyp}                          AS location_type,
            {c_zip}                             AS incident_zip,
            {c_addr}                            AS incident_address,
            {c_city}                            AS city,
            {c_boro}                            AS borough,
            TRY_CAST({c_lat} AS DOUBLE)         AS latitude,
            TRY_CAST({c_lon} AS DOUBLE)         AS longitude,
            NULL                                AS bin,
            {c_status}                          AS status,
            {c_res}                             AS resolution_description
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM service_requests_311").fetchone()[0]
    print(f"  ✅ 311 Requests: {n:,} records")


def load_fire_hydrants(con):
    fp = DATA_DIR / "fire_hydrants.csv"
    if not fp.exists():
        print("  ⏭️  Fire Hydrants: not found, skipping")
        return
    print("  📦 Loading Fire Hydrant Locations...")
    cols = get_csv_columns(con, fp)
    c_lat    = col(cols, "LATITUDE", "latitude", "Latitude")
    c_lon    = col(cols, "LONGITUDE", "longitude", "Longitude")
    c_unitid = col(cols, "UNITID", "unitid", "UnitID")
    c_boro   = col(cols, "BOROUGH", "borough", "Borough")
    con.execute("DELETE FROM fire_hydrants")
    con.execute(f"""
        INSERT INTO fire_hydrants
        SELECT
            ROW_NUMBER() OVER ()            AS id,
            TRY_CAST({c_lat} AS DOUBLE)     AS latitude,
            TRY_CAST({c_lon} AS DOUBLE)     AS longitude,
            {c_unitid}                      AS unitid,
            {c_boro}                        AS borough
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_lat} IS NOT NULL AND {c_lon} IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_hydrants").fetchone()[0]
    print(f"  ✅ Fire Hydrants: {n:,} locations")


def load_hospitals(con):
    fp = DATA_DIR / "hospitals.csv"
    if not fp.exists():
        print("  ⏭️  Hospitals: not found, skipping")
        return
    print("  📦 Loading NYC Hospitals...")
    cols = get_csv_columns(con, fp)
    c_fname = col(cols, "facname", "FacName", "facility_name")
    c_ftype = col(cols, "factype", "FacType", "facility_type")
    c_boro  = col(cols, "boro", "Borough", "borough")
    c_addr  = col(cols, "address", "Address")
    c_lat   = col(cols, "latitude", "Latitude")
    c_lon   = col(cols, "longitude", "Longitude")
    con.execute("DELETE FROM hospitals")
    con.execute(f"""
        INSERT INTO hospitals
        SELECT
            {c_fname}                       AS facility_name,
            {c_ftype}                       AS facility_type,
            {c_boro}                        AS borough,
            {c_addr}                        AS address,
            NULL                            AS phone,
            TRY_CAST({c_lat} AS DOUBLE)     AS latitude,
            TRY_CAST({c_lon} AS DOUBLE)     AS longitude
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_ftype} IN (
            'HOSPITAL', 'ACUTE CARE HOSPITAL', 'OFF-CAMPUS EMERGENCY DEPARTMENT'
        )
        AND TRY_CAST({c_lat} AS DOUBLE) IS NOT NULL
        AND TRY_CAST({c_lat} AS DOUBLE) != 0.0
        AND TRY_CAST({c_lon} AS DOUBLE) IS NOT NULL
        AND TRY_CAST({c_lon} AS DOUBLE) != 0.0
    """)
    n = con.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    print(f"  ✅ Hospitals: {n:,} facilities")


def load_elevators(con):
    fp = DATA_DIR / "elevators.csv"
    if not fp.exists():
        print("  ⏭️  Elevators: not found, skipping")
        return
    print("  📦 Loading Elevator Details...")
    cols = get_csv_columns(con, fp)
    c_bin    = col(cols, "BIN", "bin")
    c_devnum = col(cols, "DEVICENUMBER", "devicenumber", "DeviceNumber")
    c_devtyp = col(cols, "DEVICETYPE", "devicetype", "DeviceType")
    c_ffrm   = col(cols, "FLOORFROM", "floorfrom", "FloorFrom")
    c_fto    = col(cols, "FLOORTO", "floorto", "FloorTo")
    c_speed  = col(cols, "SPEED", "speed", "Speed")
    c_cap    = col(cols, "CAPACITY", "capacity", "Capacity")
    c_appr   = col(cols, "APPROVALDATE", "approvaldate", "ApprovalDate")
    c_status = col(cols, "DEVICESTATUS", "devicestatus", "DeviceStatus",
                   "STATUS", "status")
    con.execute("DELETE FROM elevators")
    con.execute(f"""
        INSERT INTO elevators
        SELECT
            ROW_NUMBER() OVER ()                AS id,
            CAST({c_bin} AS VARCHAR)            AS bin,
            {c_devnum}                          AS device_number,
            {c_devtyp}                          AS device_type,
            {c_ffrm}                            AS floor_from,
            {c_fto}                             AS floor_to,
            {c_speed}                           AS speed,
            {c_cap}                             AS capacity,
            TRY_CAST({c_appr} AS DATE)          AS approval_date,
            COALESCE({c_status}, 'UNKNOWN')     AS status
        FROM read_csv_auto('{fp_str(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {c_bin} IS NOT NULL
    """)
    total = con.execute("SELECT COUNT(*) FROM elevators").fetchone()[0]
    oos   = con.execute(
        "SELECT COUNT(*) FROM elevators WHERE status != 'ACTIVE'"
    ).fetchone()[0]
    print(f"  ✅ Elevators: {total:,} devices, {oos:,} not active")


def load_all(con):
    print("\n" + "=" * 60)
    print("📦  LOADING ALL DATASETS INTO DUCKDB")
    print("=" * 60)
    init_db(con)
    load_pluto(con)
    load_registrations(con)
    load_registration_contacts(con)
    load_dob_violations(con)
    load_dob_safety_violations(con)
    load_dob_ecb_violations(con)
    load_hpd_violations(con)
    load_hpd_complaints(con)
    load_fire_prevention_inspections(con)
    load_fire_incidents(con)
    load_fire_company_incidents(con)
    load_ems_incidents(con)
    load_311(con)
    load_fire_hydrants(con)
    load_hospitals(con)
    load_elevators(con)
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
# Owner Portfolio
# ─────────────────────────────────────────────

def compute_owner_portfolio(con):
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
    print(f"  ✅ Portfolio: {total:,} owners, {neglect:,} high-neglect")


# ─────────────────────────────────────────────
# Risk Scores
# ─────────────────────────────────────────────

def compute_risk_scores(con):
    print("\n" + "=" * 60)
    print("🧮  COMPUTING BUILDING RISK SCORES")
    print("=" * 60)
    print("  Seeding risk score table...")
    con.execute("DELETE FROM building_risk_scores")
    con.execute("INSERT INTO building_risk_scores (bin, overall_risk_score) SELECT bin, 0.0 FROM buildings")

    steps = [
        ("DOB violations", """
            UPDATE building_risk_scores SET active_dob_violations = sub.cnt
            FROM (SELECT bin, COUNT(*) AS cnt FROM dob_violations WHERE is_active GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("ECB violations", """
            UPDATE building_risk_scores SET active_ecb_violations = sub.cnt
            FROM (SELECT bin, COUNT(*) AS cnt FROM dob_ecb_violations WHERE is_active GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("HPD Class C", """
            UPDATE building_risk_scores SET active_hpd_class_c = sub.cnt
            FROM (SELECT bin, COUNT(*) AS cnt FROM hpd_violations
                  WHERE violation_class = 'C' AND current_status != 'CLOSE' GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("HPD Class B", """
            UPDATE building_risk_scores SET active_hpd_class_b = sub.cnt
            FROM (SELECT bin, COUNT(*) AS cnt FROM hpd_violations
                  WHERE violation_class = 'B' AND current_status != 'CLOSE' GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("Fire history", """
            UPDATE building_risk_scores SET prior_fire_incidents = sub.cnt
            FROM (SELECT bin, COUNT(*) AS cnt FROM fire_incidents WHERE bin IS NOT NULL GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("EMS geo-proximity", """
            UPDATE building_risk_scores SET prior_ems_incidents = sub.cnt
            FROM (
                SELECT b.bin, COUNT(*) AS cnt FROM buildings b
                JOIN ems_incidents e ON (ABS(b.latitude-e.latitude)<0.0005 AND ABS(b.longitude-e.longitude)<0.0005)
                GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("311 velocity 30d", """
            UPDATE building_risk_scores SET complaint_velocity_30d = sub.cnt
            FROM (
                SELECT b.bin, COUNT(*) AS cnt FROM buildings b
                JOIN service_requests_311 s ON LOWER(b.address)=LOWER(s.incident_address)
                WHERE s.created_date >= CURRENT_DATE - INTERVAL '30 days' GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("311 velocity 90d", """
            UPDATE building_risk_scores SET complaint_velocity_90d = sub.cnt
            FROM (
                SELECT b.bin, COUNT(*) AS cnt FROM buildings b
                JOIN service_requests_311 s ON LOWER(b.address)=LOWER(s.incident_address)
                WHERE s.created_date >= CURRENT_DATE - INTERVAL '90 days' GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("Fire prevention compliance", """
            UPDATE building_risk_scores SET last_fdny_inspection_pass = sub.latest_pass
            FROM (SELECT bin, BOOL_OR(is_compliant) AS latest_pass
                  FROM fire_prevention_inspections GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("Elevator status", """
            UPDATE building_risk_scores SET elevator_count=sub.total, elevator_out_of_service=sub.oos
            FROM (SELECT bin, COUNT(*) AS total, COUNT(*) FILTER(WHERE status!='ACTIVE') AS oos
                  FROM elevators GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("Nearest hydrant", """
            UPDATE building_risk_scores SET nearest_hydrant_ft = sub.dist_ft
            FROM (
                SELECT b.bin,
                    MIN(SQRT(POW((b.latitude-h.latitude)*364000,2)+POW((b.longitude-h.longitude)*288200,2))) AS dist_ft
                FROM buildings b, fire_hydrants h
                WHERE ABS(b.latitude-h.latitude)<0.005 AND ABS(b.longitude-h.longitude)<0.005
                GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("Nearest hospital", """
            UPDATE building_risk_scores SET nearest_hospital=sub.name, nearest_hospital_mi=sub.dist_mi
            FROM (
                SELECT b.bin,
                    FIRST(h.facility_name ORDER BY dist_mi) AS name,
                    MIN(SQRT(POW((b.latitude-h.latitude)*69.0,2)+POW((b.longitude-h.longitude)*54.6,2))) AS dist_mi
                FROM buildings b, hospitals h GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("Composite score", """
            UPDATE building_risk_scores SET overall_risk_score =
                COALESCE(active_hpd_class_c,    0)*6.0 +
                COALESCE(prior_fire_incidents,  0)*4.0 +
                COALESCE(active_ecb_violations, 0)*3.0 +
                COALESCE(active_dob_violations, 0)*2.5 +
                COALESCE(active_hpd_class_b,    0)*2.0 +
                COALESCE(complaint_velocity_30d,0)*2.0 +
                COALESCE(prior_ems_incidents,   0)*1.5 +
                COALESCE(complaint_velocity_90d,0)*1.0 +
                CASE WHEN last_fdny_inspection_pass=FALSE THEN 15.0 ELSE 0.0 END +
                CASE WHEN elevator_out_of_service>0 THEN 5.0 ELSE 0.0 END
        """),
    ]
    for label, sql in steps:
        print(f"  → {label}...")
        con.execute(sql)

    con.execute("""
        UPDATE buildings SET risk_score = brs.overall_risk_score
        FROM building_risk_scores brs WHERE buildings.bin = brs.bin
    """)
    scored    = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score>0").fetchone()[0]
    high_risk = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score>30").fetchone()[0]
    critical  = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score>60").fetchone()[0]
    print(f"  ✅ Scored: {scored:,} buildings")
    print(f"     🟡 High risk (>30): {high_risk:,}")
    print(f"     🔴 Critical  (>60): {critical:,}")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="FirstResponder Copilot — Data Ingestion Pipeline"
    )
    parser.add_argument("--download",  action="store_true", help="Download all datasets")
    parser.add_argument("--load",      action="store_true", help="Load CSVs into DuckDB")
    parser.add_argument("--risk",      action="store_true", help="Compute risk scores")
    parser.add_argument("--portfolio", action="store_true", help="Compute owner portfolios")
    parser.add_argument("--all",       action="store_true", help="Full pipeline")
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
    print(f"\n🎉  Done in {elapsed}. Database: {DB_PATH}")


if __name__ == "__main__":
    main()
