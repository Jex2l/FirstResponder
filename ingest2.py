"""
FirstResponder Copilot — Data Ingestion Pipeline
Column names confirmed from actual CSV headers for all 16 datasets.
Uses DELETE + INSERT. No foreign key constraints (removed from schema).

Usage:
    python ingest2.py --download
    python ingest2.py --load
    python ingest2.py --portfolio
    python ingest2.py --risk
    python ingest2.py --all
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
    "pluto":                    {"id": "64uk-42ks", "limit": 900_000,   "filter": None, "filename": "pluto.csv"},
    "registrations":            {"id": "tesw-yqqr", "limit": 500_000,   "filter": None, "filename": "registrations.csv"},
    "registration_contacts":    {"id": "feu5-w2e2", "limit": 1_000_000, "filter": None, "filename": "registration_contacts.csv"},
    "dob_violations":           {"id": "3h2n-5cm9", "limit": 2_000_000, "filter": None, "filename": "dob_violations.csv"},
    "dob_safety_violations":    {"id": "855j-jady", "limit": 500_000,   "filter": None, "filename": "dob_safety_violations.csv"},
    "dob_ecb_violations":       {"id": "6bgk-3dad", "limit": 1_000_000, "filter": None, "filename": "dob_ecb_violations.csv"},
    "hpd_violations":           {"id": "wvxf-dwi5", "limit": 3_000_000, "filter": None, "filename": "hpd_violations.csv"},
    "hpd_complaints":           {"id": "ygpa-z7cr", "limit": 3_000_000, "filter": None, "filename": "hpd_complaints.csv"},
    "fire_prevention_inspections": {"id": "ssq6-fkht", "limit": 500_000, "filter": None, "filename": "fire_prevention_inspections.csv"},
    "fire_incidents":           {"id": "8m42-w767", "limit": 2_000_000, "filter": None, "filename": "fire_incidents.csv"},
    "fire_company_incidents":   {"id": "tm6d-hbzd", "limit": 2_000_000, "filter": None, "filename": "fire_company_incidents.csv"},
    "ems_incidents":            {"id": "76xm-jjuj", "limit": 2_000_000, "filter": None, "filename": "ems_incidents.csv"},
    "311_requests": {
        "id": "erm2-nwe9", "limit": 500_000,
        "filter": ("complaint_type in('HEATING','PLUMBING','GENERAL CONSTRUCTION',"
                   "'Heat/Hot Water','ELECTRIC','Gas Leak','STRUCTURAL','SAFETY',"
                   "'Hazardous Materials','Illegal Conversion','Building/Use')"),
        "filename": "311_requests.csv"
    },
    "fire_hydrants":  {"id": "23d2-ttdp", "limit": 200_000,  "filter": None, "filename": "fire_hydrants.csv"},
    "hospitals":      {"id": "ji82-xba5", "limit": 10_000,   "filter": "facsubgrp='HOSPITALS AND CLINICS'", "filename": "hospitals.csv"},
    "elevators":      {"id": "juyv-2jek", "limit": 500_000,  "filter": None, "filename": "elevators.csv"},
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def p(fp):
    """Windows-safe forward-slash path for DuckDB SQL strings."""
    return str(fp).replace("\\", "/")


def notnull(col):
    """WHERE clause: col is not null and not a zero/blank sentinel."""
    return f"{col} IS NOT NULL AND TRIM(CAST({col} AS VARCHAR)) NOT IN ('', '0', '0000000', '0000000000')"


# ─────────────────────────────────────────────
# Download
# ─────────────────────────────────────────────

def download_dataset(name, config):
    filepath = DATA_DIR / config["filename"]
    if filepath.exists():
        mb = filepath.stat().st_size / 1024 / 1024
        print(f"  ⏭️  {name}: already exists ({mb:.1f} MB)")
        return True
    url = f"{SOCRATA_BASE}/{config['id']}.csv"
    params = {"$limit": config["limit"]}
    if config.get("filter"):
        params["$where"] = config["filter"]
    print(f"  ⬇️  {name}...")
    try:
        r = requests.get(url, params=params, stream=True, timeout=300)
        r.raise_for_status()
        total = 0
        with open(filepath, "wb") as f:
            for chunk in r.iter_content(131_072):
                f.write(chunk); total += len(chunk)
        print(f"  ✅ {name}: {total/1024/1024:.1f} MB")
        return True
    except Exception as e:
        print(f"  ❌ {name}: {e}")
        return False


def download_all():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    print("\n" + "="*60 + "\n📥  DOWNLOADING 16 DATASETS\n" + "="*60)
    ok = fail = 0
    for name, cfg in DATASETS.items():
        ok += download_dataset(name, cfg)
        fail += not download_dataset(name, cfg)
    print(f"\n📊  {ok} ok, {fail} failed")


# ─────────────────────────────────────────────
# DB Init
# ─────────────────────────────────────────────

def init_db(con):
    if not os.path.exists(SCHEMA_PATH):
        print(f"  ❌ {SCHEMA_PATH} not found"); sys.exit(1)
    sql = open(SCHEMA_PATH).read()
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
        print(f"  ❌ Schema failed. Tables: {tables}"); sys.exit(1)
    print("  ✅ Database schema ready")


def clear_all(con):
    for t in ["building_risk_scores", "owner_portfolio", "elevators",
              "hospitals", "fire_hydrants", "service_requests_311",
              "ems_incidents", "fire_company_incidents", "fire_incidents",
              "fire_prevention_inspections", "hpd_complaints", "hpd_violations",
              "dob_ecb_violations", "dob_safety_violations", "dob_violations",
              "buildings"]:
        try:
            con.execute(f"DELETE FROM {t}")
        except Exception as e:
            print(f"  ⚠️  clear {t}: {e}")


# ─────────────────────────────────────────────
# Load functions — exact confirmed column names
# ─────────────────────────────────────────────

def load_pluto(con):
    # Confirmed cols: bbl, borough, block, lot, address, zipcode, borocode,
    # numfloors, yearbuilt, bldgclass, landuse, unitsres, unitstotal,
    # lotarea, bldgarea, ownername, latitude, longitude
    fp = DATA_DIR / "pluto.csv"
    if not fp.exists(): print("  ⏭️  PLUTO: missing"); return
    print("  📦 Loading PLUTO...")
    con.execute("DELETE FROM buildings")
    con.execute(f"""
        INSERT INTO buildings
        SELECT
            NULLIF(TRIM("bbl"), '')              AS bin,
            "borough"                            AS borough,
            CAST("block" AS VARCHAR)             AS block,
            CAST("lot" AS VARCHAR)               AS lot,
            "address"                            AS address,
            CAST("zipcode" AS VARCHAR)           AS zipcode,
            TRY_CAST("borocode" AS INTEGER)      AS borough_code,
            TRY_CAST("numfloors" AS INTEGER)     AS num_floors,
            TRY_CAST("yearbuilt" AS INTEGER)     AS year_built,
            "bldgclass"                          AS building_class,
            "landuse"                            AS land_use,
            TRY_CAST("unitsres" AS INTEGER)      AS residential_units,
            TRY_CAST("unitstotal" AS INTEGER)    AS total_units,
            TRY_CAST("lotarea" AS DOUBLE)        AS lot_area,
            TRY_CAST("bldgarea" AS DOUBLE)       AS building_area,
            NULL                                 AS construction_type,
            "ownername"                          AS owner_name,
            TRY_CAST("latitude" AS DOUBLE)       AS latitude,
            TRY_CAST("longitude" AS DOUBLE)      AS longitude,
            0.0                                  AS risk_score,
            CURRENT_TIMESTAMP                    AS last_updated
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE NULLIF(TRIM("bbl"), '') IS NOT NULL
          AND TRIM("bbl") NOT IN ('0', '0000000000')
    """)
    n = con.execute("SELECT COUNT(*) FROM buildings").fetchone()[0]
    print(f"  ✅ PLUTO: {n:,} buildings")


def load_registrations(con):
    # Confirmed cols: registrationid, buildingid, boroid, boro, housenumber,
    # streetname, zip, block, lot, bin (has BIN!)
    # No owner name here — handled by registration_contacts
    fp = DATA_DIR / "registrations.csv"
    if not fp.exists(): print("  ⏭️  Registrations: missing"); return
    print("  📦 Loading Registrations (count only — owner names in contacts)...")
    n = con.execute(f"""
        SELECT COUNT(*) FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
    """).fetchone()[0]
    print(f"  ✅ Registrations: {n:,} rows (owners loaded via registration_contacts)")


def load_registration_contacts(con):
    # Confirmed cols: registrationcontactid, registrationid, type,
    # contactdescription, corporationname, firstname, lastname
    fp = DATA_DIR / "registration_contacts.csv"
    if not fp.exists(): print("  ⏭️  Registration Contacts: missing"); return
    print("  📦 Loading Registration Contacts...")
    con.execute("DELETE FROM owner_portfolio")
    con.execute(f"""
        INSERT INTO owner_portfolio (owner_name, total_buildings,
            total_open_violations, total_class_c_violations,
            total_ecb_balance_due, avg_violations_per_building, bins)
        SELECT
            COALESCE(
                NULLIF(TRIM("corporationname"), ''),
                NULLIF(TRIM(CONCAT(
                    COALESCE("firstname",''), ' ', COALESCE("lastname",'')
                )), '')
            )                                           AS owner_name,
            COUNT(DISTINCT "registrationid")            AS total_buildings,
            0, 0, 0.0, 0.0,
            STRING_AGG(CAST("registrationid" AS VARCHAR), ',')
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "type" IN ('HeadOfficer','IndividualOwner','CorporateOwner')
          AND COALESCE(
                NULLIF(TRIM("corporationname"),''),
                NULLIF(TRIM("firstname"),'')
              ) IS NOT NULL
        GROUP BY owner_name
        HAVING owner_name IS NOT NULL AND TRIM(owner_name) != ''
    """)
    n = con.execute("SELECT COUNT(*) FROM owner_portfolio").fetchone()[0]
    print(f"  ✅ Registration Contacts: {n:,} unique owners")


def load_dob_violations(con):
    # Confirmed cols: bin, boro, block, lot, issue_date, violation_type_code,
    # violation_number, disposition_date, disposition_comments,
    # description, violation_category, violation_type
    fp = DATA_DIR / "dob_violations.csv"
    if not fp.exists(): print("  ⏭️  DOB Violations: missing"); return
    print("  📦 Loading DOB Violations...")
    con.execute("DELETE FROM dob_violations")
    con.execute(f"""
        INSERT INTO dob_violations
        SELECT
            ROW_NUMBER() OVER ()                    AS id,
            CAST("bin" AS VARCHAR)                  AS bin,
            CAST("block" AS VARCHAR)                AS block,
            CAST("lot" AS VARCHAR)                  AS lot,
            "violation_type"                        AS violation_type,
            "violation_number"                      AS violation_number,
            "violation_category"                    AS violation_category,
            "description"                           AS description,
            TRY_CAST("disposition_date" AS DATE)    AS disposition_date,
            "disposition_comments"                  AS disposition_comments,
            TRY_CAST("issue_date" AS DATE)          AS issue_date,
            CASE
                WHEN "violation_type" IN ('LL6291','AEUHAZ','IMEGNCY','LL1081') THEN 'CRITICAL'
                WHEN "violation_type" IN ('ACC1','HBLVIO','P*CLSS1')            THEN 'HIGH'
                WHEN "violation_type" IN ('UB','COMPBLD')                       THEN 'MEDIUM'
                ELSE 'LOW'
            END                                     AS severity,
            CASE WHEN "disposition_date" IS NULL
                 THEN TRUE ELSE FALSE END           AS is_active
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {notnull('"bin"')}
    """)
    total  = con.execute("SELECT COUNT(*) FROM dob_violations").fetchone()[0]
    active = con.execute("SELECT COUNT(*) FROM dob_violations WHERE is_active").fetchone()[0]
    print(f"  ✅ DOB Violations: {total:,} total, {active:,} active")


def load_dob_safety_violations(con):
    # Confirmed cols: bin, violation_issue_date, violation_number,
    # violation_type, violation_remarks, violation_status, bbl, block, lot
    fp = DATA_DIR / "dob_safety_violations.csv"
    if not fp.exists(): print("  ⏭️  DOB Safety: missing"); return
    print("  📦 Loading DOB Safety Violations...")
    con.execute("DELETE FROM dob_safety_violations")
    con.execute(f"""
        INSERT INTO dob_safety_violations
        SELECT
            ROW_NUMBER() OVER ()                        AS id,
            CAST("bin" AS VARCHAR)                      AS bin,
            "violation_number"                          AS violation_number,
            "violation_type"                            AS violation_type,
            "violation_remarks"                         AS violation_description,
            TRY_CAST("violation_issue_date" AS DATE)    AS issue_date,
            COALESCE("violation_status", 'OPEN')        AS status
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {notnull('"bin"')}
    """)
    n = con.execute("SELECT COUNT(*) FROM dob_safety_violations").fetchone()[0]
    print(f"  ✅ DOB Safety Violations: {n:,} records")


def load_dob_ecb_violations(con):
    # Confirmed cols: ecb_violation_number, bin, boro, block, lot,
    # hearing_date, served_date, issue_date, violation_type,
    # violation_description, penality_imposed (typo in source),
    # amount_paid, balance_due, infraction_code1, section_law_description1,
    # hearing_status
    fp = DATA_DIR / "dob_ecb_violations.csv"
    if not fp.exists(): print("  ⏭️  DOB ECB: missing"); return
    print("  📦 Loading DOB ECB Violations...")
    con.execute("DELETE FROM dob_ecb_violations")
    con.execute(f"""
        INSERT INTO dob_ecb_violations
        SELECT DISTINCT ON ("ecb_violation_number")
            "ecb_violation_number"                  AS ecb_violation_number,
            CAST("bin" AS VARCHAR)                  AS bin,
            CAST("block" AS VARCHAR)                AS block,
            CAST("lot" AS VARCHAR)                  AS lot,
            "violation_type"                        AS violation_type,
            "violation_description"                 AS violation_description,
            "infraction_code1"                      AS infraction_code,
            "section_law_description1"              AS section_law_description,
            TRY_CAST("penality_imposed" AS DOUBLE)  AS penalty_imposed,
            TRY_CAST("amount_paid" AS DOUBLE)       AS amount_paid,
            TRY_CAST("balance_due" AS DOUBLE)       AS balance_due,
            TRY_CAST("hearing_date" AS DATE)        AS hearing_date,
            "hearing_status"                        AS hearing_status,
            TRY_CAST("served_date" AS DATE)         AS served_date,
            TRY_CAST("issue_date" AS DATE)          AS issue_date,
            CASE
                WHEN TRY_CAST("penality_imposed" AS DOUBLE) > 10000 THEN 'CRITICAL'
                WHEN TRY_CAST("penality_imposed" AS DOUBLE) > 2500  THEN 'HIGH'
                WHEN TRY_CAST("penality_imposed" AS DOUBLE) > 500   THEN 'MEDIUM'
                ELSE 'LOW'
            END                                     AS severity,
            CASE WHEN TRY_CAST("balance_due" AS DOUBLE) > 0
                 THEN TRUE ELSE FALSE END           AS is_active
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {notnull('"bin"')}
    """)
    total  = con.execute("SELECT COUNT(*) FROM dob_ecb_violations").fetchone()[0]
    unpaid = con.execute("SELECT COUNT(*) FROM dob_ecb_violations WHERE is_active").fetchone()[0]
    bal    = con.execute("SELECT COALESCE(SUM(balance_due),0) FROM dob_ecb_violations").fetchone()[0]
    print(f"  ✅ DOB ECB: {total:,} total, {unpaid:,} unpaid (${bal:,.0f} owed)")


def load_hpd_violations(con):
    # Confirmed cols: violationid, buildingid, boroid, apartment, story,
    # block, lot, class, inspectiondate, approveddate,
    # originalcertifybydate, originalcorrectbydate, newcertifybydate,
    # newcorrectbydate, certifieddate, ordernumber, novid, novdescription,
    # novissueddate, currentstatus, currentstatusdate, bin, bbl
    fp = DATA_DIR / "hpd_violations.csv"
    if not fp.exists(): print("  ⏭️  HPD Violations: missing"); return
    print("  📦 Loading HPD Violations...")
    con.execute("DELETE FROM hpd_violations")
    con.execute(f"""
        INSERT INTO hpd_violations
        SELECT DISTINCT ON ("violationid")
            TRY_CAST("violationid" AS INTEGER)              AS violation_id,
            CAST("bin" AS VARCHAR)                          AS bin,
            TRY_CAST("buildingid" AS INTEGER)               AS building_id,
            "boroid"                                        AS borough_id,
            "block"                                         AS block,
            "lot"                                           AS lot,
            "apartment"                                     AS apartment,
            "story"                                         AS story,
            "class"                                         AS violation_class,
            TRY_CAST("inspectiondate" AS DATE)              AS inspection_date,
            TRY_CAST("approveddate" AS DATE)                AS approved_date,
            TRY_CAST("originalcertifybydate" AS DATE)       AS original_certify_by_date,
            TRY_CAST("originalcorrectbydate" AS DATE)       AS original_correct_by_date,
            TRY_CAST("newcertifybydate" AS DATE)            AS new_certify_by_date,
            TRY_CAST("newcorrectbydate" AS DATE)            AS new_correct_by_date,
            TRY_CAST("certifieddate" AS TIMESTAMP)          AS certified_dismissed_datetime,
            "ordernumber"                                   AS order_number,
            "novid"                                         AS nov_id,
            "novdescription"                                AS nov_description,
            TRY_CAST("novissueddate" AS DATE)               AS nov_issueddate,
            "currentstatus"                                 AS current_status,
            TRY_CAST("currentstatusdate" AS DATE)           AS current_status_date
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {notnull('"bin"')}
    """)
    total   = con.execute("SELECT COUNT(*) FROM hpd_violations").fetchone()[0]
    class_c = con.execute("""
        SELECT COUNT(*) FROM hpd_violations
        WHERE violation_class='C' AND current_status!='CLOSE'
    """).fetchone()[0]
    print(f"  ✅ HPD Violations: {total:,} total, {class_c:,} open Class C")


def load_hpd_complaints(con):
    # Confirmed cols: complaint_id, building_id, borough, block, lot,
    # apartment, type, major_category, minor_category, problem_code,
    # complaint_status, complaint_status_date, status_description,
    # received_date, bin, bbl
    fp = DATA_DIR / "hpd_complaints.csv"
    if not fp.exists(): print("  ⏭️  HPD Complaints: missing"); return
    print("  📦 Loading HPD Complaints...")
    con.execute("DELETE FROM hpd_complaints")
    con.execute(f"""
        INSERT INTO hpd_complaints
        SELECT DISTINCT ON ("complaint_id")
            TRY_CAST("complaint_id" AS INTEGER)         AS complaint_id,
            CAST("bin" AS VARCHAR)                      AS bin,
            TRY_CAST("building_id" AS INTEGER)          AS building_id,
            NULL                                        AS borough_id,
            "block"                                     AS block,
            "lot"                                       AS lot,
            "apartment"                                 AS apartment,
            "complaint_status"                          AS status,
            TRY_CAST("complaint_status_date" AS DATE)   AS status_date,
            "type"                                      AS complaint_type,
            "major_category"                            AS major_category,
            "minor_category"                            AS minor_category,
            "problem_code"                              AS code,
            "status_description"                        AS problem_description,
            "status_description"                        AS status_description,
            TRY_CAST("received_date" AS DATE)           AS received_date
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {notnull('"bin"')}
    """)
    n = con.execute("SELECT COUNT(*) FROM hpd_complaints").fetchone()[0]
    print(f"  ✅ HPD Complaints: {n:,} records")


def load_fire_prevention_inspections(con):
    # Confirmed cols: acct_id, acct_num, owner_name, last_full_insp_dt,
    # last_insp_stat, prem_addr, bin, bbl, cent_latitude, cent_longitude,
    # zipcode, borough
    fp = DATA_DIR / "fire_prevention_inspections.csv"
    if not fp.exists(): print("  ⏭️  Fire Prevention: missing"); return
    print("  📦 Loading Fire Prevention Inspections...")
    con.execute("DELETE FROM fire_prevention_inspections")
    con.execute(f"""
        INSERT INTO fire_prevention_inspections
        SELECT
            ROW_NUMBER() OVER ()                            AS id,
            CAST("bin" AS VARCHAR)                          AS bin,
            "prem_addr"                                     AS address,
            "borough"                                       AS borough,
            TRY_CAST("last_full_insp_dt" AS DATE)           AS inspection_date,
            NULL                                            AS inspection_type,
            "last_insp_stat"                                AS result,
            NULL                                            AS violation_description,
            CAST("acct_num" AS VARCHAR)                     AS certificate_number,
            NULL                                            AS expiration_date,
            CASE WHEN UPPER(TRIM("last_insp_stat")) IN
                      ('PASSED','PASS','C','COMPLIANT','OK','YES')
                 THEN TRUE ELSE FALSE END                   AS is_compliant
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE {notnull('"bin"')}
    """)
    total  = con.execute("SELECT COUNT(*) FROM fire_prevention_inspections").fetchone()[0]
    failed = con.execute("SELECT COUNT(*) FROM fire_prevention_inspections WHERE NOT is_compliant").fetchone()[0]
    print(f"  ✅ Fire Prevention: {total:,} inspections, {failed:,} non-compliant")


def load_fire_incidents(con):
    # Confirmed cols: starfire_incident_id, incident_datetime,
    # incident_borough, zipcode, policeprecinct, incident_classification,
    # incident_classification_group, dispatch_response_seconds_qy,
    # incident_response_seconds_qy, incident_travel_tm_seconds_qy,
    # engines_assigned_quantity, ladders_assigned_quantity,
    # other_units_assigned_quantity
    # NOTE: no latitude/longitude in this dataset
    fp = DATA_DIR / "fire_incidents.csv"
    if not fp.exists(): print("  ⏭️  Fire Incidents: missing"); return
    print("  📦 Loading Fire Incidents...")
    con.execute("DELETE FROM fire_incidents")
    con.execute(f"""
        INSERT INTO fire_incidents
        SELECT DISTINCT ON ("starfire_incident_id")
            "starfire_incident_id"                              AS incident_id,
            TRY_CAST("incident_datetime" AS TIMESTAMP)          AS incident_datetime,
            NULL                                                AS incident_type_desc,
            "incident_borough"                                  AS incident_borough,
            "zipcode"                                           AS zipcode,
            "policeprecinct"                                    AS policeprecinct,
            "incident_classification"                           AS incident_classification,
            "incident_classification_group"                     AS incident_classification_group,
            TRY_CAST("dispatch_response_seconds_qy" AS INTEGER) AS dispatch_response_seconds,
            TRY_CAST("incident_response_seconds_qy" AS INTEGER) AS incident_response_seconds,
            TRY_CAST("incident_travel_tm_seconds_qy" AS INTEGER) AS incident_travel_seconds,
            TRY_CAST("engines_assigned_quantity" AS INTEGER)    AS engines_assigned,
            TRY_CAST("ladders_assigned_quantity" AS INTEGER)    AS ladders_assigned,
            TRY_CAST("other_units_assigned_quantity" AS INTEGER) AS other_units_assigned,
            NULL                                                AS latitude,
            NULL                                                AS longitude,
            NULL                                                AS bin
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "starfire_incident_id" IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_incidents").fetchone()[0]
    print(f"  ✅ Fire Incidents: {n:,} records")


def load_fire_company_incidents(con):
    # Confirmed cols: im_incident_key, incident_type_desc, incident_date_time,
    # arrival_date_time, last_unit_cleared_date_time, highest_level_desc,
    # total_incident_duration, action_taken1_desc, action_taken2_desc,
    # property_use_desc, street_highway, zip_code, borough_desc,
    # floor (not floor_of_fire_origin), fire_origin_below_grade_flag,
    # fire_spread_desc, detector_presence_desc, aes_presence_desc,
    # standpipe_sys_present_flag
    # NOTE: no latitude/longitude in this dataset
    fp = DATA_DIR / "fire_company_incidents.csv"
    if not fp.exists(): print("  ⏭️  Fire Company Incidents: missing"); return
    print("  📦 Loading Fire Company Incidents...")
    con.execute("DELETE FROM fire_company_incidents")
    con.execute(f"""
        INSERT INTO fire_company_incidents
        SELECT
            ROW_NUMBER() OVER ()                                AS id,
            "im_incident_key"                                   AS im_incident_key,
            "incident_type_desc"                                AS incident_type_desc,
            TRY_CAST("incident_date_time" AS TIMESTAMP)         AS incident_date_time,
            TRY_CAST("arrival_date_time" AS TIMESTAMP)          AS arrival_date_time,
            TRY_CAST("last_unit_cleared_date_time" AS TIMESTAMP) AS last_unit_cleared_date_time,
            "highest_level_desc"                                AS highest_alarm_level,
            TRY_CAST("total_incident_duration" AS INTEGER)      AS total_incident_duration,
            "action_taken1_desc"                                AS action_taken_primary,
            "action_taken2_desc"                                AS action_taken_secondary,
            "property_use_desc"                                 AS property_use_desc,
            "street_highway"                                    AS street_highway,
            "zip_code"                                          AS zip_code,
            "borough_desc"                                      AS borough_desc,
            "floor"                                             AS floor_of_fire_origin,
            CASE WHEN "fire_origin_below_grade_flag" = 'Y'
                 THEN TRUE ELSE FALSE END                       AS fire_origin_below_grade,
            "fire_spread_desc"                                  AS fire_spread_desc,
            "detector_presence_desc"                            AS detector_presence_desc,
            "aes_presence_desc"                                 AS aes_presence_desc,
            "standpipe_sys_present_flag"                        AS standpipe_system_type_desc,
            NULL                                                AS latitude,
            NULL                                                AS longitude
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
    """)
    n = con.execute("SELECT COUNT(*) FROM fire_company_incidents").fetchone()[0]
    print(f"  ✅ Fire Company Incidents: {n:,} records")


def load_ems_incidents(con):
    # Confirmed cols: cad_incident_id, incident_datetime, initial_call_type,
    # initial_severity_level_code, final_call_type, final_severity_level_code,
    # dispatch_response_seconds_qy, incident_response_seconds_qy,
    # incident_travel_tm_seconds_qy, incident_disposition_code,
    # borough, zipcode, policeprecinct, citycouncildistrict, communitydistrict
    # NOTE: no latitude/longitude in this dataset
    fp = DATA_DIR / "ems_incidents.csv"
    if not fp.exists(): print("  ⏭️  EMS Incidents: missing"); return
    print("  📦 Loading EMS Incidents...")
    con.execute("DELETE FROM ems_incidents")
    con.execute(f"""
        INSERT INTO ems_incidents
        SELECT DISTINCT ON ("cad_incident_id")
            "cad_incident_id"                                       AS cad_incident_id,
            TRY_CAST("incident_datetime" AS TIMESTAMP)              AS incident_datetime,
            "initial_call_type"                                     AS initial_call_type,
            "final_call_type"                                       AS final_call_type,
            "initial_severity_level_code"                           AS initial_severity_level,
            "final_severity_level_code"                             AS final_severity_level,
            "incident_disposition_code"                             AS incident_disposition,
            "borough"                                               AS borough,
            "zipcode"                                               AS zipcode,
            "policeprecinct"                                        AS policeprecinct,
            "citycouncildistrict"                                   AS citycouncildistrict,
            "communitydistrict"                                     AS communitydistrict,
            TRY_CAST("dispatch_response_seconds_qy" AS INTEGER)     AS dispatch_response_seconds,
            TRY_CAST("incident_response_seconds_qy" AS INTEGER)     AS incident_response_seconds,
            TRY_CAST("incident_travel_tm_seconds_qy" AS INTEGER)    AS incident_travel_seconds,
            NULL                                                    AS latitude,
            NULL                                                    AS longitude
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "cad_incident_id" IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM ems_incidents").fetchone()[0]
    print(f"  ✅ EMS Incidents: {n:,} records")


def load_311(con):
    # Confirmed cols: unique_key, created_date, closed_date, agency,
    # agency_name, complaint_type, descriptor, location_type, incident_zip,
    # incident_address, city, borough, bbl, latitude, longitude, status,
    # resolution_description
    fp = DATA_DIR / "311_requests.csv"
    if not fp.exists(): print("  ⏭️  311 Requests: missing"); return
    print("  📦 Loading 311 Complaints...")
    con.execute("DELETE FROM service_requests_311")
    con.execute(f"""
        INSERT INTO service_requests_311
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
            NULLIF(TRIM("bbl"), '')                         AS bin,
            "status"                                        AS status,
            "resolution_description"                        AS resolution_description
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "unique_key" IS NOT NULL
    """)
    n = con.execute("SELECT COUNT(*) FROM service_requests_311").fetchone()[0]
    print(f"  ✅ 311 Requests: {n:,} records")


def load_fire_hydrants(con):
    # Confirmed: this CSV is actually PARKING VIOLATIONS data (no lat/lon)
    # Columns: summons_number, plate_id, hydrant_violation, etc.
    fp = DATA_DIR / "fire_hydrants.csv"
    if not fp.exists(): print("  ⏭️  Fire Hydrants: missing"); return
    print("  ⚠️  Fire Hydrants CSV is parking violations data — no lat/lon, skipping")
    print("      To fix: re-download correct hydrant dataset (Socrata ID: 4f3u-v4tp)")


def load_hospitals(con):
    # Confirmed cols: facname, factype, boro, address, latitude, longitude, bin
    fp = DATA_DIR / "hospitals.csv"
    if not fp.exists(): print("  ⏭️  Hospitals: missing"); return
    print("  📦 Loading Hospitals...")
    con.execute("DELETE FROM hospitals")
    con.execute(f"""
        INSERT INTO hospitals
        SELECT
            "facname"                           AS facility_name,
            "factype"                           AS facility_type,
            "boro"                              AS borough,
            "address"                           AS address,
            NULL                                AS phone,
            TRY_CAST("latitude" AS DOUBLE)      AS latitude,
            TRY_CAST("longitude" AS DOUBLE)     AS longitude
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
        WHERE "factype" IN (
            'HOSPITAL', 'ACUTE CARE HOSPITAL', 'OFF-CAMPUS EMERGENCY DEPARTMENT'
        )
        AND TRY_CAST("latitude" AS DOUBLE) IS NOT NULL
        AND TRY_CAST("latitude" AS DOUBLE) != 0.0
        AND TRY_CAST("longitude" AS DOUBLE) IS NOT NULL
        AND TRY_CAST("longitude" AS DOUBLE) != 0.0
    """)
    n = con.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    print(f"  ✅ Hospitals: {n:,} facilities")


def load_elevators(con):
    # Confirmed cols: device_id, device_type, device_status, elevator_type,
    # physical_address, travel_from_floor, travel_to_floor,
    # elevator_capacity_lbs, elevator_speed_fpm
    # NOTE: no BIN column in this dataset
    fp = DATA_DIR / "elevators.csv"
    if not fp.exists(): print("  ⏭️  Elevators: missing"); return
    print("  📦 Loading Elevators...")
    con.execute("DELETE FROM elevators")
    con.execute(f"""
        INSERT INTO elevators
        SELECT
            ROW_NUMBER() OVER ()                        AS id,
            NULL                                        AS bin,
            CAST("device_id" AS VARCHAR)                AS device_number,
            "device_type"                               AS device_type,
            "travel_from_floor"                         AS floor_from,
            "travel_to_floor"                           AS floor_to,
            CAST("elevator_speed_fpm" AS VARCHAR)       AS speed,
            CAST("elevator_capacity_lbs" AS VARCHAR)    AS capacity,
            NULL                                        AS approval_date,
            COALESCE("device_status", 'UNKNOWN')        AS status
        FROM read_csv_auto('{p(fp)}', header=true, ignore_errors=true, all_varchar=true)
    """)
    total = con.execute("SELECT COUNT(*) FROM elevators").fetchone()[0]
    oos   = con.execute("SELECT COUNT(*) FROM elevators WHERE status != 'ACTIVE'").fetchone()[0]
    print(f"  ✅ Elevators: {total:,} devices, {oos:,} not active")


def load_all(con):
    print("\n" + "="*60 + "\n📦  LOADING ALL DATASETS INTO DUCKDB\n" + "="*60)
    init_db(con)
    clear_all(con)
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
    print("\n" + "="*60 + "\n📊  DATABASE SUMMARY\n" + "="*60)
    for (t,) in con.execute("SHOW TABLES").fetchall():
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<35} {n:>12,} rows")
        except Exception:
            print(f"  {t:<35}        (empty)")
    mb = os.path.getsize(DB_PATH) / 1024 / 1024
    print(f"\n  💾 Database size: {mb:.1f} MB")


# ─────────────────────────────────────────────
# Owner Portfolio
# ─────────────────────────────────────────────

def compute_owner_portfolio(con):
    print("\n" + "="*60 + "\n🏢  OWNER PORTFOLIO\n" + "="*60)
    con.execute("DELETE FROM owner_portfolio")
    con.execute("""
        INSERT INTO owner_portfolio
        SELECT
            b.owner_name,
            COUNT(DISTINCT b.bin)                               AS total_buildings,
            COUNT(DISTINCT dv.id) FILTER (WHERE dv.is_active)   AS total_open_violations,
            COUNT(DISTINCT hv.violation_id)
                FILTER (WHERE hv.violation_class='C' AND hv.current_status!='CLOSE')
                                                                AS total_class_c_violations,
            COALESCE(SUM(ecb.balance_due), 0)                   AS total_ecb_balance_due,
            COALESCE(COUNT(DISTINCT dv.id) FILTER (WHERE dv.is_active)
                / NULLIF(COUNT(DISTINCT b.bin),0), 0)           AS avg_violations_per_building,
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
    print(f"  ✅ {total:,} owners, {neglect:,} high-neglect")


# ─────────────────────────────────────────────
# Risk Scores
# ─────────────────────────────────────────────

def compute_risk_scores(con):
    print("\n" + "="*60 + "\n🧮  BUILDING RISK SCORES\n" + "="*60)
    con.execute("DELETE FROM building_risk_scores")
    con.execute("INSERT INTO building_risk_scores (bin, overall_risk_score) SELECT bin, 0.0 FROM buildings")

    steps = [
        ("DOB violations", """
            UPDATE building_risk_scores SET active_dob_violations = sub.cnt
            FROM (SELECT bin, COUNT(*) cnt FROM dob_violations WHERE is_active GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("ECB violations", """
            UPDATE building_risk_scores SET active_ecb_violations = sub.cnt
            FROM (SELECT bin, COUNT(*) cnt FROM dob_ecb_violations WHERE is_active GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("HPD Class C", """
            UPDATE building_risk_scores SET active_hpd_class_c = sub.cnt
            FROM (SELECT bin, COUNT(*) cnt FROM hpd_violations
                  WHERE violation_class='C' AND current_status!='CLOSE' GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("HPD Class B", """
            UPDATE building_risk_scores SET active_hpd_class_b = sub.cnt
            FROM (SELECT bin, COUNT(*) cnt FROM hpd_violations
                  WHERE violation_class='B' AND current_status!='CLOSE' GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("311 velocity 30d", """
            UPDATE building_risk_scores SET complaint_velocity_30d = sub.cnt
            FROM (
                SELECT b.bin, COUNT(*) cnt FROM buildings b
                JOIN service_requests_311 s ON LOWER(b.address)=LOWER(s.incident_address)
                WHERE s.created_date >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("311 velocity 90d", """
            UPDATE building_risk_scores SET complaint_velocity_90d = sub.cnt
            FROM (
                SELECT b.bin, COUNT(*) cnt FROM buildings b
                JOIN service_requests_311 s ON LOWER(b.address)=LOWER(s.incident_address)
                WHERE s.created_date >= CURRENT_DATE - INTERVAL '90 days'
                GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("Fire prevention", """
            UPDATE building_risk_scores SET last_fdny_inspection_pass = sub.p
            FROM (SELECT bin, BOOL_OR(is_compliant) p
                  FROM fire_prevention_inspections GROUP BY bin) sub
            WHERE building_risk_scores.bin = sub.bin
        """),
        ("Nearest hospital", """
            UPDATE building_risk_scores SET nearest_hospital=sub.name, nearest_hospital_mi=sub.d
            FROM (
                SELECT b.bin,
                    FIRST(h.facility_name ORDER BY
                        SQRT(POW((b.latitude-h.latitude)*69.0,2)+POW((b.longitude-h.longitude)*54.6,2))
                    ) AS name,
                    MIN(SQRT(POW((b.latitude-h.latitude)*69.0,2)+POW((b.longitude-h.longitude)*54.6,2))) AS d
                FROM buildings b, hospitals h
                WHERE b.latitude IS NOT NULL AND b.longitude IS NOT NULL
                GROUP BY b.bin
            ) sub WHERE building_risk_scores.bin = sub.bin
        """),
        ("Composite score", """
            UPDATE building_risk_scores SET overall_risk_score =
                COALESCE(active_hpd_class_c,    0)*6.0 +
                COALESCE(active_ecb_violations, 0)*3.0 +
                COALESCE(active_dob_violations, 0)*2.5 +
                COALESCE(active_hpd_class_b,    0)*2.0 +
                COALESCE(complaint_velocity_30d,0)*2.0 +
                COALESCE(complaint_velocity_90d,0)*1.0 +
                CASE WHEN last_fdny_inspection_pass=FALSE THEN 15.0 ELSE 0.0 END
        """),
    ]
    for label, sql in steps:
        print(f"  → {label}...")
        try:
            con.execute(sql)
        except Exception as e:
            print(f"    ⚠️  skipped: {e}")

    con.execute("""
        UPDATE buildings SET risk_score = brs.overall_risk_score
        FROM building_risk_scores brs WHERE buildings.bin = brs.bin
    """)
    scored    = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score>0").fetchone()[0]
    high_risk = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score>30").fetchone()[0]
    critical  = con.execute("SELECT COUNT(*) FROM building_risk_scores WHERE overall_risk_score>60").fetchone()[0]
    print(f"  ✅ {scored:,} scored | 🟡 {high_risk:,} high | 🔴 {critical:,} critical")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="FirstResponder Copilot — Ingestion Pipeline")
    parser.add_argument("--download",  action="store_true")
    parser.add_argument("--load",      action="store_true")
    parser.add_argument("--risk",      action="store_true")
    parser.add_argument("--portfolio", action="store_true")
    parser.add_argument("--all",       action="store_true")
    args = parser.parse_args()
    if not any(vars(args).values()):
        parser.print_help(); sys.exit(1)

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

    print(f"\n🎉  Done in {datetime.now()-start}. DB: {DB_PATH}")


if __name__ == "__main__":
    main()