-- FirstResponder Copilot - Database Schema
-- All tables indexed by BIN (Building Identification Number) for instant address lookups
-- Uses DuckDB for fast analytical queries on the GB10

-----------------------------------------------
-- CORE: Building Profile
-----------------------------------------------
CREATE TABLE IF NOT EXISTS buildings (
    bin VARCHAR PRIMARY KEY,          -- Building Identification Number
    borough VARCHAR,
    block VARCHAR,
    lot VARCHAR,
    address VARCHAR,
    zipcode VARCHAR,
    borough_code INTEGER,
    num_floors INTEGER,
    year_built INTEGER,
    building_class VARCHAR,
    land_use VARCHAR,
    residential_units INTEGER,
    total_units INTEGER,
    lot_area DOUBLE,
    building_area DOUBLE,
    construction_type VARCHAR,        -- Fireproof, non-fireproof, wood frame
    owner_name VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    -- Enriched fields (computed during ingestion)
    risk_score DOUBLE DEFAULT 0.0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_buildings_address ON buildings(address);
CREATE INDEX IF NOT EXISTS idx_buildings_bbl ON buildings(borough_code, block, lot);
CREATE INDEX IF NOT EXISTS idx_buildings_geo ON buildings(latitude, longitude);

-----------------------------------------------
-- DOB Violations
-----------------------------------------------
CREATE TABLE IF NOT EXISTS dob_violations (
    id INTEGER PRIMARY KEY,
    bin VARCHAR,
    block VARCHAR,
    lot VARCHAR,
    violation_type VARCHAR,
    violation_number VARCHAR,
    violation_category VARCHAR,       -- e.g., "GENERAL", "ELEVATOR", "CONSTRUCTION"
    description TEXT,
    disposition_date DATE,
    disposition_comments TEXT,
    issue_date DATE,
    severity VARCHAR,                 -- Computed: CRITICAL / HIGH / MEDIUM / LOW
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_dob_viol_bin ON dob_violations(bin);
CREATE INDEX IF NOT EXISTS idx_dob_viol_date ON dob_violations(issue_date);
CREATE INDEX IF NOT EXISTS idx_dob_viol_active ON dob_violations(is_active);

-----------------------------------------------
-- DOB Safety Violations (subset but critical)
-----------------------------------------------
CREATE TABLE IF NOT EXISTS dob_safety_violations (
    id INTEGER PRIMARY KEY,
    bin VARCHAR,
    violation_number VARCHAR,
    violation_type VARCHAR,
    violation_description TEXT,
    issue_date DATE,
    status VARCHAR,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_dob_safety_bin ON dob_safety_violations(bin);

-----------------------------------------------
-- HPD Housing Violations
-----------------------------------------------
CREATE TABLE IF NOT EXISTS hpd_violations (
    violation_id INTEGER PRIMARY KEY,
    bin VARCHAR,
    building_id INTEGER,
    borough_id VARCHAR,
    block VARCHAR,
    lot VARCHAR,
    apartment VARCHAR,
    story VARCHAR,
    violation_class VARCHAR,          -- A (non-hazardous), B (hazardous), C (immediately hazardous)
    inspection_date DATE,
    approved_date DATE,
    original_certify_by_date DATE,
    original_correct_by_date DATE,
    new_certify_by_date DATE,
    new_correct_by_date DATE,
    certified_dismissed_datetime TIMESTAMP,
    order_number VARCHAR,
    nov_id VARCHAR,
    nov_description TEXT,
    nov_issueddate DATE,
    current_status VARCHAR,           -- OPEN / CLOSE
    current_status_date DATE,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_hpd_viol_bin ON hpd_violations(bin);
CREATE INDEX IF NOT EXISTS idx_hpd_viol_class ON hpd_violations(violation_class);
CREATE INDEX IF NOT EXISTS idx_hpd_viol_status ON hpd_violations(current_status);

-----------------------------------------------
-- HPD Complaints & Problems
-----------------------------------------------
CREATE TABLE IF NOT EXISTS hpd_complaints (
    complaint_id INTEGER PRIMARY KEY,
    bin VARCHAR,
    building_id INTEGER,
    borough_id VARCHAR,
    block VARCHAR,
    lot VARCHAR,
    apartment VARCHAR,
    status VARCHAR,
    status_date DATE,
    complaint_type VARCHAR,           -- e.g., EMERGENCY, NON EMERGENCY
    major_category VARCHAR,           -- e.g., PLUMBING, ELECTRIC, GAS, HEAT/HOT WATER
    minor_category VARCHAR,
    code VARCHAR,
    problem_description TEXT,
    status_description TEXT,
    received_date DATE,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_hpd_comp_bin ON hpd_complaints(bin);
CREATE INDEX IF NOT EXISTS idx_hpd_comp_category ON hpd_complaints(major_category);
CREATE INDEX IF NOT EXISTS idx_hpd_comp_date ON hpd_complaints(received_date);

-----------------------------------------------
-- 311 Service Requests (filtered for safety-relevant)
-----------------------------------------------
CREATE TABLE IF NOT EXISTS service_requests_311 (
    unique_key VARCHAR PRIMARY KEY,
    created_date TIMESTAMP,
    closed_date TIMESTAMP,
    agency VARCHAR,
    agency_name VARCHAR,
    complaint_type VARCHAR,
    descriptor VARCHAR,
    location_type VARCHAR,
    incident_zip VARCHAR,
    incident_address VARCHAR,
    city VARCHAR,
    borough VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE,
    bin VARCHAR,                       -- May need to be geocoded/matched
    status VARCHAR,
    resolution_description TEXT,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_311_bin ON service_requests_311(bin);
CREATE INDEX IF NOT EXISTS idx_311_type ON service_requests_311(complaint_type);
CREATE INDEX IF NOT EXISTS idx_311_date ON service_requests_311(created_date);
CREATE INDEX IF NOT EXISTS idx_311_address ON service_requests_311(incident_address);

-----------------------------------------------
-- Fire Incident Dispatch Data
-----------------------------------------------
CREATE TABLE IF NOT EXISTS fire_incidents (
    incident_id VARCHAR PRIMARY KEY,
    incident_datetime TIMESTAMP,
    incident_type_desc VARCHAR,
    incident_borough VARCHAR,
    zipcode VARCHAR,
    policeprecinct VARCHAR,
    incident_classification VARCHAR,
    incident_classification_group VARCHAR,
    dispatch_response_seconds INTEGER,
    incident_response_seconds INTEGER,
    incident_travel_seconds INTEGER,
    engines_assigned INTEGER,
    ladders_assigned INTEGER,
    other_units_assigned INTEGER,
    latitude DOUBLE,
    longitude DOUBLE,
    bin VARCHAR,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_fire_inc_bin ON fire_incidents(bin);
CREATE INDEX IF NOT EXISTS idx_fire_inc_date ON fire_incidents(incident_datetime);
CREATE INDEX IF NOT EXISTS idx_fire_inc_type ON fire_incidents(incident_classification);

-----------------------------------------------
-- Fire Company Incidents (detailed)
-----------------------------------------------
CREATE TABLE IF NOT EXISTS fire_company_incidents (
    id INTEGER PRIMARY KEY,
    im_incident_key VARCHAR,
    incident_type_desc VARCHAR,
    incident_date_time TIMESTAMP,
    arrival_date_time TIMESTAMP,
    last_unit_cleared_date_time TIMESTAMP,
    highest_alarm_level VARCHAR,
    total_incident_duration INTEGER,
    action_taken_primary VARCHAR,
    action_taken_secondary VARCHAR,
    property_use_desc VARCHAR,
    street_highway VARCHAR,
    zip_code VARCHAR,
    borough_desc VARCHAR,
    floor_of_fire_origin VARCHAR,
    fire_origin_below_grade BOOLEAN,
    fire_spread_desc VARCHAR,
    detector_presence_desc VARCHAR,
    aes_presence_desc VARCHAR,        -- Automatic Extinguishing System
    standpipe_system_type_desc VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_fire_co_date ON fire_company_incidents(incident_date_time);
CREATE INDEX IF NOT EXISTS idx_fire_co_geo ON fire_company_incidents(latitude, longitude);

-----------------------------------------------
-- EMS Incident Dispatch
-----------------------------------------------
CREATE TABLE IF NOT EXISTS ems_incidents (
    cad_incident_id VARCHAR PRIMARY KEY,
    incident_datetime TIMESTAMP,
    initial_call_type VARCHAR,
    final_call_type VARCHAR,
    initial_severity_level VARCHAR,
    final_severity_level VARCHAR,
    incident_disposition VARCHAR,
    borough VARCHAR,
    zipcode VARCHAR,
    policeprecinct VARCHAR,
    citycouncildistrict VARCHAR,
    communitydistrict VARCHAR,
    dispatch_response_seconds INTEGER,
    incident_response_seconds INTEGER,
    incident_travel_seconds INTEGER,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_ems_date ON ems_incidents(incident_datetime);
CREATE INDEX IF NOT EXISTS idx_ems_geo ON ems_incidents(latitude, longitude);

-----------------------------------------------
-- NYPD Complaint Data
-----------------------------------------------
CREATE TABLE IF NOT EXISTS nypd_complaints (
    cmplnt_num VARCHAR PRIMARY KEY,
    cmplnt_fr_dt DATE,
    cmplnt_fr_tm TIME,
    ofns_desc VARCHAR,
    law_cat_cd VARCHAR,               -- FELONY / MISDEMEANOR / VIOLATION
    boro_nm VARCHAR,
    prem_typ_desc VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_nypd_date ON nypd_complaints(cmplnt_fr_dt);
CREATE INDEX IF NOT EXISTS idx_nypd_geo ON nypd_complaints(latitude, longitude);

-----------------------------------------------
-- Fire Hydrants
-----------------------------------------------
CREATE TABLE IF NOT EXISTS fire_hydrants (
    id INTEGER PRIMARY KEY,
    latitude DOUBLE,
    longitude DOUBLE,
    unitid VARCHAR,
    borough VARCHAR
);

CREATE INDEX IF NOT EXISTS idx_hydrants_geo ON fire_hydrants(latitude, longitude);

-----------------------------------------------
-- Hospitals & Facilities
-----------------------------------------------
CREATE TABLE IF NOT EXISTS hospitals (
    facility_name VARCHAR,
    facility_type VARCHAR,
    borough VARCHAR,
    address VARCHAR,
    phone VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_hospitals_geo ON hospitals(latitude, longitude);

-----------------------------------------------
-- Elevators
-----------------------------------------------
CREATE TABLE IF NOT EXISTS elevators (
    id INTEGER PRIMARY KEY,
    bin VARCHAR,
    device_number VARCHAR,
    device_type VARCHAR,
    floor_from VARCHAR,
    floor_to VARCHAR,
    speed VARCHAR,
    capacity VARCHAR,
    approval_date DATE,
    status VARCHAR,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_elevators_bin ON elevators(bin);

-----------------------------------------------
-- Building Owner Portfolio (computed table)
-----------------------------------------------
CREATE TABLE IF NOT EXISTS owner_portfolio (
    owner_name VARCHAR,
    total_buildings INTEGER,
    total_open_violations INTEGER,
    total_class_c_violations INTEGER,
    avg_violations_per_building DOUBLE,
    bins TEXT                          -- Comma-separated list of BINs
);

CREATE INDEX IF NOT EXISTS idx_owner_name ON owner_portfolio(owner_name);

-----------------------------------------------
-- Precomputed Risk Scores
-----------------------------------------------
CREATE TABLE IF NOT EXISTS building_risk_scores (
    bin VARCHAR PRIMARY KEY,
    overall_risk_score DOUBLE,
    structural_risk DOUBLE,
    fire_risk DOUBLE,
    hazmat_risk DOUBLE,
    crime_risk DOUBLE,
    complaint_velocity_30d INTEGER,    -- 311 + HPD complaints last 30 days
    complaint_velocity_90d INTEGER,
    complaint_velocity_365d INTEGER,
    active_dob_violations INTEGER,
    active_hpd_class_c INTEGER,       -- Immediately hazardous
    active_hpd_class_b INTEGER,       -- Hazardous
    prior_fire_incidents INTEGER,
    prior_ems_incidents INTEGER,
    last_fdny_inspection_pass BOOLEAN,
    elevator_count INTEGER,
    elevator_out_of_service INTEGER,
    nearest_hydrant_ft DOUBLE,
    nearest_hospital VARCHAR,
    nearest_hospital_mi DOUBLE,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);
