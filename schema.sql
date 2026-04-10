-- FirstResponder Copilot - Database Schema
-- All tables indexed by BIN (Building Identification Number) for instant address lookups
-- Uses DuckDB for fast analytical queries on the NVIDIA GB10

-----------------------------------------------
-- CORE: Building Profile
-----------------------------------------------
CREATE TABLE IF NOT EXISTS buildings (
    bin VARCHAR PRIMARY KEY,
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
    violation_category VARCHAR,
    description TEXT,
    disposition_date DATE,
    disposition_comments TEXT,
    issue_date DATE,
    severity VARCHAR,                 -- CRITICAL / HIGH / MEDIUM / LOW
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_dob_viol_bin ON dob_violations(bin);
CREATE INDEX IF NOT EXISTS idx_dob_viol_date ON dob_violations(issue_date);
CREATE INDEX IF NOT EXISTS idx_dob_viol_active ON dob_violations(is_active);

-----------------------------------------------
-- DOB Safety Violations
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
-- DOB ECB Violations (Environmental Control Board)
-- Penalty amounts and infraction codes — key neglect signal
-----------------------------------------------
CREATE TABLE IF NOT EXISTS dob_ecb_violations (
    ecb_violation_number VARCHAR PRIMARY KEY,
    bin VARCHAR,
    block VARCHAR,
    lot VARCHAR,
    violation_type VARCHAR,
    violation_description TEXT,
    infraction_code VARCHAR,
    section_law_description TEXT,
    penalty_imposed DOUBLE,           -- Financial penalty — high amounts = serious neglect
    amount_paid DOUBLE,
    balance_due DOUBLE,               -- Unpaid balance = owner non-compliance signal
    hearing_date DATE,
    hearing_status VARCHAR,
    served_date DATE,
    issue_date DATE,
    severity VARCHAR,                 -- CRITICAL / HIGH / MEDIUM / LOW
    is_active BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_ecb_bin ON dob_ecb_violations(bin);
CREATE INDEX IF NOT EXISTS idx_ecb_active ON dob_ecb_violations(is_active);
CREATE INDEX IF NOT EXISTS idx_ecb_balance ON dob_ecb_violations(balance_due);

-----------------------------------------------
-- Bureau of Fire Prevention Inspections
-- Sprinkler, standpipe, suppression system status
-- CRITICAL for first responders — know before entry
-----------------------------------------------
CREATE TABLE IF NOT EXISTS fire_prevention_inspections (
    id INTEGER PRIMARY KEY,
    bin VARCHAR,
    address VARCHAR,
    borough VARCHAR,
    inspection_date DATE,
    inspection_type VARCHAR,          -- SPRINKLER / STANDPIPE / SUPPRESSION / GENERAL
    result VARCHAR,                   -- PASSED / FAILED / PARTIAL
    violation_description TEXT,
    certificate_number VARCHAR,
    expiration_date DATE,
    is_compliant BOOLEAN,             -- Quick flag for brief generation
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_fire_prev_bin ON fire_prevention_inspections(bin);
CREATE INDEX IF NOT EXISTS idx_fire_prev_compliant ON fire_prevention_inspections(is_compliant);
CREATE INDEX IF NOT EXISTS idx_fire_prev_date ON fire_prevention_inspections(inspection_date);

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
    complaint_type VARCHAR,
    major_category VARCHAR,           -- PLUMBING, ELECTRIC, GAS, HEAT/HOT WATER
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
-- 311 Service Requests (safety-relevant only)
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
    bin VARCHAR,
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
-- Fire Company Incidents (detailed tactical data)
-- Floor of fire origin, suppression systems, spread
-- Most operationally valuable for brief generation
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
    floor_of_fire_origin VARCHAR,     -- Which floor — critical tactical info
    fire_origin_below_grade BOOLEAN,
    fire_spread_desc VARCHAR,         -- How fire spread — indicates building integrity
    detector_presence_desc VARCHAR,   -- Were detectors present/functional?
    aes_presence_desc VARCHAR,        -- Automatic Extinguishing System present?
    standpipe_system_type_desc VARCHAR,
    latitude DOUBLE,
    longitude DOUBLE
);

CREATE INDEX IF NOT EXISTS idx_fire_co_date ON fire_company_incidents(incident_date_time);
CREATE INDEX IF NOT EXISTS idx_fire_co_geo ON fire_company_incidents(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_fire_co_key ON fire_company_incidents(im_incident_key);

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
CREATE INDEX IF NOT EXISTS idx_ems_type ON ems_incidents(final_call_type);

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
-- Hospitals & Trauma Centers
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
    status VARCHAR,                   -- ACTIVE / INACTIVE / OUT OF SERVICE
    FOREIGN KEY (bin) REFERENCES buildings(bin)
);

CREATE INDEX IF NOT EXISTS idx_elevators_bin ON elevators(bin);
CREATE INDEX IF NOT EXISTS idx_elevators_status ON elevators(status);

-----------------------------------------------
-- Building Owner Portfolio (computed)
-- Enables landlord neglect pattern detection
-----------------------------------------------
CREATE TABLE IF NOT EXISTS owner_portfolio (
    owner_name VARCHAR,
    total_buildings INTEGER,
    total_open_violations INTEGER,
    total_class_c_violations INTEGER,
    total_ecb_balance_due DOUBLE,     -- Total unpaid ECB fines — strong neglect signal
    avg_violations_per_building DOUBLE,
    bins TEXT                         -- Comma-separated BINs
);

CREATE INDEX IF NOT EXISTS idx_owner_name ON owner_portfolio(owner_name);

-----------------------------------------------
-- Precomputed Risk Scores (per building)
-- Used by query_engine.py for instant brief generation
-----------------------------------------------
CREATE TABLE IF NOT EXISTS building_risk_scores (
    bin VARCHAR PRIMARY KEY,
    overall_risk_score DOUBLE,
    structural_risk DOUBLE,
    fire_risk DOUBLE,
    hazmat_risk DOUBLE,
    suppression_system_compliant BOOLEAN,  -- From fire prevention inspections
    complaint_velocity_30d INTEGER,
    complaint_velocity_90d INTEGER,
    complaint_velocity_365d INTEGER,
    active_dob_violations INTEGER,
    active_ecb_violations INTEGER,         -- ECB unpaid balance count
    active_hpd_class_c INTEGER,
    active_hpd_class_b INTEGER,
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
