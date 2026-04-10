"""
FirstResponder Copilot — Building Intelligence Query Engine
Takes an address or BIN and returns a comprehensive building profile
for tactical brief generation.

Usage:
    from query_engine import BuildingIntelligence
    engine = BuildingIntelligence("data/responder.duckdb")
    profile = engine.get_profile("450 W 33RD ST")
    print(profile)
"""

import duckdb
import json
import math
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta


@dataclass
class NearbyResource:
    name: str
    distance_ft: float
    latitude: float
    longitude: float
    type: str  # "hydrant", "hospital", "fire_station"


@dataclass
class ViolationSummary:
    source: str  # "DOB", "HPD", "FDNY"
    violation_type: str
    description: str
    date: str
    severity: str
    is_active: bool


@dataclass 
class IncidentHistory:
    incident_type: str
    date: str
    description: str
    response_seconds: Optional[int] = None
    details: Optional[str] = None


@dataclass
class BuildingProfile:
    """Complete building intelligence profile for tactical brief generation."""
    # Identity
    bin: str
    address: str
    borough: str
    zipcode: str
    
    # Structure
    num_floors: int
    year_built: int
    building_class: str
    land_use: str
    construction_type: str
    residential_units: int
    total_units: int
    lot_area: float
    building_area: float
    owner_name: str
    
    # Location
    latitude: float
    longitude: float
    
    # Risk Assessment
    overall_risk_score: float
    risk_level: str  # "CRITICAL", "HIGH", "MEDIUM", "LOW"
    
    # Violations & Hazards
    active_dob_violations: int
    active_hpd_class_c: int  # Immediately hazardous
    active_hpd_class_b: int  # Hazardous
    critical_violations: List[ViolationSummary] = field(default_factory=list)
    recent_complaints: List[ViolationSummary] = field(default_factory=list)
    
    # Incident History
    prior_fire_incidents: int = 0
    prior_ems_incidents: int = 0
    incident_history: List[IncidentHistory] = field(default_factory=list)
    
    # 311 Complaint Velocity
    complaints_30d: int = 0
    complaints_90d: int = 0
    complaints_365d: int = 0
    
    # Infrastructure
    elevator_count: int = 0
    elevator_out_of_service: int = 0
    
    # Nearby Resources
    nearest_hydrants: List[NearbyResource] = field(default_factory=list)
    nearest_hospital: Optional[NearbyResource] = None
    
    # Owner Portfolio (red flags)
    owner_total_buildings: int = 0
    owner_total_violations: int = 0
    
    # Raw data for LLM context
    raw_311_complaints: List[Dict] = field(default_factory=list)
    raw_hpd_complaints: List[Dict] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, default=str)


def haversine_ft(lat1, lon1, lat2, lon2):
    """Calculate distance in feet between two lat/lon points."""
    R = 20902231  # Earth radius in feet
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    
    a = math.sin(dphi/2)**2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


class BuildingIntelligence:
    """Query engine for building intelligence profiles."""
    
    def __init__(self, db_path: str = "data/responder.duckdb"):
        self.con = duckdb.connect(db_path, read_only=True)
    
    def close(self):
        self.con.close()
    
    def search_address(self, query: str) -> List[Dict]:
        """Search for buildings matching an address query."""
        query_upper = query.upper().strip()
        
        results = self.con.execute("""
            SELECT bin, address, borough, zipcode, num_floors, year_built
            FROM buildings 
            WHERE UPPER(address) LIKE ?
            LIMIT 10
        """, [f"%{query_upper}%"]).fetchall()
        
        return [
            {
                "bin": r[0], "address": r[1], "borough": r[2],
                "zipcode": r[3], "floors": r[4], "year_built": r[5]
            }
            for r in results
        ]
    
    def get_profile(self, address_or_bin: str) -> Optional[BuildingProfile]:
        """
        Get complete building intelligence profile.
        Accepts either an address string or a BIN number.
        """
        # Determine if input is BIN or address
        if address_or_bin.isdigit() and len(address_or_bin) == 7:
            building = self._get_building_by_bin(address_or_bin)
        else:
            building = self._get_building_by_address(address_or_bin)
        
        if not building:
            return None
        
        bin_id = building["bin"]
        lat = building.get("latitude", 0)
        lon = building.get("longitude", 0)
        
        # Parallel data retrieval (all indexed by BIN)
        dob_violations = self._get_dob_violations(bin_id)
        hpd_violations = self._get_hpd_violations(bin_id)
        hpd_complaints = self._get_hpd_complaints(bin_id)
        complaints_311 = self._get_311_complaints(bin_id, building.get("address", ""))
        fire_history = self._get_fire_history(lat, lon)
        ems_history = self._get_ems_history(lat, lon)
        hydrants = self._get_nearest_hydrants(lat, lon) if lat and lon else []
        hospital = self._get_nearest_hospital(lat, lon) if lat and lon else None
        
        # Compute risk
        active_dob = len([v for v in dob_violations if v.is_active])
        active_c = len([v for v in hpd_violations if v.severity == "CRITICAL" and v.is_active])
        active_b = len([v for v in hpd_violations if v.severity == "HIGH" and v.is_active])
        
        risk_score = (
            active_dob * 2.0 +
            active_c * 5.0 +
            active_b * 2.0 +
            len(fire_history) * 3.0 +
            len(complaints_311) * 0.5
        )
        
        risk_level = (
            "CRITICAL" if risk_score > 30 else
            "HIGH" if risk_score > 15 else
            "MEDIUM" if risk_score > 5 else
            "LOW"
        )
        
        # Complaint velocity
        now = datetime.now()
        c30 = len([c for c in complaints_311 if c.get("date") and _parse_date(c["date"]) > now - timedelta(days=30)])
        c90 = len([c for c in complaints_311 if c.get("date") and _parse_date(c["date"]) > now - timedelta(days=90)])
        c365 = len([c for c in complaints_311 if c.get("date") and _parse_date(c["date"]) > now - timedelta(days=365)])
        
        # Build profile
        profile = BuildingProfile(
            bin=bin_id,
            address=building.get("address", ""),
            borough=building.get("borough", ""),
            zipcode=building.get("zipcode", ""),
            num_floors=building.get("num_floors", 0) or 0,
            year_built=building.get("year_built", 0) or 0,
            building_class=building.get("building_class", ""),
            land_use=building.get("land_use", ""),
            construction_type=building.get("construction_type", "Unknown"),
            residential_units=building.get("residential_units", 0) or 0,
            total_units=building.get("total_units", 0) or 0,
            lot_area=building.get("lot_area", 0) or 0,
            building_area=building.get("building_area", 0) or 0,
            owner_name=building.get("owner_name", "Unknown"),
            latitude=lat or 0,
            longitude=lon or 0,
            overall_risk_score=risk_score,
            risk_level=risk_level,
            active_dob_violations=active_dob,
            active_hpd_class_c=active_c,
            active_hpd_class_b=active_b,
            critical_violations=dob_violations[:20],  # Top 20 most relevant
            recent_complaints=[
                ViolationSummary(
                    source="HPD",
                    violation_type=c.get("major_category", ""),
                    description=c.get("problem_description", ""),
                    date=str(c.get("received_date", "")),
                    severity="HIGH" if c.get("complaint_type") == "EMERGENCY" else "MEDIUM",
                    is_active=c.get("status") != "CLOSE"
                )
                for c in hpd_complaints[:20]
            ],
            prior_fire_incidents=len(fire_history),
            prior_ems_incidents=len(ems_history),
            incident_history=fire_history[:10],
            complaints_30d=c30,
            complaints_90d=c90,
            complaints_365d=c365,
            nearest_hydrants=hydrants[:3],
            nearest_hospital=hospital,
            raw_311_complaints=complaints_311[:15],
            raw_hpd_complaints=hpd_complaints[:15],
        )
        
        return profile
    
    # ─────────────────────────────────────────
    # Private query methods
    # ─────────────────────────────────────────
    
    def _get_building_by_bin(self, bin_id: str) -> Optional[Dict]:
        result = self.con.execute("""
            SELECT bin, address, borough, zipcode, borough_code,
                   num_floors, year_built, building_class, land_use,
                   residential_units, total_units, lot_area, building_area,
                   construction_type, owner_name, latitude, longitude
            FROM buildings WHERE bin = ?
        """, [bin_id]).fetchone()
        
        if not result:
            return None
        
        cols = ["bin", "address", "borough", "zipcode", "borough_code",
                "num_floors", "year_built", "building_class", "land_use",
                "residential_units", "total_units", "lot_area", "building_area",
                "construction_type", "owner_name", "latitude", "longitude"]
        return dict(zip(cols, result))
    
    def _get_building_by_address(self, address: str) -> Optional[Dict]:
        addr_upper = address.upper().strip()
        
        # Try exact match first
        result = self.con.execute("""
            SELECT bin, address, borough, zipcode, borough_code,
                   num_floors, year_built, building_class, land_use,
                   residential_units, total_units, lot_area, building_area,
                   construction_type, owner_name, latitude, longitude
            FROM buildings 
            WHERE UPPER(address) = ?
            LIMIT 1
        """, [addr_upper]).fetchone()
        
        # Fuzzy match if exact fails
        if not result:
            result = self.con.execute("""
                SELECT bin, address, borough, zipcode, borough_code,
                       num_floors, year_built, building_class, land_use,
                       residential_units, total_units, lot_area, building_area,
                       construction_type, owner_name, latitude, longitude
                FROM buildings 
                WHERE UPPER(address) LIKE ?
                LIMIT 1
            """, [f"%{addr_upper}%"]).fetchone()
        
        if not result:
            return None
        
        cols = ["bin", "address", "borough", "zipcode", "borough_code",
                "num_floors", "year_built", "building_class", "land_use",
                "residential_units", "total_units", "lot_area", "building_area",
                "construction_type", "owner_name", "latitude", "longitude"]
        return dict(zip(cols, result))
    
    def _get_dob_violations(self, bin_id: str) -> List[ViolationSummary]:
        results = self.con.execute("""
            SELECT violation_type, violation_category, description, 
                   issue_date, severity, is_active
            FROM dob_violations 
            WHERE bin = ?
            ORDER BY is_active DESC, issue_date DESC
            LIMIT 30
        """, [bin_id]).fetchall()
        
        return [
            ViolationSummary(
                source="DOB",
                violation_type=r[0] or "",
                description=r[2] or "",
                date=str(r[3]) if r[3] else "",
                severity=r[4] or "LOW",
                is_active=bool(r[5])
            )
            for r in results
        ]
    
    def _get_hpd_violations(self, bin_id: str) -> List[ViolationSummary]:
        results = self.con.execute("""
            SELECT violation_class, nov_description, inspection_date,
                   current_status, order_number
            FROM hpd_violations 
            WHERE bin = ?
            ORDER BY 
                CASE violation_class WHEN 'C' THEN 1 WHEN 'B' THEN 2 ELSE 3 END,
                inspection_date DESC
            LIMIT 30
        """, [bin_id]).fetchall()
        
        return [
            ViolationSummary(
                source="HPD",
                violation_type=f"Class {r[0]}" if r[0] else "",
                description=r[1] or "",
                date=str(r[2]) if r[2] else "",
                severity="CRITICAL" if r[0] == "C" else "HIGH" if r[0] == "B" else "LOW",
                is_active=r[3] != "CLOSE"
            )
            for r in results
        ]
    
    def _get_hpd_complaints(self, bin_id: str) -> List[Dict]:
        results = self.con.execute("""
            SELECT complaint_id, status, complaint_type, major_category,
                   minor_category, problem_description, received_date
            FROM hpd_complaints 
            WHERE bin = ?
            ORDER BY received_date DESC
            LIMIT 20
        """, [bin_id]).fetchall()
        
        return [
            {
                "complaint_id": r[0], "status": r[1], "complaint_type": r[2],
                "major_category": r[3], "minor_category": r[4],
                "problem_description": r[5], "received_date": str(r[6]) if r[6] else ""
            }
            for r in results
        ]
    
    def _get_311_complaints(self, bin_id: str, address: str) -> List[Dict]:
        """Get 311 complaints — try BIN first, fall back to address match."""
        results = []
        
        if bin_id:
            results = self.con.execute("""
                SELECT unique_key, created_date, complaint_type, descriptor,
                       status, resolution_description, incident_address
                FROM service_requests_311 
                WHERE bin = ?
                ORDER BY created_date DESC
                LIMIT 20
            """, [bin_id]).fetchall()
        
        # Fallback to address matching if no BIN match
        if not results and address:
            addr_upper = address.upper().strip()
            results = self.con.execute("""
                SELECT unique_key, created_date, complaint_type, descriptor,
                       status, resolution_description, incident_address
                FROM service_requests_311 
                WHERE UPPER(incident_address) LIKE ?
                ORDER BY created_date DESC
                LIMIT 20
            """, [f"%{addr_upper}%"]).fetchall()
        
        return [
            {
                "id": r[0], "date": str(r[1]) if r[1] else "",
                "type": r[2], "descriptor": r[3],
                "status": r[4], "resolution": r[5], "address": r[6]
            }
            for r in results
        ]
    
    def _get_fire_history(self, lat: float, lon: float, radius_ft: float = 300) -> List[IncidentHistory]:
        """Get fire incidents near this location (within radius)."""
        if not lat or not lon:
            return []
        
        # Approximate bounding box (1 degree ≈ 364,000 ft at NYC latitude)
        delta = radius_ft / 364000
        
        results = self.con.execute("""
            SELECT incident_id, incident_datetime, incident_type_desc,
                   incident_classification, dispatch_response_seconds,
                   engines_assigned, ladders_assigned, latitude, longitude
            FROM fire_incidents 
            WHERE latitude BETWEEN ? AND ?
              AND longitude BETWEEN ? AND ?
            ORDER BY incident_datetime DESC
            LIMIT 20
        """, [lat - delta, lat + delta, lon - delta, lon + delta]).fetchall()
        
        # Filter by actual distance
        nearby = []
        for r in results:
            if r[7] and r[8]:
                dist = haversine_ft(lat, lon, r[7], r[8])
                if dist <= radius_ft:
                    nearby.append(IncidentHistory(
                        incident_type=r[3] or r[2] or "",
                        date=str(r[1]) if r[1] else "",
                        description=f"{r[2] or 'Incident'} - {r[5] or 0} engines, {r[6] or 0} ladders",
                        response_seconds=r[4],
                        details=f"Distance: {dist:.0f}ft from building"
                    ))
        
        return nearby
    
    def _get_ems_history(self, lat: float, lon: float, radius_ft: float = 300) -> List[IncidentHistory]:
        """Get EMS incidents near this location."""
        if not lat or not lon:
            return []
        
        delta = radius_ft / 364000
        
        results = self.con.execute("""
            SELECT cad_incident_id, incident_datetime, initial_call_type,
                   final_call_type, final_severity_level,
                   dispatch_response_seconds, latitude, longitude
            FROM ems_incidents 
            WHERE latitude BETWEEN ? AND ?
              AND longitude BETWEEN ? AND ?
            ORDER BY incident_datetime DESC
            LIMIT 15
        """, [lat - delta, lat + delta, lon - delta, lon + delta]).fetchall()
        
        nearby = []
        for r in results:
            if r[6] and r[7]:
                dist = haversine_ft(lat, lon, r[6], r[7])
                if dist <= radius_ft:
                    nearby.append(IncidentHistory(
                        incident_type=r[3] or r[2] or "",
                        date=str(r[1]) if r[1] else "",
                        description=f"Severity: {r[4] or 'N/A'}",
                        response_seconds=r[5]
                    ))
        
        return nearby
    
    def _get_nearest_hydrants(self, lat: float, lon: float, limit: int = 5) -> List[NearbyResource]:
        """Find nearest fire hydrants."""
        delta = 0.005  # ~600m bounding box
        
        results = self.con.execute("""
            SELECT latitude, longitude, unitid, borough
            FROM fire_hydrants
            WHERE latitude BETWEEN ? AND ?
              AND longitude BETWEEN ? AND ?
        """, [lat - delta, lat + delta, lon - delta, lon + delta]).fetchall()
        
        hydrants = []
        for r in results:
            dist = haversine_ft(lat, lon, r[0], r[1])
            hydrants.append(NearbyResource(
                name=f"Hydrant {r[2] or ''}",
                distance_ft=dist,
                latitude=r[0],
                longitude=r[1],
                type="hydrant"
            ))
        
        hydrants.sort(key=lambda h: h.distance_ft)
        return hydrants[:limit]
    
    def _get_nearest_hospital(self, lat: float, lon: float) -> Optional[NearbyResource]:
        """Find nearest hospital."""
        results = self.con.execute("""
            SELECT facility_name, latitude, longitude, facility_type
            FROM hospitals
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """).fetchall()
        
        if not results:
            return None
        
        nearest = None
        min_dist = float('inf')
        
        for r in results:
            dist = haversine_ft(lat, lon, r[1], r[2])
            if dist < min_dist:
                min_dist = dist
                nearest = NearbyResource(
                    name=r[0],
                    distance_ft=dist,
                    latitude=r[1],
                    longitude=r[2],
                    type="hospital"
                )
        
        return nearest


def _parse_date(date_str: str) -> datetime:
    """Try to parse various date formats."""
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(str(date_str)[:19], fmt)
        except (ValueError, TypeError):
            continue
    return datetime.min


# ─────────────────────────────────────────────
# CLI for testing
# ─────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    
    engine = BuildingIntelligence()
    
    if len(sys.argv) > 1:
        query = " ".join(sys.argv[1:])
    else:
        query = input("Enter address or BIN: ")
    
    print(f"\n🔍 Searching for: {query}\n")
    
    # Try direct profile
    profile = engine.get_profile(query)
    
    if profile:
        print(f"🏢 {profile.address}, {profile.borough}")
        print(f"   BIN: {profile.bin}")
        print(f"   Floors: {profile.num_floors} | Year Built: {profile.year_built}")
        print(f"   Building Class: {profile.building_class}")
        print(f"   Owner: {profile.owner_name}")
        print(f"\n⚠️  RISK: {profile.risk_level} (score: {profile.overall_risk_score:.1f})")
        print(f"   Active DOB Violations: {profile.active_dob_violations}")
        print(f"   Active HPD Class C (IMMEDIATELY HAZARDOUS): {profile.active_hpd_class_c}")
        print(f"   Active HPD Class B (HAZARDOUS): {profile.active_hpd_class_b}")
        print(f"   Prior Fire Incidents: {profile.prior_fire_incidents}")
        print(f"   311 Complaints (30d/90d/365d): {profile.complaints_30d}/{profile.complaints_90d}/{profile.complaints_365d}")
        
        if profile.nearest_hydrants:
            print(f"\n🚒 Nearest Hydrants:")
            for h in profile.nearest_hydrants[:3]:
                print(f"   {h.name}: {h.distance_ft:.0f} ft")
        
        if profile.nearest_hospital:
            miles = profile.nearest_hospital.distance_ft / 5280
            print(f"\n🏥 Nearest Hospital: {profile.nearest_hospital.name} ({miles:.1f} mi)")
        
        if profile.critical_violations:
            print(f"\n🔴 Critical Violations:")
            for v in profile.critical_violations[:5]:
                status = "ACTIVE" if v.is_active else "CLOSED"
                print(f"   [{status}] {v.source} - {v.description[:80]}")
        
        # Dump full JSON for LLM consumption
        print(f"\n📄 Full JSON profile: {len(profile.to_json())} chars")
    else:
        # Show search results
        print("No exact match. Searching...")
        results = engine.search_address(query)
        if results:
            for r in results:
                print(f"  BIN {r['bin']}: {r['address']}, {r['borough']} ({r['floors']} floors, built {r['year_built']})")
        else:
            print("  No buildings found.")
    
    engine.close()
