"""
FirstResponder Copilot — Tactical Brief Generator
Takes a BuildingProfile and generates a structured tactical brief
using Nemotron Nano (or any OpenAI-compatible local LLM).

Designed to run on NVIDIA GB10 via llama.cpp or Ollama serving Nemotron.

Usage:
    from brief_generator import TacticalBriefGenerator
    gen = TacticalBriefGenerator()
    brief = gen.generate(profile)
"""

import json
import requests
from typing import Optional
from query_engine import BuildingProfile


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

# Default: Ollama running Nemotron Nano locally
LLM_BASE_URL = "http://localhost:11434/v1"  # Ollama OpenAI-compatible endpoint
LLM_MODEL = "nemotron-3-nano"               # Model name in Ollama

# Alternative: llama.cpp server
# LLM_BASE_URL = "http://localhost:8080/v1"
# LLM_MODEL = "nemotron-3-nano"

# Alternative: vLLM server  
# LLM_BASE_URL = "http://localhost:8000/v1"
# LLM_MODEL = "nemotron-3-nano"


SYSTEM_PROMPT = """You are FirstResponder Copilot, an AI system that generates tactical intelligence briefs for emergency first responders in New York City. 

Your job is to take structured building data and produce a clear, scannable tactical brief that a firefighter, EMS worker, or police officer can read in 10-15 seconds to understand the critical hazards and resources at an incident location.

RULES:
1. Lead with the MOST DANGEROUS information first
2. Use short, direct sentences - no filler words
3. Flag specific hazard types: GAS LEAK, STRUCTURAL, ELECTRICAL, FIRE SUPPRESSION FAILURE, ILLEGAL CONVERSION
4. Include actionable tactical notes (e.g., "standpipe unreliable above floor 6")
5. Always mention elevator status for buildings over 6 floors
6. Include nearest hydrant distance and nearest hospital
7. If the building owner has a pattern of violations across multiple buildings, flag it as "NEGLECT PATTERN"
8. Quantify everything — use numbers, not vague language
9. Highlight any UNRESOLVED complaints from 311 or HPD that indicate active hazards
10. Format for maximum scannability with clear section headers

OUTPUT FORMAT:
Use exactly this structure:

TACTICAL BRIEF — [ADDRESS], [BOROUGH]
═══════════════════════════════════════

BUILDING: [floors]-story, [construction type], [building class], built [year]. [units] units. [elevator info].

🔴 CRITICAL ALERTS:
[List the most dangerous active conditions — gas leaks, structural failures, fire suppression issues]

⚠️ HAZARD HISTORY:
[Prior fire incidents, violation patterns, complaint velocity]

🔧 TACTICAL NOTES:
[Specific actionable guidance for responders based on the data]

📍 NEAREST RESOURCES:
[Hydrants with distance, hospital with distance]

👤 OWNER INTEL:
[Owner name, portfolio info, violation patterns if relevant]
"""


def build_context(profile: BuildingProfile) -> str:
    """Convert a BuildingProfile into a structured context string for the LLM."""
    
    context = f"""
BUILDING DATA FOR: {profile.address}, {profile.borough} {profile.zipcode}
BIN: {profile.bin}

=== STRUCTURE ===
Floors: {profile.num_floors}
Year Built: {profile.year_built}
Building Class: {profile.building_class}
Land Use: {profile.land_use}
Construction Type: {profile.construction_type}
Residential Units: {profile.residential_units}
Total Units: {profile.total_units}
Building Area: {profile.building_area:,.0f} sqft
Owner: {profile.owner_name}

=== RISK ASSESSMENT ===
Overall Risk Score: {profile.overall_risk_score:.1f}
Risk Level: {profile.risk_level}
Active DOB Violations: {profile.active_dob_violations}
Active HPD Class C (Immediately Hazardous): {profile.active_hpd_class_c}
Active HPD Class B (Hazardous): {profile.active_hpd_class_b}
Prior Fire Incidents (within 300ft): {profile.prior_fire_incidents}
Prior EMS Incidents (within 300ft): {profile.prior_ems_incidents}
311 Complaints: {profile.complaints_30d} (30d) / {profile.complaints_90d} (90d) / {profile.complaints_365d} (365d)

=== ELEVATOR STATUS ===
Total Elevators: {profile.elevator_count}
Out of Service: {profile.elevator_out_of_service}
"""
    
    # Add critical violations
    if profile.critical_violations:
        context += "\n=== ACTIVE VIOLATIONS (most critical first) ===\n"
        for v in profile.critical_violations[:15]:
            status = "🔴 ACTIVE" if v.is_active else "✅ CLOSED"
            context += f"[{status}] {v.source} | {v.severity} | {v.date} | {v.description[:150]}\n"
    
    # Add HPD complaints
    if profile.recent_complaints:
        context += "\n=== RECENT HPD COMPLAINTS ===\n"
        for c in profile.recent_complaints[:10]:
            context += f"[{c.severity}] {c.violation_type} | {c.date} | {c.description[:150]}\n"
    
    # Add 311 complaints
    if profile.raw_311_complaints:
        context += "\n=== 311 SERVICE REQUESTS ===\n"
        for c in profile.raw_311_complaints[:10]:
            context += f"[{c.get('status', 'N/A')}] {c.get('type', '')} - {c.get('descriptor', '')} | {c.get('date', '')} | {c.get('resolution', '')[:100]}\n"
    
    # Add fire incident history
    if profile.incident_history:
        context += "\n=== FIRE INCIDENT HISTORY ===\n"
        for inc in profile.incident_history[:10]:
            rt = f" (response: {inc.response_seconds}s)" if inc.response_seconds else ""
            context += f"{inc.date} | {inc.incident_type} | {inc.description}{rt}\n"
    
    # Add nearby resources
    context += "\n=== NEAREST RESOURCES ===\n"
    if profile.nearest_hydrants:
        for h in profile.nearest_hydrants[:3]:
            context += f"Hydrant: {h.name} — {h.distance_ft:.0f} ft\n"
    
    if profile.nearest_hospital:
        miles = profile.nearest_hospital.distance_ft / 5280
        context += f"Hospital: {profile.nearest_hospital.name} — {miles:.1f} miles\n"
    
    # Owner intel
    if profile.owner_total_buildings > 1:
        context += f"\n=== OWNER PORTFOLIO ===\n"
        context += f"Owner {profile.owner_name} operates {profile.owner_total_buildings} buildings with {profile.owner_total_violations} total violations.\n"
    
    return context


class TacticalBriefGenerator:
    """Generates tactical briefs using a local LLM (Nemotron Nano on GB10)."""
    
    def __init__(self, base_url: str = LLM_BASE_URL, model: str = LLM_MODEL):
        self.base_url = base_url.rstrip('/')
        self.model = model
    
    def generate(self, profile: BuildingProfile, follow_up: str = None) -> str:
        """
        Generate a tactical brief from a building profile.
        
        Args:
            profile: Complete building intelligence profile
            follow_up: Optional follow-up question about the building
            
        Returns:
            Formatted tactical brief string
        """
        context = build_context(profile)
        
        if follow_up:
            user_message = f"""Based on the following building data, answer this question: {follow_up}

{context}"""
        else:
            user_message = f"""Generate a tactical brief for this building based on the following data:

{context}"""
        
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_message}
        ]
        
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                json={
                    "model": self.model,
                    "messages": messages,
                    "temperature": 0.3,  # Low temperature for factual accuracy
                    "max_tokens": 2000,
                    "stream": False
                },
                timeout=60
            )
            response.raise_for_status()
            data = response.json()
            return data["choices"][0]["message"]["content"]
            
        except requests.exceptions.ConnectionError:
            # LLM server not running — generate a template-based brief instead
            return self._generate_template_brief(profile)
        except Exception as e:
            return f"Error generating brief: {e}\n\n" + self._generate_template_brief(profile)
    
    def _generate_template_brief(self, p: BuildingProfile) -> str:
        """
        Fallback: generate a brief using templates when LLM is unavailable.
        Still useful for demo and testing without the LLM running.
        """
        
        # Determine critical alerts
        alerts = []
        
        # Check for gas-related complaints
        gas_complaints = [c for c in p.raw_311_complaints if 'gas' in c.get('type', '').lower() or 'gas' in c.get('descriptor', '').lower()]
        if gas_complaints:
            latest = gas_complaints[0]
            alerts.append(f"Gas complaint filed on {latest.get('date', 'unknown date')} — status: {latest.get('status', 'UNKNOWN')}")
        
        # Check for structural violations
        structural = [v for v in p.critical_violations if v.is_active and ('STRUCT' in v.description.upper() or v.severity == 'CRITICAL')]
        if structural:
            alerts.append(f"{len(structural)} active structural/critical DOB violation(s)")
        
        # Check for HPD Class C
        if p.active_hpd_class_c > 0:
            alerts.append(f"{p.active_hpd_class_c} HPD Class C (IMMEDIATELY HAZARDOUS) violation(s) open")
        
        # Check for fire suppression issues
        fire_violations = [v for v in p.critical_violations if any(kw in v.description.upper() for kw in ['SPRINKLER', 'STANDPIPE', 'FIRE', 'SUPPRESSION', 'ALARM'])]
        if fire_violations:
            alerts.append(f"Fire suppression system concern: {fire_violations[0].description[:100]}")
        
        # Build the brief
        brief = f"""
TACTICAL BRIEF — {p.address}, {p.borough}
═══════════════════════════════════════

BUILDING: {p.num_floors}-story, {p.construction_type or 'type unknown'}, class {p.building_class}, built {p.year_built}. {p.total_units} units. {f'{p.elevator_count} elevators ({p.elevator_out_of_service} out of service)' if p.elevator_count > 0 else 'No elevator data'}.

🔴 CRITICAL ALERTS:
"""
        if alerts:
            for a in alerts:
                brief += f"• {a}\n"
        else:
            brief += "• No critical alerts identified from available data.\n"
        
        brief += f"""
⚠️ HAZARD HISTORY:
• {p.prior_fire_incidents} fire incident(s) within 300ft of building
• {p.prior_ems_incidents} EMS incident(s) within 300ft
• {p.active_dob_violations} active DOB violations
• {p.active_hpd_class_b} HPD Class B (hazardous) violations open
• 311 complaint velocity: {p.complaints_30d} (30d) / {p.complaints_90d} (90d) / {p.complaints_365d} (1yr)

🔧 TACTICAL NOTES:
"""
        # Generate tactical notes
        if p.num_floors > 6 and p.elevator_count == 0:
            brief += "• HIGH-RISE with no elevator data — plan for stair operations\n"
        if p.year_built and p.year_built < 1970:
            brief += f"• Pre-1970 construction ({p.year_built}) — potential asbestos, lead paint, outdated electrical\n"
        if p.active_hpd_class_c > 0:
            brief += f"• {p.active_hpd_class_c} IMMEDIATELY HAZARDOUS conditions documented by HPD\n"
        if gas_complaints:
            brief += "• RECENT GAS COMPLAINT — approach with gas detection equipment\n"
        if p.complaints_30d > 5:
            brief += f"• HIGH complaint velocity ({p.complaints_30d} in 30 days) — building in active distress\n"
        
        brief += f"""
📍 NEAREST RESOURCES:
"""
        if p.nearest_hydrants:
            for h in p.nearest_hydrants[:3]:
                direction = ""  # Could compute direction from lat/lon
                brief += f"• Hydrant: {h.distance_ft:.0f}ft {direction}\n"
        else:
            brief += "• Hydrant data not available\n"
        
        if p.nearest_hospital:
            miles = p.nearest_hospital.distance_ft / 5280
            brief += f"• Hospital: {p.nearest_hospital.name} — {miles:.1f} mi\n"
        
        brief += f"""
👤 OWNER INTEL:
• Owner: {p.owner_name}
"""
        if p.owner_total_buildings > 1:
            brief += f"• Portfolio: {p.owner_total_buildings} buildings, {p.owner_total_violations} total violations\n"
            if p.owner_total_violations / max(p.owner_total_buildings, 1) > 10:
                brief += "• ⚠️ NEGLECT PATTERN — high violation rate across portfolio\n"
        
        return brief.strip()
    
    def ask(self, profile: BuildingProfile, question: str) -> str:
        """Ask a follow-up question about a building."""
        return self.generate(profile, follow_up=question)


# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────

if __name__ == "__main__":
    from query_engine import BuildingIntelligence
    import sys
    
    address = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else input("Enter address: ")
    
    print(f"\n🔍 Looking up: {address}")
    
    engine = BuildingIntelligence()
    profile = engine.get_profile(address)
    
    if not profile:
        print("❌ Building not found")
        engine.close()
        sys.exit(1)
    
    print(f"✅ Found: {profile.address}, {profile.borough} (BIN: {profile.bin})")
    print(f"🧮 Risk Score: {profile.overall_risk_score:.1f} ({profile.risk_level})")
    print("\n⏳ Generating tactical brief...\n")
    
    gen = TacticalBriefGenerator()
    brief = gen.generate(profile)
    
    print(brief)
    
    # Interactive follow-up
    print("\n" + "=" * 60)
    print("💬 Ask follow-up questions (or 'quit' to exit)")
    print("=" * 60)
    
    while True:
        q = input("\n> ").strip()
        if q.lower() in ('quit', 'exit', 'q'):
            break
        if q:
            answer = gen.ask(profile, q)
            print(f"\n{answer}")
    
    engine.close()
