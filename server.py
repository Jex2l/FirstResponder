"""
FirstResponder Copilot — Web Server
FastAPI application serving the tactical brief UI.

Usage:
    uvicorn server:app --host 0.0.0.0 --port 8888 --reload
"""

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import json
import os

from query_engine import BuildingIntelligence
from brief_generator import TacticalBriefGenerator

app = FastAPI(title="FirstResponder Copilot", version="0.1.0")

# Initialize engines
DB_PATH = os.environ.get("DB_PATH", "data/responder.duckdb")
engine = None
brief_gen = None


def get_engine():
    global engine
    if engine is None:
        engine = BuildingIntelligence(DB_PATH)
    return engine


def get_brief_gen():
    global brief_gen
    if brief_gen is None:
        brief_gen = TacticalBriefGenerator()
    return brief_gen


# ─────────────────────────────────────────────
# API Endpoints
# ─────────────────────────────────────────────

@app.get("/api/search")
async def search_buildings(q: str):
    """Search for buildings by address."""
    eng = get_engine()
    results = eng.search_address(q)
    return JSONResponse(content={"results": results})


@app.get("/api/profile/{bin_or_address:path}")
async def get_building_profile(bin_or_address: str):
    """Get full building intelligence profile."""
    eng = get_engine()
    profile = eng.get_profile(bin_or_address)
    
    if not profile:
        return JSONResponse(
            status_code=404,
            content={"error": f"Building not found: {bin_or_address}"}
        )
    
    return JSONResponse(content=profile.to_dict())


@app.get("/api/brief/{bin_or_address:path}")
async def get_tactical_brief(bin_or_address: str):
    """Generate a tactical brief for a building."""
    eng = get_engine()
    profile = eng.get_profile(bin_or_address)
    
    if not profile:
        return JSONResponse(
            status_code=404,
            content={"error": f"Building not found: {bin_or_address}"}
        )
    
    gen = get_brief_gen()
    brief = gen.generate(profile)
    
    return JSONResponse(content={
        "address": profile.address,
        "borough": profile.borough,
        "bin": profile.bin,
        "risk_level": profile.risk_level,
        "risk_score": profile.overall_risk_score,
        "brief": brief,
        "profile": profile.to_dict()
    })


@app.post("/api/ask")
async def ask_followup(request: Request):
    """Ask a follow-up question about a building."""
    body = await request.json()
    bin_or_address = body.get("address", "")
    question = body.get("question", "")
    
    eng = get_engine()
    profile = eng.get_profile(bin_or_address)
    
    if not profile:
        return JSONResponse(
            status_code=404,
            content={"error": "Building not found"}
        )
    
    gen = get_brief_gen()
    answer = gen.ask(profile, question)
    
    return JSONResponse(content={"answer": answer})


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "db": DB_PATH}


# ─────────────────────────────────────────────
# Frontend
# ─────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve the main UI."""
    return """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>FirstResponder Copilot</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        :root {
            --bg: #0a0a0f;
            --surface: #12121a;
            --surface2: #1a1a25;
            --border: #2a2a3a;
            --text: #e0e0e8;
            --text-dim: #8888a0;
            --accent: #ff4444;
            --accent-glow: rgba(255, 68, 68, 0.15);
            --warning: #ffaa00;
            --safe: #44cc88;
            --blue: #4488ff;
        }
        
        body {
            font-family: 'SF Mono', 'Fira Code', 'Consolas', monospace;
            background: var(--bg);
            color: var(--text);
            min-height: 100vh;
        }
        
        .header {
            background: linear-gradient(180deg, #1a0000 0%, var(--bg) 100%);
            border-bottom: 1px solid var(--accent);
            padding: 20px 30px;
            display: flex;
            align-items: center;
            gap: 15px;
        }
        
        .header h1 {
            font-size: 1.4rem;
            color: var(--accent);
            font-weight: 700;
            letter-spacing: 2px;
            text-transform: uppercase;
        }
        
        .header .badge {
            background: var(--accent);
            color: #000;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 0.65rem;
            font-weight: 800;
            letter-spacing: 1px;
        }
        
        .header .gpu-badge {
            background: #76b900;
            color: #000;
            padding: 2px 8px;
            border-radius: 3px;
            font-size: 0.65rem;
            font-weight: 800;
            margin-left: auto;
        }
        
        .search-bar {
            padding: 20px 30px;
            background: var(--surface);
            border-bottom: 1px solid var(--border);
        }
        
        .search-container {
            display: flex;
            gap: 10px;
            max-width: 900px;
        }
        
        .search-input {
            flex: 1;
            background: var(--bg);
            border: 2px solid var(--border);
            color: var(--text);
            padding: 14px 18px;
            font-size: 1.1rem;
            font-family: inherit;
            border-radius: 6px;
            outline: none;
            transition: border-color 0.2s;
        }
        
        .search-input:focus {
            border-color: var(--accent);
            box-shadow: 0 0 20px var(--accent-glow);
        }
        
        .search-input::placeholder {
            color: var(--text-dim);
        }
        
        .search-btn {
            background: var(--accent);
            color: #fff;
            border: none;
            padding: 14px 28px;
            font-size: 1rem;
            font-family: inherit;
            font-weight: 700;
            border-radius: 6px;
            cursor: pointer;
            letter-spacing: 1px;
            text-transform: uppercase;
            transition: all 0.2s;
        }
        
        .search-btn:hover {
            background: #ff6666;
            transform: translateY(-1px);
        }
        
        .search-btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        
        .main {
            display: grid;
            grid-template-columns: 1fr 380px;
            gap: 0;
            max-height: calc(100vh - 140px);
        }
        
        .brief-panel {
            padding: 25px 30px;
            overflow-y: auto;
        }
        
        .sidebar {
            background: var(--surface);
            border-left: 1px solid var(--border);
            padding: 20px;
            overflow-y: auto;
        }
        
        .brief-content {
            white-space: pre-wrap;
            line-height: 1.6;
            font-size: 0.9rem;
        }
        
        .risk-badge {
            display: inline-block;
            padding: 6px 16px;
            border-radius: 4px;
            font-weight: 800;
            font-size: 0.85rem;
            letter-spacing: 2px;
            margin-bottom: 15px;
        }
        
        .risk-CRITICAL { background: #ff0000; color: #fff; animation: pulse 1.5s infinite; }
        .risk-HIGH { background: #ff6600; color: #fff; }
        .risk-MEDIUM { background: #ffaa00; color: #000; }
        .risk-LOW { background: var(--safe); color: #000; }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.7; }
        }
        
        .stat-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-bottom: 20px;
        }
        
        .stat-card {
            background: var(--surface2);
            border: 1px solid var(--border);
            border-radius: 6px;
            padding: 12px;
        }
        
        .stat-card .label {
            font-size: 0.7rem;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .stat-card .value {
            font-size: 1.4rem;
            font-weight: 700;
            margin-top: 4px;
        }
        
        .stat-card .value.danger { color: var(--accent); }
        .stat-card .value.warning { color: var(--warning); }
        .stat-card .value.safe { color: var(--safe); }
        
        .section-title {
            font-size: 0.75rem;
            color: var(--text-dim);
            text-transform: uppercase;
            letter-spacing: 2px;
            margin: 20px 0 10px 0;
            padding-bottom: 5px;
            border-bottom: 1px solid var(--border);
        }
        
        .chat-container {
            margin-top: 20px;
            border-top: 1px solid var(--border);
            padding-top: 15px;
        }
        
        .chat-input-row {
            display: flex;
            gap: 8px;
        }
        
        .chat-input {
            flex: 1;
            background: var(--bg);
            border: 1px solid var(--border);
            color: var(--text);
            padding: 10px 12px;
            font-size: 0.85rem;
            font-family: inherit;
            border-radius: 4px;
            outline: none;
        }
        
        .chat-input:focus { border-color: var(--blue); }
        
        .chat-btn {
            background: var(--blue);
            color: #fff;
            border: none;
            padding: 10px 16px;
            font-family: inherit;
            font-weight: 700;
            border-radius: 4px;
            cursor: pointer;
            font-size: 0.8rem;
        }
        
        .chat-messages {
            margin-top: 10px;
            max-height: 300px;
            overflow-y: auto;
        }
        
        .chat-msg {
            background: var(--surface2);
            border-radius: 6px;
            padding: 10px;
            margin-bottom: 8px;
            font-size: 0.82rem;
            line-height: 1.5;
        }
        
        .loading {
            display: none;
            text-align: center;
            padding: 60px;
            color: var(--text-dim);
        }
        
        .loading.active { display: block; }
        
        .loading .spinner {
            width: 40px;
            height: 40px;
            border: 3px solid var(--border);
            border-top-color: var(--accent);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
            margin: 0 auto 15px;
        }
        
        @keyframes spin { to { transform: rotate(360deg); } }
        
        .empty-state {
            text-align: center;
            padding: 100px 40px;
            color: var(--text-dim);
        }
        
        .empty-state h2 {
            font-size: 1.2rem;
            color: var(--text);
            margin-bottom: 10px;
        }
        
        .empty-state p {
            font-size: 0.85rem;
            line-height: 1.6;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>🚨 FirstResponder Copilot</h1>
        <span class="badge">ON-DEVICE AI</span>
        <span class="gpu-badge">⚡ NVIDIA GB10</span>
    </div>
    
    <div class="search-bar">
        <div class="search-container">
            <input type="text" class="search-input" id="searchInput" 
                   placeholder="Enter any NYC address or BIN number..."
                   onkeydown="if(event.key==='Enter') search()">
            <button class="search-btn" id="searchBtn" onclick="search()">
                ANALYZE
            </button>
        </div>
    </div>
    
    <div class="main">
        <div class="brief-panel" id="briefPanel">
            <div class="empty-state" id="emptyState">
                <h2>Enter an address to generate a tactical brief</h2>
                <p>Type any NYC address (e.g., "350 5th Ave" or "100 Centre St")<br>
                The system will analyze building data from 15+ NYC agencies<br>
                and generate an intelligence brief in seconds.</p>
            </div>
            <div class="loading" id="loading">
                <div class="spinner"></div>
                <div>Querying building intelligence database...</div>
                <div style="margin-top:8px; font-size:0.8rem;">Analyzing violations, incidents, and hazards</div>
            </div>
            <div id="briefContent" style="display:none;"></div>
        </div>
        
        <div class="sidebar" id="sidebar" style="display:none;">
            <div class="section-title">Building Profile</div>
            <div id="buildingInfo"></div>
            
            <div class="section-title">Risk Metrics</div>
            <div class="stat-grid" id="statsGrid"></div>
            
            <div class="section-title">Follow-up Questions</div>
            <div class="chat-container">
                <div class="chat-input-row">
                    <input type="text" class="chat-input" id="chatInput" 
                           placeholder="Ask about this building..."
                           onkeydown="if(event.key==='Enter') askQuestion()">
                    <button class="chat-btn" onclick="askQuestion()">ASK</button>
                </div>
                <div class="chat-messages" id="chatMessages"></div>
            </div>
        </div>
    </div>

    <script>
        let currentAddress = '';
        
        async function search() {
            const query = document.getElementById('searchInput').value.trim();
            if (!query) return;
            
            currentAddress = query;
            
            // Show loading
            document.getElementById('emptyState').style.display = 'none';
            document.getElementById('briefContent').style.display = 'none';
            document.getElementById('loading').classList.add('active');
            document.getElementById('sidebar').style.display = 'none';
            document.getElementById('searchBtn').disabled = true;
            
            try {
                const response = await fetch(`/api/brief/${encodeURIComponent(query)}`);
                const data = await response.json();
                
                if (response.ok) {
                    displayBrief(data);
                } else {
                    displayError(data.error || 'Building not found');
                }
            } catch (err) {
                displayError('Connection error: ' + err.message);
            } finally {
                document.getElementById('loading').classList.remove('active');
                document.getElementById('searchBtn').disabled = false;
            }
        }
        
        function displayBrief(data) {
            const briefEl = document.getElementById('briefContent');
            briefEl.style.display = 'block';
            
            // Risk badge
            const riskClass = `risk-${data.risk_level}`;
            
            briefEl.innerHTML = `
                <div class="risk-badge ${riskClass}">${data.risk_level} RISK — Score: ${data.risk_score.toFixed(1)}</div>
                <div class="brief-content">${escapeHtml(data.brief)}</div>
            `;
            
            // Sidebar
            const sidebar = document.getElementById('sidebar');
            sidebar.style.display = 'block';
            
            const p = data.profile;
            document.getElementById('buildingInfo').innerHTML = `
                <div style="font-size:0.85rem; line-height:1.6;">
                    <strong>${p.address}</strong><br>
                    ${p.borough} ${p.zipcode}<br>
                    BIN: ${p.bin}<br>
                    ${p.num_floors} floors · Built ${p.year_built}<br>
                    Class: ${p.building_class}<br>
                    Owner: ${p.owner_name || 'Unknown'}
                </div>
            `;
            
            const dob = p.active_dob_violations || 0;
            const hpdC = p.active_hpd_class_c || 0;
            const hpdB = p.active_hpd_class_b || 0;
            const fires = p.prior_fire_incidents || 0;
            
            document.getElementById('statsGrid').innerHTML = `
                <div class="stat-card">
                    <div class="label">DOB Violations</div>
                    <div class="value ${dob > 5 ? 'danger' : dob > 0 ? 'warning' : 'safe'}">${dob}</div>
                </div>
                <div class="stat-card">
                    <div class="label">HPD Class C</div>
                    <div class="value ${hpdC > 0 ? 'danger' : 'safe'}">${hpdC}</div>
                </div>
                <div class="stat-card">
                    <div class="label">HPD Class B</div>
                    <div class="value ${hpdB > 3 ? 'danger' : hpdB > 0 ? 'warning' : 'safe'}">${hpdB}</div>
                </div>
                <div class="stat-card">
                    <div class="label">Fire History</div>
                    <div class="value ${fires > 2 ? 'danger' : fires > 0 ? 'warning' : 'safe'}">${fires}</div>
                </div>
                <div class="stat-card">
                    <div class="label">311 (30d)</div>
                    <div class="value ${p.complaints_30d > 5 ? 'danger' : p.complaints_30d > 0 ? 'warning' : 'safe'}">${p.complaints_30d || 0}</div>
                </div>
                <div class="stat-card">
                    <div class="label">311 (1yr)</div>
                    <div class="value">${p.complaints_365d || 0}</div>
                </div>
            `;
            
            // Clear chat
            document.getElementById('chatMessages').innerHTML = '';
        }
        
        function displayError(msg) {
            const briefEl = document.getElementById('briefContent');
            briefEl.style.display = 'block';
            briefEl.innerHTML = `<div style="color: var(--accent); padding: 40px;">❌ ${escapeHtml(msg)}</div>`;
        }
        
        async function askQuestion() {
            const input = document.getElementById('chatInput');
            const question = input.value.trim();
            if (!question || !currentAddress) return;
            
            input.value = '';
            
            const msgContainer = document.getElementById('chatMessages');
            msgContainer.innerHTML += `<div class="chat-msg" style="color:var(--blue);">Q: ${escapeHtml(question)}</div>`;
            msgContainer.innerHTML += `<div class="chat-msg" id="pendingAnswer">⏳ Thinking...</div>`;
            
            try {
                const response = await fetch('/api/ask', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ address: currentAddress, question })
                });
                const data = await response.json();
                
                document.getElementById('pendingAnswer').id = '';
                const answers = msgContainer.querySelectorAll('.chat-msg');
                answers[answers.length - 1].innerHTML = escapeHtml(data.answer || data.error || 'No answer');
                
            } catch (err) {
                document.getElementById('pendingAnswer').innerHTML = '❌ Error: ' + err.message;
                document.getElementById('pendingAnswer').id = '';
            }
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
    </script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8888)
