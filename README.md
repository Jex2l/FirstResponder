# 🚨 FirstResponder Copilot

**On-device AI tactical intelligence for NYC first responders.**

Built for the NVIDIA Spark Hack Series NYC — running entirely on the GB10 Grace Blackwell Superchip.

## What It Does

Enter any NYC address → Get an instant tactical intelligence brief synthesizing data from 15+ city agencies:
- Building structure (floors, age, construction type, elevators)
- Active violations (DOB, HPD Class A/B/C)
- Fire suppression system status
- 311 complaint history & velocity
- Historical fire/EMS incidents
- Nearest hydrants & hospitals
- Owner portfolio analysis (neglect pattern detection)

All running **locally on the GB10** — zero cloud, zero latency, zero data exposure.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    GB10 Unified Memory (128GB)               │
│                                                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐  │
│  │   DuckDB     │  │   FAISS      │  │  Nemotron Nano   │  │
│  │  (Building   │  │  (Address    │  │  (Tactical Brief │  │
│  │   Database)  │  │   Search)    │  │   Generation)    │  │
│  └──────┬───────┘  └──────┬───────┘  └────────┬─────────┘  │
│         │                  │                    │            │
│  ┌──────┴──────────────────┴────────────────────┴────────┐  │
│  │              Query Engine + Risk Scorer                │  │
│  └──────────────────────┬────────────────────────────────┘  │
│                         │                                    │
│  ┌──────────────────────┴────────────────────────────────┐  │
│  │              FastAPI Web Server (:8888)                │  │
│  └───────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Download & Load NYC Data
```bash
# Download all datasets from NYC Open Data (~2-5 GB total)
python ingest.py --download

# Load into DuckDB + compute risk scores
python ingest.py --load
python ingest.py --risk

# Or do everything at once:
python ingest.py --all
```

### 3. Set Up Nemotron Nano on GB10
```bash
# Option A: Ollama (easiest)
ollama pull nemotron-3-nano
ollama serve

# Option B: llama.cpp (fastest)
# Follow: https://build.nvidia.com/spark/nemotron
```

### 4. Launch the Copilot
```bash
# Start the web UI
python server.py

# Open http://localhost:8888
```

### 5. Test It
```bash
# CLI test
python brief_generator.py "350 5th Ave"

# Or search for any address
python query_engine.py "100 Centre St"
```

## Project Structure

```
firstresponder-copilot/
├── schema.sql           # DuckDB database schema
├── ingest.py            # Data download & loading pipeline  
├── query_engine.py      # Building intelligence query engine
├── brief_generator.py   # LLM tactical brief generation
├── server.py            # FastAPI web server + UI
├── requirements.txt     # Python dependencies
├── data/
│   ├── raw/             # Downloaded CSV files
│   └── responder.duckdb # Compiled database
└── README.md
```

## Data Sources

All data from [NYC Open Data](https://data.cityofnewyork.us):

| Source | Dataset | Records |
|--------|---------|---------|
| DOB | Building violations | ~2M |
| DOB | Safety violations | ~500K |
| HPD | Housing violations (Class A/B/C) | ~3M |
| HPD | Complaints & problems | ~3M |
| 311 | Service requests (safety-filtered) | ~500K |
| FDNY | Fire incident dispatch | ~2M |
| FDNY | Fire company incidents | ~2M |
| FDNY | Bureau of Fire Prevention inspections | Historical |
| EMS | Incident dispatch | ~2M |
| NYPD | Complaint data | ~500K |
| DCP | PLUTO (building profiles) | ~900K |
| DEP | Fire hydrant locations | ~200K |
| H+H | Hospital/facility locations | ~100 |
| DOB | Elevator device details | ~500K |
| HPD | Building registrations & contacts | ~1.5M |

## Why On-Device?

1. **Latency kills in emergencies.** Cloud API = 2-5s. Local inference = <500ms.
2. **Network fails when you need it most.** Cell towers go down during major incidents.
3. **Sensitive data stays local.** Law enforcement + building vulnerability data shouldn't traverse the internet.

## Team

Built at NVIDIA Spark Hack NYC 2026 — Track 1: Human Impact

## License

MIT
