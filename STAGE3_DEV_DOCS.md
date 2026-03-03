# Stage 3 Developer Documentation

## Stage 3 Goal
Stage 3 adds an insight layer over Stage 2 outputs so inferred traffic metrics can be explored interactively.

Why this stage:
- Stage 2 produces rich viewpoint-level statistics, but JSONL alone is hard to interpret quickly.
- Teams need a fast way to inspect congestion hotspots, composition shifts, and entropy patterns.
- Stage 3 turns computed metrics into explorable visuals for validation and decision-making.

## Core File
- `stage3/dashboard_app.py`

Input source:
- `UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp.jsonl`

## Dashboard Features
- KPI summary cards:
  - viewpoint count
  - total vehicles
  - mean congestion
  - mean bbox density
- Top congestion ranking chart.
- Scatter: `avg_vehicle_count` vs `avg_bbox_density`.
- Ratio view: heavy vehicle ratio vs two-wheeler ratio.
- Congestion histogram.
- Top viewpoints table.
- Single-viewpoint deep dive with class distribution chart.

## Filters
- `top_n` viewpoint ranking size
- minimum images per viewpoint
- maximum entropy threshold

## Stage 3 Run
PowerShell:
```powershell
& .\.venv\Scripts\Activate.ps1
pip install -r requirements-stage3.txt
streamlit run .\stage3\dashboard_app.py
```

Bash:
```bash
source .venv/bin/activate
pip install -r requirements-stage3.txt
streamlit run ./stage3/dashboard_app.py
```

Default URL:
- `http://localhost:8501`

## Notes
- Stage 3 is local visualization and does not replace Stage 2 distributed compute.
- If input file path differs, update the sidebar `Analytics JSONL` input in the app.