# Stage 3 Setup Guide

Stage 3 adds an interactive dashboard for browsing Stage 2 analytics output.

## 0) Prerequisite
Make sure Stage 2 full output exists:
- `UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp.jsonl`

## 1) Install dependencies
PowerShell:
```powershell
& .\.venv\Scripts\Activate.ps1
pip install -r requirements-stage3.txt
```

Bash:
```bash
source .venv/bin/activate
pip install -r requirements-stage3.txt
```

## 2) Start dashboard
PowerShell:
```powershell
streamlit run .\stage3\dashboard_app.py
```

Bash:
```bash
streamlit run ./stage3/dashboard_app.py
```

Open browser:
- `http://localhost:8501`

## 3) What to check
- Top congestion viewpoints chart
- Vehicle-density scatter
- Ratio analysis chart
- Class distribution for selected viewpoint

## 4) Example exploration workflow
1. Set minimum images per viewpoint to 20.
2. Review top 20 congestion viewpoints.
3. Select one high-congestion viewpoint in deep dive.
4. Inspect class distribution to understand congestion composition.