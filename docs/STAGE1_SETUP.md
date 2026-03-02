# Stage 1 Detailed Setup

This page provides complete setup instructions for running Stage 1 of the UVH-26 viewpoint mapping pipeline.

## 1. Prerequisites
- Windows PowerShell
- Python virtual environment at `.venv`
- Internet access for Hugging Face dataset file retrieval

## 2. Environment Setup
From repository root:
```powershell
& .\.venv\Scripts\Activate.ps1
```

Install dependencies (if needed):
```powershell
pip install -r requirements.txt
```

## 3. Data Layout Expectations
Required raw paths created/used by the pipeline:
- `UVH26_Project/data/raw/UVH-26/UVH-26-Train/UVH-26-MV-Train.json`
- `UVH26_Project/data/raw/UVH-26/UVH-26-Val/UVH-26-MV-Val.json`
- `UVH26_Project/data/raw/UVH-26/UVH-26-Train/data/*.png`
- `UVH26_Project/data/raw/UVH-26/UVH-26-Val/data/*.png`

The loader downloads missing files when needed.

Git note:
- `UVH26_Project/data/raw/` is excluded by `.gitignore`.
- Raw UVH-26 files are not pushed to GitHub.
- On a fresh clone, running `src/main.py` is sufficient to populate required raw files automatically.

## 4. Run Stage 1
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\main.py
Pop-Location
```

## 5. Checkpoint / Resume
Stage 1 run id is configured in `UVH26_Project/src/main.py`.
Current run id: `masked40_rerun`.

Checkpoint files:
- `UVH26_Project/outputs/viewpoint_registry_masked40_rerun.pkl`
- `UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv`

Resume behavior:
- Rerun `src/main.py` after interruption.
- Pipeline resumes from `last_index` in registry file.

## 6. Progress Checks
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe -c "import pickle; from pathlib import Path; s=pickle.load(Path('outputs/viewpoint_registry_masked40_rerun.pkl').open('rb')); print('last_index', s.get('last_index')); print('viewpoints', len(s.get('viewpoints', {})))"
Pop-Location
```

```powershell
if (Test-Path .\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv) {
  (Get-Content .\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv | Measure-Object -Line).Lines
}
```

## 7. Review / Merge Workflow
Generate near-identical viewpoint candidates:
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\identical_viewpoints.py
Pop-Location
```

Interactive reviewer:
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\review_viewpoint_pairs.py
Pop-Location
```

Decision keys:
- `m` => merge
- `x` => no merge
- `q` => quit/save

## 8. Common Issues
- If processing stops due to network issues, rerun `src/main.py`; it resumes from checkpoint.
- If reviewer exits immediately, verify candidate CSV has undecided rows.
- If checkpoint mismatch error appears, ensure mapping and registry files belong to the same run id.
