# Stage 2 Setup Guide

This page is designed for fresh clones so users can copy-paste commands and run Stage 2 without local project assumptions.

## 0) Clone and open project
PowerShell:
```powershell
git clone <your-fork-or-repo-url>
cd DS-City-Analytics
```

Bash:
```bash
git clone <your-fork-or-repo-url>
cd DS-City-Analytics
```

## 1) Prerequisites
- Python 3.9+
- Java 11+
- Apache Spark 3.5+

## 2) Install dependencies
From repository root:

PowerShell:
```powershell
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-stage2.txt
```

Bash:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-stage2.txt
```

## 3) Generate Stage 1 mapping (required once)
PowerShell:
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\main.py
Pop-Location
```

Bash:
```bash
cd UVH26_Project
../.venv/bin/python src/main.py
cd ..
```

Expected output:
- `UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv`

## 4) Build Spark-friendly grouped mapping
PowerShell:
```powershell
& .\.venv\Scripts\python.exe .\spark_jobs\build_viewpoint_mapping.py --input-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-dir ..\UVH26_Project\outputs\stage2\mappings_full_26646 --num-node-buckets 2
```

Bash:
```bash
./.venv/bin/python ./spark_jobs/build_viewpoint_mapping.py --input-csv ../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv --output-dir ../UVH26_Project/outputs/stage2/mappings_full_26646 --num-node-buckets 2
```

Generated files:
- `viewpoint_to_images.jsonl`
- `viewpoint_to_images.csv`
- `viewpoint_bucket_assignment.csv`
- `bucket_summary.csv`

## 5) Run Stage 2 smoke test
First create smoke mapping:
PowerShell:
```powershell
& .\.venv\Scripts\python.exe .\spark_jobs\create_smoke_mapping.py --input-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --max-viewpoints 10 --max-images 100
```

Bash:
```bash
./.venv/bin/python ./spark_jobs/create_smoke_mapping.py --input-csv ../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv --output-csv ../UVH26_Project/outputs/image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --max-viewpoints 10 --max-images 100
```

Then run smoke analytics:
PowerShell:
```powershell
spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp_preview.jsonl --preview-limit 20
```

Bash:
```bash
spark-submit --master spark://<master-host>:7077 ./spark_jobs/analytics_job.py --mapping-csv ../UVH26_Project/outputs/image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --output-dir ../UVH26_Project/outputs/stage2/viewpoint_analytics_smoke_10vp --preview-jsonl ../UVH26_Project/outputs/stage2/viewpoint_analytics_smoke_10vp_preview.jsonl --preview-limit 20
```

## 6) Expand to full run
PowerShell:
```powershell
spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp_preview.jsonl --preview-limit 50
```

Bash:
```bash
spark-submit --master spark://<master-host>:7077 ./spark_jobs/analytics_job.py --mapping-csv ../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv --output-dir ../UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp --preview-jsonl ../UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp_preview.jsonl --preview-limit 50
```

Optional compute-only validation:
- add `--skip-output-write`

## 7) Validate run outputs
- Preview file exists (`*_preview.jsonl`)
- Expected fields present:
  - `viewpoint_id`
  - `total_images`
  - `total_vehicles`
  - `avg_vehicle_count`
  - `per_vehicle_count`
  - `avg_bbox_density`
  - `congestion_index`

## 8) Distributed execution evidence (for docs/GitHub)
Keep:
- `spark_jobs/logs/stage2_analytics_*.log`

From logs, verify:
- `Spark master: spark://...` (distributed)
- `Default parallelism`
- `Shuffle partitions`
- `Input mapping rows`
- `Joined annotation rows`
- `Output viewpoint rows`

If logs show `Spark master: local[*]`, that run is local mode only.

## 9) Common issues
- **Path not found**: ensure commands run from repository root.
- **Java/Spark not found**: verify `java -version` and `spark-submit --version` in same shell.
- **OOM during shuffle**: increase executor memory or tune shuffle partitions.

## 10) Quick start block (copy-paste)
```powershell
git clone <your-fork-or-repo-url>
cd DS-City-Analytics
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-stage2.txt
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\main.py
Pop-Location
& .\.venv\Scripts\python.exe .\spark_jobs\create_smoke_mapping.py --input-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --max-viewpoints 10 --max-images 100
spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp_preview.jsonl --preview-limit 20
```
