# DS-City Analytics

This repository contains:
- **Stage 1**: deterministic viewpoint reconstruction from UVH-26 images.
- **Stage 2**: Spark-based viewpoint analytics (smoke and full-scale runs).

## Motivation and Problem
- Large traffic datasets become slow and difficult to analyze with single-machine workflows.
- Stage 1 reconstructs camera viewpoints, but raw `image_id -> viewpoint_id` pairs are not ideal for distributed scheduling.
- Stage 2 addresses this by using Spark for distributed aggregation and by building a one-to-many viewpoint index that is easier to split across nodes.

## What this solves
- scalable per-viewpoint analytics over tens of thousands of images,
- reproducible smoke-to-full validation flow,
- clear run evidence for distributed execution claims.

## 1) Prerequisites
- Python 3.9+
- Java 11+
- Apache Spark 3.5+

## 2) Install
From repo root:

```powershell
& .\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-stage2.txt
```

## 2.1 Clone and enter repo
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

## 3) Stage 1 (build mapping)
From repo root:

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

Main Stage 1 outputs:
- `UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv`
- `UVH26_Project/outputs/viewpoint_registry_masked40_rerun.pkl`

## 4) Stage 2 (grouped mapping + analytics)

### 4.1 Build Spark-friendly viewpoint mapping (one-to-many)
PowerShell:
```powershell
& .\.venv\Scripts\python.exe .\spark_jobs\build_viewpoint_mapping.py --input-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-dir ..\UVH26_Project\outputs\stage2\mappings_full_26646 --num-node-buckets 2
```

Bash:
```bash
./.venv/bin/python ./spark_jobs/build_viewpoint_mapping.py --input-csv ../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv --output-dir ../UVH26_Project/outputs/stage2/mappings_full_26646 --num-node-buckets 2
```

### 4.2 Smoke test (small subset)
Create smoke mapping from full Stage 1 mapping:
PowerShell:
```powershell
& .\.venv\Scripts\python.exe .\spark_jobs\create_smoke_mapping.py --input-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --max-viewpoints 10 --max-images 100
```

Bash:
```bash
./.venv/bin/python ./spark_jobs/create_smoke_mapping.py --input-csv ../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv --output-csv ../UVH26_Project/outputs/image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --max-viewpoints 10 --max-images 100
```

Run smoke analytics:
PowerShell:
```powershell
spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp_preview.jsonl --preview-limit 20
```

Bash:
```bash
spark-submit --master spark://<master-host>:7077 ./spark_jobs/analytics_job.py --mapping-csv ../UVH26_Project/outputs/image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --output-dir ../UVH26_Project/outputs/stage2/viewpoint_analytics_smoke_10vp --preview-jsonl ../UVH26_Project/outputs/stage2/viewpoint_analytics_smoke_10vp_preview.jsonl --preview-limit 20
```

### 4.3 Full run (all viewpoints)
PowerShell:
```powershell
spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp_preview.jsonl --preview-limit 50
```

Bash:
```bash
spark-submit --master spark://<master-host>:7077 ./spark_jobs/analytics_job.py --mapping-csv ../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv --output-dir ../UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp --preview-jsonl ../UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp_preview.jsonl --preview-limit 50
```

For local-only validation (no cluster), use `--master local[*]`.

## 5) What proves distributed execution
Capture and share:
- Spark submit command with non-local master (`spark://...`).
- `spark_jobs/logs/stage2_analytics_*.log` containing:
  - `Spark master`
  - `Default parallelism`
  - `Shuffle partitions`
  - `Input mapping rows`, `Joined annotation rows`, `Output viewpoint rows`
- one preview file (`*_preview.jsonl`) for readable metric samples.

In this repository, Stage 2 logs are intentionally committed for reproducibility and auditability of distributed runs.

## 5.2 Committed full analytics artifact
This repository includes the main full Stage 2 analytics output:
- `UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp.jsonl`

Artifact details:
- Generated from full Stage 1 mapping (`26,646` images).
- Contains `4,780` viewpoint rows.
- One JSON object per viewpoint with metrics:
  - `viewpoint_id`
  - `total_images`
  - `total_vehicles`
  - `avg_vehicle_count`
  - `per_vehicle_count`
  - `avg_bbox_density`
  - `heavy_vehicle_ratio`
  - `two_wheeler_ratio`
  - `entropy`
  - `congestion_index`
  - `class_distribution_vector`

## 5.1 Detailed Stage 2 metrics
- `total_images`: frames in viewpoint.
- `total_vehicles`: total detections in viewpoint.
- `avg_vehicle_count`: mean vehicles per image.
- `per_vehicle_count`: `total_vehicles / total_images`.
- `avg_bbox_density`: mean `(sum_bbox_area / image_area)` per image.
- `heavy_vehicle_ratio`: heavy vehicle share.
- `two_wheeler_ratio`: two-wheeler share.
- `entropy`: Shannon entropy over class distribution.
- `congestion_index`: `avg_vehicle_count * avg_bbox_density`.
- `class_distribution_vector`: JSON class probability vector.

## 6) Repository policy
- Raw UVH-26 data is not committed.
- Runtime logs and generated Stage 2 outputs are excluded by `.gitignore`.

## Documentation
- Stage 1 dev docs: `STAGE1_DEV_DOCS.md`
- Stage 1 setup: `docs/STAGE1_SETUP.md`
- Stage 2 dev docs: `STAGE2_DEV_DOCS.md`
- Stage 2 setup: `docs/STAGE2_SETUP.md`

## Dataset Acknowledgment
This project uses the **UVH-26** dataset released by **AIM @ IISc**.

I sincerely thank the UVH-26 authors and contributors for making this dataset publicly available, which enables reproducible research and development for Indian traffic-scene analytics.

## Citation
If you use UVH-26 in your work, please cite:

```bibtex
@techreport{sharma2025uvh26,
   title        = {Towards Image Annotations and Accurate Vision Models for Indian Traffic, Preliminary Dataset Release, UVH-26-v1.0},
   author       = {Akash Sharma and Chinmay Mhatre and Sankalp Gawali and Ruthvik Bokkasam and Brij Kishore and Vishwajeet Pattanaik and Tarun Rambha and Abdul R. Pinjari and Vijay Kovvali and Anirban Chakraborty and Punit Rathore and Raghu Krishnapuram and Yogesh Simmhan},
   institution  = {Indian Institute of Science},
   type         = {Technical Report},
   number       = {arXiv:2511.02563},
   year         = {2025},
   month        = {November},
   doi          = {10.48550/arXiv.2511.02563}
}
```
