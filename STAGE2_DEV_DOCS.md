# Stage 2 Developer Documentation

## Scope
Stage 2 implements distributed viewpoint analytics over UVH-26 using Apache Spark DataFrame APIs.

Primary goals:
- process Stage 1 mapping + COCO annotations at scale,
- compute viewpoint-level traffic/congestion metrics,
- support smoke testing before full 4k+ viewpoint expansion,
- keep execution and verification reproducible from a fresh clone.

## Motivation
- Traffic-scene analytics require both scale and reproducibility.
- Direct per-image workflows become costly as annotations grow.
- Grouping analytics by reconstructed viewpoints gives stable, camera-level signals for congestion and composition.

## Cause / Why Stage 2 exists
Stage 1 produces an `image_id -> viewpoint_id` mapping. That is useful for association, but not ideal for distributed scheduling and grouped analytics. Stage 2 transforms that mapping and COCO annotations into viewpoint-level aggregates that can be processed efficiently with Spark's distributed `groupBy` and shuffle model.

## Core Files
- `spark_jobs/analytics_job.py` — distributed analytics pipeline.
- `spark_jobs/build_viewpoint_mapping.py` — converts one-to-one mapping into one-to-many grouped viewpoint index.
- `requirements-stage2.txt` — Stage 2 runtime dependencies.

## Input Contracts
- Stage 1 mapping CSV: `image_id, viewpoint_id`
- COCO JSON: `images`, `annotations`, `categories`

Generated grouped mapping files:
- `viewpoint_to_images.jsonl`
- `viewpoint_to_images.csv`
- `viewpoint_bucket_assignment.csv`
- `bucket_summary.csv`

## Processing Model
1. Read train + val COCO JSON.
2. Normalize and explode image/annotation arrays.
3. Join annotations with Stage 1 mapping by `image_id`.
4. Compute per-image metrics.
5. Compute per-viewpoint metrics via distributed `groupBy`.
6. Compute entropy and congestion metrics.
7. Write Parquet output (or preview-only for smoke validation).

## Detailed explanation of data flow
- **Normalization**: COCO arrays are exploded into row-level records.
- **Join strategy**: mapping table is joined on `image_id`; broadcast optimization is used for smaller mapping sizes.
- **Per-image stage**: computes density and count features for each `(viewpoint_id, image_id)`.
- **Per-viewpoint stage**: applies wide transformations and shuffle to aggregate camera-level statistics.
- **Scoring stage**: computes entropy and congestion index to support ranking and downstream dashboards.

## Output Metrics
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

## Metric definitions (detailed)
- `total_images`: number of images mapped to viewpoint.
- `total_vehicles`: sum of vehicle detections across viewpoint images.
- `avg_vehicle_count`: average detections per image.
- `per_vehicle_count`: explicit `total_vehicles / total_images` (same unit as avg count, kept for reporting clarity).
- `avg_bbox_density`: average of `total_bbox_area / image_area` per image.
- `heavy_vehicle_ratio`: heavy-vehicle detections divided by `total_vehicles`.
- `two_wheeler_ratio`: two-wheeler detections divided by `total_vehicles`.
- `entropy`: Shannon entropy on class probabilities.
- `congestion_index`: `avg_vehicle_count * avg_bbox_density`.
- `class_distribution_vector`: serialized JSON probabilities by class.

## Smoke to Full Execution

Build grouped mapping:
```powershell
& .\.venv\Scripts\python.exe .\spark_jobs\build_viewpoint_mapping.py --input-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-dir ..\UVH26_Project\outputs\stage2\mappings_full_26646 --num-node-buckets 2
```

Smoke analytics:
```powershell
& .\.venv\Scripts\python.exe .\spark_jobs\create_smoke_mapping.py --input-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --max-viewpoints 10 --max-images 100

spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_stage2_smoke_100img_10vp.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_smoke_10vp_preview.jsonl --preview-limit 20
```

Full analytics:
```powershell
spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp_preview.jsonl --preview-limit 50
```

Optional validation mode:
- `--skip-output-write` (compute + log + preview only).

## Distributed Run Evidence
For distributed claims, keep and share:
- `spark_jobs/logs/stage2_analytics_*.log`
- log lines for:
  - `Spark master: spark://...`
  - `Default parallelism`
  - `Shuffle partitions`
  - `Input mapping rows`, `Joined annotation rows`, `Output viewpoint rows`
- one preview file (`*_preview.jsonl`) for readable metrics.

Log policy in this repository:
- Stage 2 run logs are kept as tracked artifacts to support verification of distributed execution claims.

## Committed Full Output Artifact
Main full-output artifact committed in repository:
- `UVH26_Project/outputs/stage2/viewpoint_analytics_full_4780vp.jsonl`

Content summary:
- Built from full mapping coverage (`26,646` images).
- Contains `4,780` viewpoint-level rows.
- Stores full metric schema from `analytics_job.py`.

Reproducible generation command:
```powershell
spark-submit --master spark://<master-host>:7077 .\spark_jobs\analytics_job.py --mapping-csv ..\UVH26_Project\outputs\image_viewpoint_mapping_masked40_rerun.csv --output-dir ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp --skip-output-write --preview-jsonl ..\UVH26_Project\outputs\stage2\viewpoint_analytics_full_4780vp.jsonl --preview-limit 10000
```

For step-by-step environment setup, see `docs/STAGE2_SETUP.md`.
