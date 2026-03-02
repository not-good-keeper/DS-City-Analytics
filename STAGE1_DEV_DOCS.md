# Stage 1 Developer Documentation

## Scope
This document describes Stage 1 of the UVH-26 viewpoint reconstruction pipeline in this repository.

Stage 1 objective:
- Build deterministic `image_id -> viewpoint_id` mapping using online embedding assignment.
- Support interruption-safe resume via checkpoints.
- Support near-identical viewpoint review for merge decisions.

---

## Repository Layout

### Root
- `.git`
- `.gitignore`
- `.venv`
- `requirements.txt`
- `README.md`
- `STAGE1_DEV_DOCS.md`
- `docs/`
- `UVH26_Project/`

### Git / data policy
- Raw dataset content under `UVH26_Project/data/raw/` is excluded from Git (`.gitignore`).
- This repository is designed so a fresh clone does not need bundled dataset files.
- Running `src/main.py` downloads required UVH-26 JSON/image files automatically during processing.

### Project (`UVH26_Project`)
- `src/main.py` — full Stage 1 runner.
- `src/dataset_loader.py` — ordered dataset index + per-batch image retrieval + bbox loading.
- `src/embedding_extractor.py` — ResNet50 embedding extraction + bbox masking.
- `src/clustering.py` — online assignment + checkpoint persistence/resume validation.
- `src/identical_viewpoints.py` — near-identical viewpoint pair generation.
- `src/review_viewpoint_pairs.py` — interactive review UI (`m/x/q`).
- `data/raw/UVH-26/` — train/val images + COCO JSONs.
- `outputs/` — run outputs and review artifacts.

---

## Stage 1 Processing Model

## Deterministic order
- Process all train images first, then val images.
- Keep original JSON image order.
- No shuffling, no reordering.

## Batching
- `PROCESS_BATCH_SIZE = 128`
- `EMBEDDING_BATCH_SIZE = 32`

## Embeddings
- Model: `resnet50(IMAGENET1K_V1)`
- Final FC removed (`Identity`) to produce 2048-D vectors.
- L2-normalize embeddings before similarity comparison.

## Bbox masking
- Uses COCO annotation bboxes.
- Applies shrink padding (`MASK_PADDING = -40`) before masking.
- Intention: reduce vehicle-content influence and keep static scene cues.

## Online assignment
For each embedding:
1. Cosine similarity with all viewpoint centroids.
2. If best similarity `>= 0.90`: assign and update centroid (running mean + normalize).
3. Else: create new viewpoint id.

---

## Checkpoint Files

For current run id (`masked40_rerun`):
- `UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv`
- `UVH26_Project/outputs/viewpoint_registry_masked40_rerun.pkl`

Registry structure:
- `last_index`
- `viewpoints` dictionary with:
  - `centroid` (2048-D)
  - `count`

Resume checks:
- `last_index == mapping length`
- mapping order matches expected dataset order
- sum of viewpoint counts equals mapping length

Any mismatch raises `ValueError`.

---

## Runbook

## 1) Activate environment (PowerShell)
```powershell
& .\.venv\Scripts\Activate.ps1
```

## 2) Run full Stage 1 pipeline
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\main.py
Pop-Location
```

Fresh clone behavior:
- No manual raw dataset copy is required.
- The loader will fetch required files into `UVH26_Project/data/raw/UVH-26/`.

## 3) Check progress
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe -c "import pickle; from pathlib import Path; s=pickle.load(Path('outputs/viewpoint_registry_masked40_rerun.pkl').open('rb')); print('last_index', s.get('last_index')); print('viewpoints', len(s.get('viewpoints', {})))"
Pop-Location
```

---

## Near-identical Viewpoint Review

Generate candidates:
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\identical_viewpoints.py
Pop-Location
```

Review interactively:
```powershell
Push-Location .\UVH26_Project
& ..\.venv\Scripts\python.exe src\review_viewpoint_pairs.py
Pop-Location
```

Controls:
- `m` = merge
- `x` = no merge
- `q` = quit/save

---

## Notes
- Pipeline is deterministic with seed 42.
- Do not manually edit checkpoint files unless performing controlled migration.
- Resume is automatic when rerunning `main.py`.
