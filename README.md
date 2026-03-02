# DS-City Analytics (Stage 1)

Stage 1 focuses on deterministic viewpoint reconstruction from UVH-26 images using online embedding-based assignment.

## Stage 1 Overview
- Extract 2048-D embeddings from ResNet50.
- Suppress vehicle interior influence via bbox masking (`MASK_PADDING=-40`).
- Assign each image to a viewpoint using online cosine-similarity centroid matching.
- Persist checkpoint files for interruption-safe resume.
- Generate near-identical viewpoint candidates for manual merge review.

## Active Stage 1 Outputs
- `UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv`
- `UVH26_Project/outputs/viewpoint_registry_masked40_rerun.pkl`
- `UVH26_Project/outputs/viewpoint_similarity_review_masked40_identical.csv`
- `UVH26_Project/outputs/viewpoint_similarity_review_masked40_identical_decisions.csv`

## Quick Start
From a fresh clone, you only need environment setup + `main.py`.
The pipeline automatically downloads required UVH-26 metadata and images during processing.

1. Activate virtual environment:
   ```powershell
   & .\.venv\Scripts\Activate.ps1
   ```
2. Install dependencies (first time only):
   ```powershell
   pip install -r requirements.txt
   ```
3. Run Stage 1 processing:
   ```powershell
   Push-Location .\UVH26_Project
   & ..\.venv\Scripts\python.exe src\main.py
   Pop-Location
   ```
4. Generate near-identical candidate pairs:
   ```powershell
   Push-Location .\UVH26_Project
   & ..\.venv\Scripts\python.exe src\identical_viewpoints.py
   Pop-Location
   ```
5. Review candidates interactively (`m/x/q`):
   ```powershell
   Push-Location .\UVH26_Project
   & ..\.venv\Scripts\python.exe src\review_viewpoint_pairs.py
   Pop-Location
   ```

## Git/Data Policy
- Raw dataset files are not committed to Git.
- `UVH26_Project/data/raw/` is intentionally ignored in `.gitignore` to avoid pushing large public dataset files.
- Outputs and code are tracked; raw data is re-downloadable by the pipeline.

## Documentation
- Developer details: `STAGE1_DEV_DOCS.md`
- Setup details: `docs/STAGE1_SETUP.md`

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
