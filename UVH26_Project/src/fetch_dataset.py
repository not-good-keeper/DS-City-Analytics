"""Download UVH-26 at the pinned Hugging Face revision into UVH26_Project/data/raw/UVH-26.

Annotations only (enough for Stage 2 and Stage 3):
    python UVH26_Project/src/fetch_dataset.py

Annotations plus all 26,646 images (needed to rerun Stage 1 from scratch):
    python UVH26_Project/src/fetch_dataset.py --images
"""
import argparse
from pathlib import Path

from huggingface_hub import snapshot_download

from dataset_loader import DATASET_REPO_ID, DATASET_REVISION, download_annotations

EXPECTED_IMAGES = {"UVH-26-Train": 21349, "UVH-26-Val": 5297}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--images", action="store_true", help="Also download every image (large).")
    parser.add_argument("--max-workers", type=int, default=8, help="Parallel downloads for --images.")
    args = parser.parse_args()

    dataset_dir = Path(__file__).resolve().parents[1] / "data" / "raw" / "UVH-26"
    print(f"Dataset: {DATASET_REPO_ID} @ {DATASET_REVISION}")
    print(f"Target:  {dataset_dir}")

    for rel_path, local_path in download_annotations(dataset_dir).items():
        print(f"ok  {rel_path}  (sha256 verified)")

    if not args.images:
        return

    snapshot_download(
        repo_id=DATASET_REPO_ID,
        repo_type="dataset",
        revision=DATASET_REVISION,
        local_dir=str(dataset_dir),
        allow_patterns=["UVH-26-Train/data/**", "UVH-26-Val/data/**"],
        max_workers=args.max_workers,
    )

    for split, expected in EXPECTED_IMAGES.items():
        found = sum(1 for _ in (dataset_dir / split / "data").rglob("*.png"))
        status = "ok " if found == expected else "BAD"
        print(f"{status} {split}: {found} images (expected {expected})")
        if found != expected:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
