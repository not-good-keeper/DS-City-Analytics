from pathlib import Path
import csv
import pickle
from typing import Dict, List, Tuple

import numpy as np


SIMILARITY_THRESHOLD = 0.97
MAX_CANDIDATES = 2000


def _load_registry(registry_path: Path) -> Dict[int, Dict]:
    if not registry_path.exists():
        raise ValueError("Registry file not found.")

    with registry_path.open("rb") as handle:
        state = pickle.load(handle)

    if "viewpoints" not in state:
        raise ValueError("Invalid registry format.")

    registry = state["viewpoints"]
    if not isinstance(registry, dict) or len(registry) == 0:
        raise ValueError("Registry viewpoints invalid/empty.")

    return registry


def _load_mapping(mapping_path: Path) -> List[Tuple[str, int]]:
    if not mapping_path.exists():
        raise ValueError("Mapping file not found.")

    rows: List[Tuple[str, int]] = []
    with mapping_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != ["image_id", "viewpoint_id"]:
            raise ValueError("Unexpected mapping schema.")

        for row in reader:
            rows.append((str(row["image_id"]), int(row["viewpoint_id"])))

    if len(rows) == 0:
        raise ValueError("Mapping is empty.")

    return rows


def _build_first_image_per_viewpoint(mapping_rows: List[Tuple[str, int]]) -> Dict[int, str]:
    first: Dict[int, str] = {}
    for image_id, viewpoint_id in mapping_rows:
        if viewpoint_id not in first:
            first[viewpoint_id] = image_id
    return first


def _build_image_path_map(raw_dataset_dir: Path) -> Dict[str, str]:
    train_dir = raw_dataset_dir / "UVH-26-Train" / "data"
    val_dir = raw_dataset_dir / "UVH-26-Val" / "data"

    if not train_dir.exists() or not val_dir.exists():
        raise ValueError("Raw dataset folders missing.")

    image_path_map: Dict[str, str] = {}
    for root in [train_dir, val_dir]:
        for image_path in root.rglob("*.png"):
            image_id = image_path.stem
            if image_id in image_path_map:
                raise ValueError(f"Duplicate image_id in raw data: {image_id}")
            image_path_map[image_id] = str(image_path)

    if len(image_path_map) == 0:
        raise ValueError("No images found in raw dataset.")

    return image_path_map


def _compute_candidate_pairs(
    viewpoint_ids: List[int],
    centroids: np.ndarray,
    threshold: float,
    max_candidates: int,
) -> List[Tuple[int, int, float]]:
    candidates: List[Tuple[int, int, float]] = []
    n = len(viewpoint_ids)

    for i in range(n - 1):
        row_sim = centroids[i + 1 :] @ centroids[i]
        matched = np.where(row_sim >= threshold)[0]

        for relative_idx in matched.tolist():
            j = i + 1 + int(relative_idx)
            candidates.append((viewpoint_ids[i], viewpoint_ids[j], float(row_sim[relative_idx])))

    candidates.sort(key=lambda row: row[2], reverse=True)
    if len(candidates) > max_candidates:
        candidates = candidates[:max_candidates]

    return candidates


def generate_identical_candidates() -> Path:
    project_root = Path(__file__).resolve().parents[1]
    outputs_dir = project_root / "outputs"
    raw_dataset_dir = project_root / "data" / "raw" / "UVH-26"

    registry_path = outputs_dir / "viewpoint_registry_masked40_rerun.pkl"
    mapping_path = outputs_dir / "image_viewpoint_mapping_masked40_rerun.csv"
    output_path = outputs_dir / "viewpoint_similarity_review_masked40_identical.csv"

    registry = _load_registry(registry_path)
    mapping_rows = _load_mapping(mapping_path)

    first_image = _build_first_image_per_viewpoint(mapping_rows)
    image_path_map = _build_image_path_map(raw_dataset_dir)

    viewpoint_ids = sorted(registry.keys())
    centroids = np.stack([registry[vp_id]["centroid"] for vp_id in viewpoint_ids], axis=0).astype(np.float32)

    if centroids.shape[1] != 2048:
        raise ValueError("Centroid dimension mismatch.")

    pairs = _compute_candidate_pairs(
        viewpoint_ids=viewpoint_ids,
        centroids=centroids,
        threshold=SIMILARITY_THRESHOLD,
        max_candidates=MAX_CANDIDATES,
    )

    rows: List[Dict] = []
    for vp_a, vp_b, sim in pairs:
        if vp_a not in first_image or vp_b not in first_image:
            raise ValueError("Missing representative image for viewpoint.")

        img_a = first_image[vp_a]
        img_b = first_image[vp_b]

        if img_a not in image_path_map or img_b not in image_path_map:
            raise ValueError("Representative image path missing.")

        rows.append(
            {
                "viewpoint_id_a": vp_a,
                "image_id_a": img_a,
                "image_path_a": image_path_map[img_a],
                "viewpoint_id_b": vp_b,
                "image_id_b": img_b,
                "image_path_b": image_path_map[img_b],
                "centroid_similarity": f"{sim:.6f}",
                "merge_decision": "",
                "notes": "",
            }
        )

    with output_path.open("w", newline="", encoding="utf-8") as handle:
        fieldnames = [
            "viewpoint_id_a",
            "image_id_a",
            "image_path_a",
            "viewpoint_id_b",
            "image_id_b",
            "image_path_b",
            "centroid_similarity",
            "merge_decision",
            "notes",
        ]
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return output_path


if __name__ == "__main__":
    out = generate_identical_candidates()
    print(out)
