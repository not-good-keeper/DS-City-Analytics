import csv
import pickle
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np


THRESHOLD = 0.90


def _normalize(vector: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(vector))
    if norm <= 0:
        raise ValueError("Vector norm must be > 0 for normalization.")
    return vector / norm


def load_mapping(mapping_path: Path) -> List[Dict]:
    if not mapping_path.exists():
        return []

    rows: List[Dict] = []
    with mapping_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        expected = ["image_id", "viewpoint_id"]
        if reader.fieldnames != expected:
            raise ValueError("mapping CSV schema mismatch.")

        for row in reader:
            rows.append(
                {
                    "image_id": str(row["image_id"]),
                    "viewpoint_id": int(row["viewpoint_id"]),
                }
            )

    return rows


def save_mapping(mapping_path: Path, mapping_rows: List[Dict]) -> None:
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    with mapping_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["image_id", "viewpoint_id"])
        writer.writeheader()
        for row in mapping_rows:
            writer.writerow(row)


def load_registry(registry_path: Path) -> Tuple[Dict[int, Dict], int]:
    if not registry_path.exists():
        return {}, 0

    with registry_path.open("rb") as handle:
        state = pickle.load(handle)

    if not isinstance(state, dict):
        raise ValueError("Registry is corrupted: expected dict state.")
    if "viewpoints" not in state or "last_index" not in state:
        raise ValueError("Registry is corrupted: missing viewpoints/last_index.")

    registry = state["viewpoints"]
    last_index = int(state["last_index"])

    if not isinstance(registry, dict):
        raise ValueError("Registry is corrupted: viewpoints must be dict.")
    if last_index < 0:
        raise ValueError("Registry is corrupted: last_index must be >= 0.")

    for viewpoint_id, payload in registry.items():
        if not isinstance(viewpoint_id, int):
            raise ValueError("Registry is corrupted: viewpoint_id must be int.")
        if "centroid" not in payload or "count" not in payload:
            raise ValueError("Registry is corrupted: missing keys.")

        centroid = np.array(payload["centroid"], dtype=np.float32)
        if centroid.shape != (2048,):
            raise ValueError("Registry centroid must be 2048-dimensional.")
        payload["centroid"] = _normalize(centroid)

        count = int(payload["count"])
        if count <= 0:
            raise ValueError("Registry count must be > 0.")
        payload["count"] = count

    return registry, last_index


def save_registry(registry_path: Path, registry: Dict[int, Dict], last_index: int) -> None:
    if last_index < 0:
        raise ValueError("last_index must be >= 0.")

    state = {
        "last_index": int(last_index),
        "viewpoints": registry,
    }

    registry_path.parent.mkdir(parents=True, exist_ok=True)
    with registry_path.open("wb") as handle:
        pickle.dump(state, handle)


def validate_resume_state(
    ordered_entries: List[Dict],
    mapping_rows: List[Dict],
    registry: Dict[int, Dict],
    last_index: int,
) -> int:
    if len(mapping_rows) > len(ordered_entries):
        raise ValueError("Mapping has more rows than dataset size.")

    if last_index != len(mapping_rows):
        raise ValueError("Saved last_index does not match mapping length.")

    for index, row in enumerate(mapping_rows):
        expected_image_id = str(ordered_entries[index]["image_id"])
        if str(row["image_id"]) != expected_image_id:
            raise ValueError("Mapping order mismatch: cannot resume deterministically.")

    registry_total = sum(int(payload["count"]) for payload in registry.values())
    if registry_total != len(mapping_rows):
        raise ValueError("Registry count sum does not match mapping length.")

    return last_index


def assign_embeddings_online(
    embeddings: np.ndarray,
    image_ids: List[str],
    registry: Dict[int, Dict],
) -> List[int]:
    if embeddings.ndim != 2 or embeddings.shape[1] != 2048:
        raise ValueError("Embeddings must have shape (N, 2048).")
    if embeddings.shape[0] != len(image_ids):
        raise ValueError("Embedding/image_id size mismatch.")

    assigned: List[int] = []

    for row_index in range(embeddings.shape[0]):
        embedding = embeddings[row_index].astype(np.float32)
        embedding = _normalize(embedding)

        if len(registry) == 0:
            new_viewpoint_id = 0
            registry[new_viewpoint_id] = {
                "centroid": embedding,
                "count": 1,
            }
            assigned.append(new_viewpoint_id)
            continue

        viewpoint_ids = sorted(registry.keys())
        centroid_matrix = np.stack([registry[vp_id]["centroid"] for vp_id in viewpoint_ids], axis=0)

        similarities = centroid_matrix @ embedding
        best_index = int(np.argmax(similarities))
        best_similarity = float(similarities[best_index])
        best_viewpoint_id = int(viewpoint_ids[best_index])

        if best_similarity >= THRESHOLD:
            payload = registry[best_viewpoint_id]
            old_centroid = payload["centroid"]
            old_count = int(payload["count"])

            updated = (old_centroid * old_count + embedding) / float(old_count + 1)
            payload["centroid"] = _normalize(updated.astype(np.float32))
            payload["count"] = old_count + 1
            assigned.append(best_viewpoint_id)
        else:
            new_viewpoint_id = int(max(viewpoint_ids) + 1)
            registry[new_viewpoint_id] = {
                "centroid": embedding,
                "count": 1,
            }
            assigned.append(new_viewpoint_id)

    if len(assigned) != len(image_ids):
        raise ValueError("Failed to assign viewpoint IDs for all embeddings.")

    return assigned
