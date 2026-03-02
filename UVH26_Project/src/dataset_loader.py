from pathlib import Path
import json
from typing import Dict, List, Tuple

from huggingface_hub import HfApi, hf_hub_download


DATASET_REPO_ID = "iisc-aim/UVH-26"


def _download_required_jsons(dataset_dir: Path) -> Tuple[Path, Path]:
    train_json_path = Path(
        hf_hub_download(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            filename="UVH-26-Train/UVH-26-MV-Train.json",
            local_dir=str(dataset_dir),
        )
    )
    val_json_path = Path(
        hf_hub_download(
            repo_id=DATASET_REPO_ID,
            repo_type="dataset",
            filename="UVH-26-Val/UVH-26-MV-Val.json",
            local_dir=str(dataset_dir),
        )
    )

    if not train_json_path.exists() or not val_json_path.exists():
        raise ValueError("Failed to download required annotation JSON files.")

    return train_json_path, val_json_path


def _load_json(path: Path) -> Dict:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if "images" not in payload or "annotations" not in payload:
        raise ValueError("Invalid COCO JSON schema: missing images/annotations.")

    return payload


def _build_annotation_map(payload: Dict) -> Dict[int, List[List[float]]]:
    annotation_map: Dict[int, List[List[float]]] = {}
    for ann in payload["annotations"]:
        image_id = int(ann["image_id"])
        bbox = ann.get("bbox")
        if not isinstance(bbox, list) or len(bbox) != 4:
            raise ValueError("Invalid bbox in annotations.")
        bbox_values = [float(bbox[0]), float(bbox[1]), float(bbox[2]), float(bbox[3])]
        if image_id not in annotation_map:
            annotation_map[image_id] = []
        annotation_map[image_id].append(bbox_values)
    return annotation_map


def _build_split_path_lookup(repo_files: List[str], split_prefix: str) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    base_prefix = f"{split_prefix}/data/"
    for path in repo_files:
        if path.startswith(base_prefix):
            basename = Path(path).name
            if basename in lookup:
                raise ValueError(f"Duplicate image basename in split {split_prefix}: {basename}")
            lookup[basename] = path
    return lookup


def load_ordered_image_index(raw_data_dir: Path) -> Tuple[List[Dict], Dict[str, Dict[str, str]]]:
    dataset_dir = raw_data_dir / "UVH-26"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    train_json_path, val_json_path = _download_required_jsons(dataset_dir)
    train_json = _load_json(train_json_path)
    val_json = _load_json(val_json_path)

    train_ann_map = _build_annotation_map(train_json)
    val_ann_map = _build_annotation_map(val_json)

    ordered_entries: List[Dict] = []

    for row in train_json["images"]:
        file_name = str(row["file_name"])
        source_image_id = int(row["id"])
        ordered_entries.append(
            {
                "split": "train",
                "image_id": Path(file_name).stem,
                "file_name": file_name,
                "bboxes": train_ann_map.get(source_image_id, []),
            }
        )

    for row in val_json["images"]:
        file_name = str(row["file_name"])
        source_image_id = int(row["id"])
        ordered_entries.append(
            {
                "split": "val",
                "image_id": Path(file_name).stem,
                "file_name": file_name,
                "bboxes": val_ann_map.get(source_image_id, []),
            }
        )

    if len(ordered_entries) == 0:
        raise ValueError("No images found in dataset index.")

    api = HfApi()
    repo_files = api.list_repo_files(repo_id=DATASET_REPO_ID, repo_type="dataset")

    path_lookups = {
        "train": _build_split_path_lookup(repo_files, "UVH-26-Train"),
        "val": _build_split_path_lookup(repo_files, "UVH-26-Val"),
    }

    return ordered_entries, path_lookups


def download_image_batch(
    raw_data_dir: Path,
    batch_entries: List[Dict],
    path_lookups: Dict[str, Dict[str, str]],
) -> List[Dict]:
    dataset_dir = raw_data_dir / "UVH-26"
    dataset_dir.mkdir(parents=True, exist_ok=True)

    records: List[Dict] = []
    for entry in batch_entries:
        split = entry["split"]
        image_id = str(entry["image_id"])
        file_name = entry["file_name"]
        bboxes = entry.get("bboxes", [])
        if not isinstance(bboxes, list):
            raise ValueError("Entry bboxes must be a list.")

        if split not in path_lookups:
            raise ValueError(f"Unknown split in entry: {split}")
        if file_name not in path_lookups[split]:
            raise ValueError(f"Image file not found in repo for split {split}: {file_name}")

        rel_path = path_lookups[split][file_name]
        local_path = Path(
            hf_hub_download(
                repo_id=DATASET_REPO_ID,
                repo_type="dataset",
                filename=rel_path,
                local_dir=str(dataset_dir),
            )
        )
        if not local_path.exists():
            raise ValueError(f"Failed to download image file: {rel_path}")

        records.append(
            {
                "image_id": image_id,
                "split": split,
                "image_path": str(local_path),
                "bboxes": bboxes,
            }
        )

    if len(records) != len(batch_entries):
        raise ValueError("Downloaded record count does not match batch size.")

    return records
