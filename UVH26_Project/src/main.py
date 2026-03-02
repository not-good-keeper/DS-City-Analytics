from pathlib import Path

import numpy as np
import torch

from clustering import (
    assign_embeddings_online,
    load_mapping,
    load_registry,
    save_mapping,
    save_registry,
    validate_resume_state,
)
from dataset_loader import download_image_batch, load_ordered_image_index
from embedding_extractor import build_resnet50_embedder, extract_embeddings


SEED = 42
PROCESS_BATCH_SIZE = 128
EMBEDDING_BATCH_SIZE = 32
RUN_ID = "masked40_rerun"


def run_pipeline() -> None:
    np.random.seed(SEED)
    torch.manual_seed(SEED)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(SEED)

    project_root = Path(__file__).resolve().parents[1]
    data_raw_dir = project_root / "data" / "raw"
    outputs_dir = project_root / "outputs"

    data_raw_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    registry_path = outputs_dir / f"viewpoint_registry_{RUN_ID}.pkl"
    mapping_path = outputs_dir / f"image_viewpoint_mapping_{RUN_ID}.csv"

    ordered_entries, path_lookups = load_ordered_image_index(data_raw_dir)
    dataset_size = len(ordered_entries)

    mapping_rows = load_mapping(mapping_path)
    registry, saved_last_index = load_registry(registry_path)
    start_index = validate_resume_state(
        ordered_entries=ordered_entries,
        mapping_rows=mapping_rows,
        registry=registry,
        last_index=saved_last_index,
    )

    model, device = build_resnet50_embedder()

    while start_index < dataset_size:
        end_index = min(start_index + PROCESS_BATCH_SIZE, dataset_size)
        batch_entries = ordered_entries[start_index:end_index]

        records = download_image_batch(
            raw_data_dir=data_raw_dir,
            batch_entries=batch_entries,
            path_lookups=path_lookups,
        )

        embeddings = extract_embeddings(
            records=records,
            model=model,
            device=device,
            batch_size=EMBEDDING_BATCH_SIZE,
        )

        image_ids = [str(record["image_id"]) for record in records]
        assigned_viewpoints = assign_embeddings_online(
            embeddings=embeddings,
            image_ids=image_ids,
            registry=registry,
        )

        for image_id, viewpoint_id in zip(image_ids, assigned_viewpoints):
            mapping_rows.append(
                {
                    "image_id": str(image_id),
                    "viewpoint_id": int(viewpoint_id),
                }
            )

        save_registry(registry_path, registry, last_index=end_index)
        save_mapping(mapping_path, mapping_rows)

        start_index = end_index

    total_images_processed = len(mapping_rows)
    if total_images_processed != dataset_size:
        raise ValueError("total_images_processed does not match dataset size.")

    if len(mapping_rows) != dataset_size:
        raise ValueError("mapping_registry length does not match dataset size.")

    registry_total = sum(int(payload["count"]) for payload in registry.values())
    if registry_total != dataset_size:
        raise ValueError("sum(viewpoint_registry[count]) does not match dataset size.")

    total_viewpoints = len(registry)
    if total_viewpoints <= 0:
        raise ValueError("total_viewpoints must be > 0.")


if __name__ == "__main__":
    run_pipeline()
