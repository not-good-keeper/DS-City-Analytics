from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Spark-friendly viewpoint to image-id mapping (one-to-many)."
    )
    parser.add_argument(
        "--input-csv",
        default="../UVH26_Project/outputs/image_viewpoint_mapping_stage2_smoke_100img_10vp.csv",
        help="Input mapping CSV with columns image_id, viewpoint_id.",
    )
    parser.add_argument(
        "--output-dir",
        default="../UVH26_Project/outputs/stage2/mappings_smoke_10vp",
        help="Directory to write grouped mapping outputs.",
    )
    parser.add_argument(
        "--num-node-buckets",
        type=int,
        default=2,
        help="Number of node buckets for balanced viewpoint assignment.",
    )
    return parser.parse_args()


def _resolve_from_script(path_text: str) -> Path:
    script_dir = Path(__file__).resolve().parent
    raw_path = Path(path_text)
    if raw_path.is_absolute():
        return raw_path
    return (script_dir / raw_path).resolve()


def _load_mapping_rows(input_csv: Path) -> list[dict]:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input mapping CSV not found: {input_csv}")

    rows: list[dict] = []
    with input_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        expected_cols = {"image_id", "viewpoint_id"}
        if reader.fieldnames is None or not expected_cols.issubset(set(reader.fieldnames)):
            raise ValueError("Input CSV must contain columns: image_id, viewpoint_id")
        for row in reader:
            image_id = str(row["image_id"]).strip()
            viewpoint_id = int(str(row["viewpoint_id"]).strip())
            rows.append({"image_id": image_id, "viewpoint_id": viewpoint_id})
    return rows


def _group_by_viewpoint(rows: list[dict]) -> dict[int, list[str]]:
    grouped: dict[int, list[str]] = {}
    for row in rows:
        viewpoint_id = int(row["viewpoint_id"])
        image_id = str(row["image_id"])
        if viewpoint_id not in grouped:
            grouped[viewpoint_id] = []
        grouped[viewpoint_id].append(image_id)

    for viewpoint_id in grouped:
        grouped[viewpoint_id] = sorted(grouped[viewpoint_id], key=lambda x: int(x) if x.isdigit() else x)

    return dict(sorted(grouped.items(), key=lambda kv: kv[0]))


def _write_grouped_jsonl(grouped: dict[int, list[str]], output_jsonl: Path) -> None:
    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with output_jsonl.open("w", encoding="utf-8") as handle:
        for viewpoint_id, image_ids in grouped.items():
            record = {
                "viewpoint_id": int(viewpoint_id),
                "image_count": int(len(image_ids)),
                "image_ids": image_ids,
            }
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def _write_grouped_csv(grouped: dict[int, list[str]], output_csv: Path) -> None:
    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["viewpoint_id", "image_count", "image_ids_json"])
        writer.writeheader()
        for viewpoint_id, image_ids in grouped.items():
            writer.writerow(
                {
                    "viewpoint_id": int(viewpoint_id),
                    "image_count": int(len(image_ids)),
                    "image_ids_json": json.dumps(image_ids, ensure_ascii=False),
                }
            )


def _make_bucket_assignments(grouped: dict[int, list[str]], num_buckets: int) -> list[dict]:
    if num_buckets <= 0:
        raise ValueError("num_buckets must be > 0")

    viewpoints_by_size = sorted(grouped.items(), key=lambda kv: len(kv[1]), reverse=True)
    buckets = [{"bucket_id": bucket_id, "image_count": 0, "viewpoints": []} for bucket_id in range(num_buckets)]

    for viewpoint_id, image_ids in viewpoints_by_size:
        target_bucket = min(buckets, key=lambda b: b["image_count"])
        target_bucket["viewpoints"].append(int(viewpoint_id))
        target_bucket["image_count"] += len(image_ids)

    assignments: list[dict] = []
    for bucket in buckets:
        bucket_id = int(bucket["bucket_id"])
        for viewpoint_id in sorted(bucket["viewpoints"]):
            assignments.append(
                {
                    "viewpoint_id": int(viewpoint_id),
                    "bucket_id": bucket_id,
                    "image_count": int(len(grouped[viewpoint_id])),
                }
            )

    return sorted(assignments, key=lambda row: (row["bucket_id"], row["viewpoint_id"]))


def _write_bucket_files(assignments: list[dict], output_dir: Path, num_buckets: int) -> None:
    assignment_csv = output_dir / "viewpoint_bucket_assignment.csv"
    with assignment_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["viewpoint_id", "bucket_id", "image_count"])
        writer.writeheader()
        writer.writerows(assignments)

    summary_rows = []
    for bucket_id in range(num_buckets):
        rows = [row for row in assignments if row["bucket_id"] == bucket_id]
        summary_rows.append(
            {
                "bucket_id": bucket_id,
                "viewpoint_count": len(rows),
                "image_count": sum(int(row["image_count"]) for row in rows),
                "viewpoint_ids_json": json.dumps([int(row["viewpoint_id"]) for row in rows]),
            }
        )

    summary_csv = output_dir / "bucket_summary.csv"
    with summary_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["bucket_id", "viewpoint_count", "image_count", "viewpoint_ids_json"],
        )
        writer.writeheader()
        writer.writerows(summary_rows)


def main() -> None:
    args = _parse_args()

    input_csv = _resolve_from_script(args.input_csv)
    output_dir = _resolve_from_script(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows = _load_mapping_rows(input_csv)
    grouped = _group_by_viewpoint(rows)

    grouped_jsonl = output_dir / "viewpoint_to_images.jsonl"
    grouped_csv = output_dir / "viewpoint_to_images.csv"

    _write_grouped_jsonl(grouped, grouped_jsonl)
    _write_grouped_csv(grouped, grouped_csv)

    assignments = _make_bucket_assignments(grouped, args.num_node_buckets)
    _write_bucket_files(assignments, output_dir, args.num_node_buckets)

    print(f"Input CSV: {input_csv}")
    print(f"Output directory: {output_dir}")
    print(f"Total rows: {len(rows)}")
    print(f"Total viewpoints: {len(grouped)}")
    print(f"Grouped JSONL: {grouped_jsonl}")
    print(f"Grouped CSV: {grouped_csv}")
    print(f"Node buckets: {args.num_node_buckets}")


if __name__ == "__main__":
    main()
