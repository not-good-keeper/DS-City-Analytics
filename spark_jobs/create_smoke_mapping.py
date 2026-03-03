from __future__ import annotations

import argparse
import csv
from pathlib import Path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a smoke mapping CSV from Stage 1 mapping (first K viewpoints, max N images)."
    )
    parser.add_argument(
        "--input-csv",
        default="../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv",
        help="Input Stage 1 mapping CSV with image_id,viewpoint_id.",
    )
    parser.add_argument(
        "--output-csv",
        default="../UVH26_Project/outputs/image_viewpoint_mapping_stage2_smoke_100img_10vp.csv",
        help="Output smoke mapping CSV.",
    )
    parser.add_argument("--max-viewpoints", type=int, default=10)
    parser.add_argument("--max-images", type=int, default=100)
    return parser.parse_args()


def _resolve(path_text: str) -> Path:
    script_dir = Path(__file__).resolve().parent
    raw = Path(path_text)
    if raw.is_absolute():
        return raw
    return (script_dir / raw).resolve()


def main() -> None:
    args = _parse_args()

    input_csv = _resolve(args.input_csv)
    output_csv = _resolve(args.output_csv)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    if not input_csv.exists():
        raise FileNotFoundError(f"Input mapping not found: {input_csv}")

    selected_viewpoints: list[str] = []
    rows_out: list[dict[str, str]] = []

    with input_csv.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"image_id", "viewpoint_id"}
        if reader.fieldnames is None or not required.issubset(set(reader.fieldnames)):
            raise ValueError("Input CSV must include image_id and viewpoint_id columns.")

        for row in reader:
            viewpoint_id = str(row["viewpoint_id"]).strip()
            image_id = str(row["image_id"]).strip()

            if viewpoint_id not in selected_viewpoints:
                if len(selected_viewpoints) >= int(args.max_viewpoints):
                    continue
                selected_viewpoints.append(viewpoint_id)

            rows_out.append({"image_id": image_id, "viewpoint_id": viewpoint_id})

            if len(rows_out) >= int(args.max_images):
                break

    with output_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["image_id", "viewpoint_id"])
        writer.writeheader()
        writer.writerows(rows_out)

    print(f"input={input_csv}")
    print(f"output={output_csv}")
    print(f"rows={len(rows_out)}")
    print(f"viewpoints={len(selected_viewpoints)}")


if __name__ == "__main__":
    main()
