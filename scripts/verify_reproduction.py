"""Check that the dataset and the committed outputs line up, and compare fresh runs to them.

    python scripts/verify_reproduction.py                      # dataset + committed artifacts
    python scripts/verify_reproduction.py --stage1 NEW.csv     # compare a rebuilt Stage 1 mapping
    python scripts/verify_reproduction.py --stage2 NEW.jsonl   # compare a rebuilt Stage 2 JSONL

Exits non-zero if any check fails.
"""
import argparse
import csv
import json
import math
import pickle
import sys
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
PROJECT = REPO / "UVH26_Project"
sys.path.insert(0, str(PROJECT / "src"))

from dataset_loader import ANNOTATION_SHA256, sha256_of  # noqa: E402

DATASET_DIR = PROJECT / "data" / "raw" / "UVH-26"
MAPPING_CSV = PROJECT / "outputs" / "image_viewpoint_mapping_masked40_rerun.csv"
REGISTRY_PKL = PROJECT / "outputs" / "viewpoint_registry_masked40_rerun.pkl"
ANALYTICS_JSONL = PROJECT / "outputs" / "stage2" / "viewpoint_analytics_full_4780vp.jsonl"

EXPECTED_IMAGES = 26646
EXPECTED_VIEWPOINTS = 4780
FLOAT_FIELDS = (
    "avg_vehicle_count",
    "per_vehicle_count",
    "avg_bbox_density",
    "heavy_vehicle_ratio",
    "two_wheeler_ratio",
    "entropy",
    "congestion_index",
)

failures = []


def check(ok: bool, message: str) -> None:
    print(("PASS  " if ok else "FAIL  ") + message)
    if not ok:
        failures.append(message)


def read_mapping(path: Path) -> dict:
    with path.open(newline="", encoding="utf-8") as handle:
        return {row["image_id"]: int(row["viewpoint_id"]) for row in csv.DictReader(handle)}


def read_analytics(path: Path) -> dict:
    rows = {}
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                row = json.loads(line)
                rows[int(row["viewpoint_id"])] = row
    return rows


def check_dataset() -> None:
    print("\n== Dataset (UVH-26 annotations)")
    for rel_path, expected in ANNOTATION_SHA256.items():
        path = DATASET_DIR / rel_path
        if not path.exists():
            print(f"SKIP  {rel_path} not downloaded (run UVH26_Project/src/fetch_dataset.py)")
            continue
        check(sha256_of(path) == expected, f"{rel_path} sha256")

    image_dirs = [DATASET_DIR / split / "data" for split in ("UVH-26-Train", "UVH-26-Val")]
    if all(d.exists() for d in image_dirs):
        found = sum(1 for d in image_dirs for _ in d.rglob("*.png"))
        check(found == EXPECTED_IMAGES, f"{found} images on disk (expected {EXPECTED_IMAGES})")


def check_committed() -> None:
    print("\n== Committed artifacts")
    mapping = read_mapping(MAPPING_CSV)
    check(len(mapping) == EXPECTED_IMAGES, f"Stage 1 mapping has {len(mapping)} unique images")
    per_vp = Counter(mapping.values())
    check(len(per_vp) == EXPECTED_VIEWPOINTS, f"Stage 1 mapping has {len(per_vp)} viewpoints")

    try:
        with REGISTRY_PKL.open("rb") as handle:
            state = pickle.load(handle)
        registry_counts = {vp: int(p["count"]) for vp, p in state["viewpoints"].items()}
        check(state["last_index"] == EXPECTED_IMAGES, f"registry last_index = {state['last_index']}")
        check(registry_counts == dict(per_vp), "registry counts match mapping per viewpoint")
    except ModuleNotFoundError as exc:
        print(f"SKIP  registry check needs {exc.name} (install requirements.txt)")

    analytics = read_analytics(ANALYTICS_JSONL)
    check(len(analytics) == EXPECTED_VIEWPOINTS, f"Stage 2 JSONL has {len(analytics)} viewpoints")
    mismatched = [vp for vp, row in analytics.items() if row["total_images"] != per_vp.get(vp)]
    check(not mismatched, "Stage 2 total_images per viewpoint matches the Stage 1 mapping")


def adjusted_rand_index(labels_a: list, labels_b: list) -> float:
    def pairs(n: int) -> int:
        return n * (n - 1) // 2

    contingency = Counter(zip(labels_a, labels_b))
    sum_ij = sum(pairs(n) for n in contingency.values())
    sum_a = sum(pairs(n) for n in Counter(labels_a).values())
    sum_b = sum(pairs(n) for n in Counter(labels_b).values())
    expected = sum_a * sum_b / pairs(len(labels_a))
    max_index = (sum_a + sum_b) / 2
    return 1.0 if max_index == expected else (sum_ij - expected) / (max_index - expected)


def compare_stage1(new_csv: Path) -> None:
    print(f"\n== Stage 1: {new_csv} vs committed")
    ref = read_mapping(MAPPING_CSV)
    new = read_mapping(new_csv)
    check(set(new) == set(ref), f"same image set ({len(new)} vs {len(ref)})")
    common = sorted(set(new) & set(ref))
    same = sum(1 for image_id in common if new[image_id] == ref[image_id])
    check(same == len(ref), f"identical viewpoint_id for {same}/{len(ref)} images")
    ari = adjusted_rand_index([ref[i] for i in common], [new[i] for i in common])
    print(f"INFO  viewpoints: new={len(set(new.values()))} committed={len(set(ref.values()))}, ARI={ari:.6f}")


def compare_stage2(new_jsonl: Path, rel_tol: float) -> None:
    print(f"\n== Stage 2: {new_jsonl} vs committed")
    ref = read_analytics(ANALYTICS_JSONL)
    new = read_analytics(new_jsonl)
    check(set(new) == set(ref), f"same viewpoint ids ({len(new)} vs {len(ref)})")

    bad = []
    for vp in sorted(set(new) & set(ref)):
        a, b = ref[vp], new[vp]
        if a["total_images"] != b["total_images"] or a["total_vehicles"] != b["total_vehicles"]:
            bad.append((vp, "counts"))
            continue
        for field in FLOAT_FIELDS:
            if not math.isclose(a[field], b[field], rel_tol=rel_tol, abs_tol=1e-12):
                bad.append((vp, field))
                break
        else:
            dist_a = json.loads(a["class_distribution_vector"])
            dist_b = json.loads(b["class_distribution_vector"])
            if dist_a.keys() != dist_b.keys() or any(abs(dist_a[k] - dist_b[k]) > 1e-6 for k in dist_a):
                bad.append((vp, "class_distribution_vector"))
    check(not bad, f"all metrics match within rel_tol={rel_tol} ({len(bad)} mismatched viewpoints)")
    for vp, field in bad[:10]:
        print(f"      viewpoint {vp}: {field}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--stage1", type=Path, help="Rebuilt Stage 1 mapping CSV to compare.")
    parser.add_argument("--stage2", type=Path, help="Rebuilt Stage 2 analytics JSONL to compare.")
    parser.add_argument("--rel-tol", type=float, default=1e-9, help="Float tolerance for Stage 2 metrics.")
    args = parser.parse_args()

    check_dataset()
    check_committed()
    if args.stage1:
        compare_stage1(args.stage1)
    if args.stage2:
        compare_stage2(args.stage2, args.rel_tol)

    print(f"\n{'OK' if not failures else f'{len(failures)} check(s) failed'}")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
