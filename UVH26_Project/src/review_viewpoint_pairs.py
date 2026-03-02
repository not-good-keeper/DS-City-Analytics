from pathlib import Path
import csv
import tkinter as tk

import cv2
import numpy as np


PROJECT_ROOT = Path(__file__).resolve().parents[1]
INPUT_CSV = PROJECT_ROOT / "outputs" / "viewpoint_similarity_review_masked40_identical.csv"
OUTPUT_CSV = PROJECT_ROOT / "outputs" / "viewpoint_similarity_review_masked40_identical_decisions.csv"
PREVIEW_DIR = PROJECT_ROOT / "outputs" / "review_previews"
WINDOW_NAME = "Viewpoint Pair Review"


def load_rows(csv_path: Path):
    if not csv_path.exists():
        raise ValueError(f"Input review file not found: {csv_path}")

    with csv_path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        expected = [
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
        if reader.fieldnames != expected:
            raise ValueError("Unexpected input CSV schema.")

        rows = [row for row in reader]

    if len(rows) == 0:
        raise ValueError("No rows found in input review CSV.")

    return rows


def load_or_initialize_output(input_rows):
    if not OUTPUT_CSV.exists():
        return input_rows

    with OUTPUT_CSV.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        output_rows = [row for row in reader]

    if len(output_rows) != len(input_rows):
        raise ValueError("Existing output decision file has mismatched row count.")

    for idx in range(len(input_rows)):
        if output_rows[idx]["viewpoint_id_a"] != input_rows[idx]["viewpoint_id_a"]:
            raise ValueError("Decision file row mismatch on viewpoint_id_a.")
        if output_rows[idx]["viewpoint_id_b"] != input_rows[idx]["viewpoint_id_b"]:
            raise ValueError("Decision file row mismatch on viewpoint_id_b.")

    return output_rows


def save_rows(rows):
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

    with OUTPUT_CSV.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def resize_to_height(image, target_height):
    h, w = image.shape[:2]
    scale = float(target_height) / float(h)
    target_width = max(1, int(w * scale))
    return cv2.resize(image, (target_width, target_height), interpolation=cv2.INTER_AREA)


def build_canvas(row, index, total):
    image_a = cv2.imread(row["image_path_a"], cv2.IMREAD_COLOR)
    image_b = cv2.imread(row["image_path_b"], cv2.IMREAD_COLOR)

    if image_a is None:
        raise ValueError(f"Failed to load image A: {row['image_path_a']}")
    if image_b is None:
        raise ValueError(f"Failed to load image B: {row['image_path_b']}")

    target_height = 640
    image_a = resize_to_height(image_a, target_height)
    image_b = resize_to_height(image_b, target_height)

    spacer = np.full((target_height, 24, 3), 40, dtype=np.uint8)
    combined = np.concatenate([image_a, spacer, image_b], axis=1)

    top_bar_height = 120
    canvas = np.full((combined.shape[0] + top_bar_height, combined.shape[1], 3), 0, dtype=np.uint8)
    canvas[top_bar_height:, :, :] = combined

    cv2.putText(
        canvas,
        f"Pair {index + 1}/{total}  similarity={row['centroid_similarity']}",
        (10, 30),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.8,
        (0, 255, 255),
        2,
        cv2.LINE_AA,
    )
    cv2.putText(
        canvas,
        f"A: vp={row['viewpoint_id_a']} image={row['image_id_a']}",
        (10, 60),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.7,
        (200, 200, 200),
        2,
        cv2.LINE_AA,
    )
    cv2.putText(
        canvas,
        f"B: vp={row['viewpoint_id_b']} image={row['image_id_b']}",
        (10, 88),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.7,
        (200, 200, 200),
        2,
        cv2.LINE_AA,
    )

    decision = row.get("merge_decision", "")
    if decision == "merge":
        status_text = "Current decision: MERGE"
        status_color = (0, 255, 0)
    elif decision == "no_merge":
        status_text = "Current decision: NO_MERGE"
        status_color = (0, 0, 255)
    else:
        status_text = "Current decision: pending"
        status_color = (150, 150, 150)

    cv2.putText(
        canvas,
        status_text,
        (combined.shape[1] - 330, 30),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.7,
        status_color,
        2,
        cv2.LINE_AA,
    )
    cv2.putText(
        canvas,
        "Keys: m=merge, x=no_merge, q=quit",
        (combined.shape[1] - 420, 88),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.6,
        (220, 220, 220),
        2,
        cv2.LINE_AA,
    )

    return canvas


def run_review():
    input_rows = load_rows(INPUT_CSV)
    rows = load_or_initialize_output(input_rows)

    start_index = 0
    for idx, row in enumerate(rows):
        if row.get("merge_decision", "") not in ["merge", "no_merge"]:
            start_index = idx
            break
        if idx == len(rows) - 1:
            start_index = len(rows)

    if start_index == len(rows):
        save_rows(rows)
        return

    index = start_index
    PREVIEW_DIR.mkdir(parents=True, exist_ok=True)

    root = tk.Tk()
    root.title(WINDOW_NAME)
    root.protocol("WM_DELETE_WINDOW", lambda: None)

    pair_text = tk.StringVar(value="")
    detail_text = tk.StringVar(value="")

    header = tk.Label(root, textvariable=pair_text, font=("Segoe UI", 11, "bold"))
    header.pack(padx=10, pady=(10, 4), anchor="w")

    detail = tk.Label(root, textvariable=detail_text, font=("Segoe UI", 10))
    detail.pack(padx=10, pady=(0, 8), anchor="w")

    hint = tk.Label(root, text="Press only: m = merge, x = no_merge, q = quit", font=("Segoe UI", 9))
    hint.pack(padx=10, pady=(0, 8), anchor="w")

    image_label = tk.Label(root)
    image_label.pack(padx=10, pady=(0, 10))

    preview_path = PREVIEW_DIR / "current_pair.png"

    state = {
        "index": index,
    }

    def show_current_pair() -> None:
        current_index = state["index"]
        if current_index >= len(rows):
            save_rows(rows)
            root.destroy()
            return

        canvas = build_canvas(rows[current_index], current_index, len(rows))
        if not cv2.imwrite(str(preview_path), canvas):
            raise ValueError(f"Failed to write preview image: {preview_path}")

        image = tk.PhotoImage(file=str(preview_path))
        image_label.configure(image=image)
        image_label.image = image

        row = rows[current_index]
        pair_text.set(f"Pair {current_index + 1}/{len(rows)} | similarity={row['centroid_similarity']}")
        detail_text.set(
            f"A(vp={row['viewpoint_id_a']}, img={row['image_id_a']}) vs "
            f"B(vp={row['viewpoint_id_b']}, img={row['image_id_b']})"
        )

    def set_merge(_event=None) -> None:
        current_index = state["index"]
        if current_index >= len(rows):
            return
        rows[current_index]["merge_decision"] = "merge"
        save_rows(rows)
        state["index"] = current_index + 1
        show_current_pair()

    def set_no_merge(_event=None) -> None:
        current_index = state["index"]
        if current_index >= len(rows):
            return
        rows[current_index]["merge_decision"] = "no_merge"
        save_rows(rows)
        state["index"] = current_index + 1
        show_current_pair()

    def quit_review(_event=None) -> None:
        save_rows(rows)
        root.destroy()

    root.bind("m", set_merge)
    root.bind("M", set_merge)
    root.bind("x", set_no_merge)
    root.bind("X", set_no_merge)
    root.bind("q", quit_review)
    root.bind("Q", quit_review)

    show_current_pair()
    root.mainloop()


if __name__ == "__main__":
    run_review()
