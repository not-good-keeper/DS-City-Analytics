from __future__ import annotations

import argparse
import logging
import time
from datetime import datetime
from pathlib import Path

from pyspark.sql.column import Column
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


# =========================
# Vehicle class group rules
# =========================
HEAVY_VEHICLE_KEYWORDS = (
    "truck",
    "bus",
    "trailer",
    "tractor",
    "lorry",
    "tanker",
    "dumper",
)

TWO_WHEELER_KEYWORDS = (
    "bike",
    "motorbike",
    "motorcycle",
    "bicycle",
    "cycle",
    "scooter",
    "moped",
    "two wheeler",
    "two-wheeler",
)


# =========================
# CLI / runtime parameters
# =========================
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Stage 2 distributed viewpoint analytics engine.")
    parser.add_argument("--master", default=None, help="Spark master URL, e.g. spark://host:7077")
    parser.add_argument("--app-name", default="DS-City-Stage2-Analytics")
    parser.add_argument(
        "--mapping-csv",
        default="../UVH26_Project/outputs/image_viewpoint_mapping_masked40_rerun.csv",
        help="Path to Stage 1 mapping CSV (image_id,viewpoint_id).",
    )
    parser.add_argument(
        "--train-json",
        default="../UVH26_Project/data/raw/UVH-26/UVH-26-Train/UVH-26-MV-Train.json",
        help="Path to COCO train JSON.",
    )
    parser.add_argument(
        "--val-json",
        default="../UVH26_Project/data/raw/UVH-26/UVH-26-Val/UVH-26-MV-Val.json",
        help="Path to COCO val JSON.",
    )
    parser.add_argument(
        "--output-dir",
        default="../UVH26_Project/outputs/stage2/viewpoint_analytics/",
        help="Output Parquet directory.",
    )
    parser.add_argument("--log-dir", default="logs/", help="Driver log directory.")
    parser.add_argument("--shuffle-partitions", type=int, default=48)
    parser.add_argument("--default-parallelism", type=int, default=48)
    parser.add_argument("--skip-output-write", action="store_true", help="Compute metrics but skip Parquet write (useful for smoke tests).")
    parser.add_argument(
        "--preview-jsonl",
        default=None,
        help="Optional path to write a small JSONL preview of viewpoint metrics.",
    )
    parser.add_argument(
        "--preview-limit",
        type=int,
        default=20,
        help="Maximum rows to include in preview JSONL when --preview-jsonl is set.",
    )
    return parser


def _build_logger(log_dir: Path) -> tuple[logging.Logger, Path]:
    log_dir.mkdir(parents=True, exist_ok=True)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = log_dir / f"stage2_analytics_{run_stamp}.log"

    logger = logging.getLogger("stage2_analytics")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger, log_path


# =========================
# Spark session bootstrap
# =========================
def _spark_builder(args: argparse.Namespace) -> SparkSession:
    builder = SparkSession.builder.appName(args.app_name)
    if args.master:
        builder = builder.master(args.master)
    builder = builder.config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
    builder = builder.config("spark.default.parallelism", str(args.default_parallelism))
    builder = builder.config("spark.sql.adaptive.enabled", "true")
    return builder.getOrCreate()


def _load_coco_payload(spark: SparkSession, json_path: str) -> DataFrame:
    return spark.read.option("multiline", "true").json(json_path)


# =========================
# COCO normalization layer
# =========================
def _normalize_coco(coco_df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    images_df = (
        coco_df.select(F.explode("images").alias("img"))
        .select(
            F.col("img.id").cast("long").alias("coco_image_id"),
            F.col("img.file_name").cast("string").alias("file_name"),
            F.col("img.width").cast("double").alias("width"),
            F.col("img.height").cast("double").alias("height"),
        )
        .withColumn("image_area", F.col("width") * F.col("height"))
        .withColumn("file_name_only", F.regexp_replace(F.col("file_name"), r"^.*[\\/]", ""))
        .withColumn("image_id", F.regexp_replace(F.col("file_name_only"), r"\.[^\.]+$", ""))
        .drop("file_name_only")
    )

    ann_df = coco_df.select(F.explode("annotations").alias("ann")).select(
        F.col("ann.id").cast("long").alias("annotation_id"),
        F.col("ann.image_id").cast("long").alias("coco_image_id"),
        F.col("ann.category_id").cast("int").alias("category_id"),
        F.col("ann.area").cast("double").alias("bbox_area_raw"),
        F.col("ann.bbox").alias("bbox"),
    )

    ann_df = ann_df.withColumn(
        "bbox_area",
        F.when(F.col("bbox_area_raw").isNotNull(), F.col("bbox_area_raw")).otherwise(
            F.when(
                (F.col("bbox").isNotNull()) & (F.size(F.col("bbox")) >= 4),
                F.col("bbox").getItem(2).cast("double") * F.col("bbox").getItem(3).cast("double"),
            ).otherwise(F.lit(0.0))
        ),
    ).drop("bbox_area_raw")

    categories_df = coco_df.select(F.explode("categories").alias("cat")).select(
        F.col("cat.id").cast("int").alias("category_id"),
        F.col("cat.name").cast("string").alias("category_name"),
    )
    return images_df, ann_df, categories_df


def _keyword_flag(column: Column, keywords: tuple[str, ...]) -> Column:
    lowered = F.lower(F.coalesce(column, F.lit("")))
    expr = F.lit(False)
    for token in keywords:
        expr = expr | lowered.contains(token)
    return expr


# =========================
# Stage 2 analytics pipeline
# =========================
def _compute_analytics(spark: SparkSession, args: argparse.Namespace, logger: logging.Logger) -> DataFrame:
    mapping_df = (
        spark.read.option("header", "true")
        .csv(args.mapping_csv)
        .select(
            F.col("image_id").cast("string").alias("image_id"),
            F.col("viewpoint_id").cast("long").alias("viewpoint_id"),
        )
        .dropna(subset=["image_id", "viewpoint_id"])
    )

    train_coco = _load_coco_payload(spark, args.train_json)
    val_coco = _load_coco_payload(spark, args.val_json)

    train_images, train_ann, train_cats = _normalize_coco(train_coco)
    val_images, val_ann, val_cats = _normalize_coco(val_coco)

    images_df = train_images.unionByName(val_images)
    annotations_df = train_ann.unionByName(val_ann)
    categories_df = train_cats.unionByName(val_cats).dropDuplicates(["category_id"])

    # Annotation ↔ image join, then attach Stage 1 viewpoint mapping.
    # Mapping is broadcasted because it is comparatively small.
    joined_ann = (
        annotations_df.join(images_df, on="coco_image_id", how="inner")
        .join(F.broadcast(mapping_df), on="image_id", how="inner")
        .join(categories_df, on="category_id", how="left")
        .withColumn("category_name", F.coalesce(F.col("category_name"), F.concat(F.lit("category_"), F.col("category_id"))))
        .withColumn("bbox_area", F.when(F.col("bbox_area") < 0, F.lit(0.0)).otherwise(F.col("bbox_area")))
        .withColumn("image_area", F.when(F.col("image_area") <= 0, F.lit(1.0)).otherwise(F.col("image_area")))
    )

    # Per-image metrics: required Stage 2 intermediate aggregation.
    image_metrics = (
        joined_ann.groupBy("viewpoint_id", "image_id")
        .agg(
            F.count(F.lit(1)).alias("vehicle_count"),
            F.sum("bbox_area").alias("total_bbox_area"),
            F.first("image_area", ignorenulls=True).alias("image_area"),
            F.sum(F.when(_keyword_flag(F.col("category_name"), HEAVY_VEHICLE_KEYWORDS), 1).otherwise(0)).alias("heavy_vehicle_count"),
            F.sum(F.when(_keyword_flag(F.col("category_name"), TWO_WHEELER_KEYWORDS), 1).otherwise(0)).alias("two_wheeler_count"),
        )
        .withColumn("bbox_density", F.col("total_bbox_area") / F.col("image_area"))
    )

    # Viewpoint-level aggregation (wide transformation with shuffle).
    viewpoint_core = image_metrics.groupBy("viewpoint_id").agg(
        F.count(F.lit(1)).alias("total_images"),
        F.sum("vehicle_count").alias("total_vehicles"),
        F.avg("vehicle_count").alias("avg_vehicle_count"),
        F.avg("bbox_density").alias("avg_bbox_density"),
        F.sum("heavy_vehicle_count").alias("heavy_vehicle_total"),
        F.sum("two_wheeler_count").alias("two_wheeler_total"),
    )

    viewpoint_core = (
        viewpoint_core.withColumn(
            "heavy_vehicle_ratio",
            F.when(F.col("total_vehicles") > 0, F.col("heavy_vehicle_total") / F.col("total_vehicles")).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "two_wheeler_ratio",
            F.when(F.col("total_vehicles") > 0, F.col("two_wheeler_total") / F.col("total_vehicles")).otherwise(F.lit(0.0)),
        )
        .drop("heavy_vehicle_total", "two_wheeler_total")
    )

    class_counts = joined_ann.groupBy("viewpoint_id", "category_name").agg(F.count(F.lit(1)).alias("class_count"))
    class_totals = class_counts.groupBy("viewpoint_id").agg(F.sum("class_count").alias("class_total"))

    class_probs = class_counts.join(class_totals, on="viewpoint_id", how="inner").withColumn(
        "class_prob",
        F.when(F.col("class_total") > 0, F.col("class_count") / F.col("class_total")).otherwise(F.lit(0.0)),
    )

    # Shannon entropy using log2 as requested.
    entropy_df = class_probs.withColumn(
        "entropy_component",
        F.when(F.col("class_prob") > 0, -F.col("class_prob") * F.log2(F.col("class_prob"))).otherwise(F.lit(0.0)),
    ).groupBy("viewpoint_id").agg(F.sum("entropy_component").alias("entropy"))

    class_distribution = class_probs.groupBy("viewpoint_id").agg(
        F.to_json(
            F.map_from_entries(F.collect_list(F.struct(F.col("category_name"), F.round(F.col("class_prob"), 6))))
        ).alias("class_distribution_vector")
    )

    result = (
        viewpoint_core.join(entropy_df, on="viewpoint_id", how="left")
        .join(class_distribution, on="viewpoint_id", how="left")
        .fillna({"entropy": 0.0, "class_distribution_vector": "{}"})
        .withColumn(
            "per_vehicle_count",
            F.when(F.col("total_images") > 0, F.col("total_vehicles") / F.col("total_images")).otherwise(F.lit(0.0)),
        )
        .withColumn("congestion_index", F.col("avg_vehicle_count") * F.col("avg_bbox_density"))
        .select(
            "viewpoint_id",
            "total_images",
            "total_vehicles",
            "avg_vehicle_count",
            "per_vehicle_count",
            "avg_bbox_density",
            "heavy_vehicle_ratio",
            "two_wheeler_ratio",
            "entropy",
            "congestion_index",
            "class_distribution_vector",
        )
    )

    logger.info("Input mapping rows: %s", mapping_df.count())
    logger.info("Joined annotation rows: %s", joined_ann.count())
    logger.info("Per-image metric rows: %s", image_metrics.count())
    logger.info("Output viewpoint rows: %s", result.count())
    logger.info("Joined partitions: %s", joined_ann.rdd.getNumPartitions())
    logger.info("Result partitions: %s", result.rdd.getNumPartitions())
    return result


# =========================
# Main entrypoint
# =========================
def main() -> None:
    args = _build_parser().parse_args()

    script_dir = Path(__file__).resolve().parent
    log_dir = Path(args.log_dir)
    output_dir = Path(args.output_dir)
    mapping_csv = Path(args.mapping_csv)
    train_json = Path(args.train_json)
    val_json = Path(args.val_json)
    preview_jsonl = Path(args.preview_jsonl) if args.preview_jsonl else None

    if not log_dir.is_absolute():
        log_dir = script_dir / log_dir
    if not output_dir.is_absolute():
        output_dir = script_dir / output_dir
    if not mapping_csv.is_absolute():
        mapping_csv = script_dir / mapping_csv
    if not train_json.is_absolute():
        train_json = script_dir / train_json
    if not val_json.is_absolute():
        val_json = script_dir / val_json
    if preview_jsonl is not None and not preview_jsonl.is_absolute():
        preview_jsonl = script_dir / preview_jsonl

    args.mapping_csv = str(mapping_csv)
    args.train_json = str(train_json)
    args.val_json = str(val_json)
    args.output_dir = str(output_dir)
    args.preview_jsonl = str(preview_jsonl) if preview_jsonl is not None else None

    logger, log_path = _build_logger(log_dir)
    logger.info("Starting Stage 2 analytics job")
    logger.info("Log path: %s", log_path)

    start_ts = time.time()
    spark = _spark_builder(args)

    try:
        sc = spark.sparkContext
        logger.info("Spark app id: %s", sc.applicationId)
        logger.info("Spark master: %s", sc.master)
        logger.info("Default parallelism: %s", sc.defaultParallelism)
        logger.info("Shuffle partitions: %s", spark.conf.get("spark.sql.shuffle.partitions"))
        logger.info("AQE enabled: %s", spark.conf.get("spark.sql.adaptive.enabled"))

        executor_entries = int(sc._jsc.sc().getExecutorMemoryStatus().size())
        executor_count = max(0, executor_entries - 1)
        logger.info("Executor count: %s", executor_count)

        result_df = _compute_analytics(spark, args, logger)

        if args.preview_jsonl:
            preview_path = Path(args.preview_jsonl)
            preview_path.parent.mkdir(parents=True, exist_ok=True)
            preview_rows = (
                result_df.orderBy(F.col("viewpoint_id").asc())
                .limit(max(1, int(args.preview_limit)))
                .toJSON()
                .collect()
            )
            with preview_path.open("w", encoding="utf-8") as handle:
                for row in preview_rows:
                    handle.write(row + "\n")
            logger.info("Preview JSONL written to: %s", preview_path)
            logger.info("Preview rows: %s", len(preview_rows))

        if args.skip_output_write:
            logger.info("Skip-output-write enabled; Parquet write step is skipped.")
        else:
            output_dir.mkdir(parents=True, exist_ok=True)
            result_df.orderBy(F.col("viewpoint_id").asc()).write.mode("overwrite").parquet(str(output_dir))
            logger.info("Stage 2 output written to: %s", output_dir)

        elapsed_s = time.time() - start_ts
        logger.info("Total execution time (s): %.2f", elapsed_s)
        logger.info("Total execution time (min): %.2f", elapsed_s / 60.0)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
