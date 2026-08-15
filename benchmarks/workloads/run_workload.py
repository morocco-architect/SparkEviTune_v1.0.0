#!/usr/bin/env python3
"""Execute one deterministic Spark workload and emit SparkEviTune benchmark metrics.

This script requires a real PySpark/Spark installation. It intentionally imports
pyspark inside main so the repository can be tested without Spark installed.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path


def load_config(path: Path | None) -> dict[str, str]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {str(k): str(v) for k, v in payload.items() if not str(k).startswith("_")}


def latest_event_log(directory: Path) -> Path:
    candidates = [p for p in directory.rglob("*") if p.is_file() and not p.name.endswith(".inprogress")]
    if not candidates:
        raise FileNotFoundError(f"No completed event log found under {directory}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def build_data(spark, rows: int, skew_pct: int):
    from pyspark.sql import functions as F

    return (
        spark.range(rows)
        .withColumn("key", (F.col("id") % 1000).cast("int"))
        .withColumn(
            "skew_key",
            F.when((F.col("id") % 100) < skew_pct, F.lit(0)).otherwise((F.col("id") % 1000).cast("int")),
        )
        .withColumn("group_a", (F.col("id") % 24).cast("int"))
        .withColumn("group_b", (F.col("id") % 7).cast("int"))
        .withColumn("value", ((F.col("id") * 17) % 10000).cast("double") / 100.0)
    )


def execute_workload(spark, workload: str, rows: int, output: Path, partitions: int, skew_pct: int) -> None:
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    df = build_data(spark, rows, skew_pct)
    output_str = str(output.resolve())

    if workload == "etl":
        result = (
            df.filter(F.col("value") > 5.0)
            .withColumn("bucket", (F.col("value") / 10).cast("int"))
            .groupBy("key", "bucket")
            .agg(F.count("*").alias("records"), F.avg("value").alias("avg_value"))
        )
    elif workload == "sql_joins":
        dim_rows = max(1_000_000, min(2_000_000, rows // 5))
        sql_df = (
            df
            .withColumn("join_key1", (F.col("id") % dim_rows).cast("long"))
            .withColumn("join_key2", ((F.col("id") * 17 + 13) % dim_rows).cast("long"))
        )
        dim = spark.range(dim_rows).select(
            F.col("id").alias("dim_key"),
            (F.col("id") % 64).cast("int").alias("region"),
            ((F.col("id") * 13) % 1000).cast("double").alias("weight1"),
        )
        dim2 = spark.range(dim_rows).select(
            F.col("id").alias("dim_key2"),
            (F.col("id") % 32).cast("int").alias("region2"),
            ((F.col("id") * 7) % 1000).cast("double").alias("weight2"),
        )
        joined = (
            sql_df
            .join(dim, F.col("join_key1") == F.col("dim_key"), "left")
            .drop("dim_key")
            .join(dim2, F.col("join_key2") == F.col("dim_key2"), "left")
            .drop("dim_key2")
        )
        aggregated = joined.groupBy("region", "region2", "group_a").agg(
            F.count("*").alias("records"),
            F.sum("value").alias("total_value"),
            F.avg("weight1").alias("avg_weight1"),
            F.avg("weight2").alias("avg_weight2"),
        )
        window = Window.partitionBy("region").orderBy(F.desc("records"))
        result = aggregated.withColumn("rank_in_region", F.rank().over(window))
    elif workload == "heavy_shuffle":
        repartitioned = df.repartition(partitions, "key")
        a = repartitioned.groupBy("group_a", "key").agg(F.sum("value").alias("metric"))
        b = repartitioned.groupBy("group_b", "key").agg(F.avg("value").alias("metric"))
        result = a.select("key", "metric").unionByName(b.select("key", "metric")).groupBy("key").agg(
            F.sum("metric").alias("metric")
        ).orderBy(F.desc("metric"))
    elif workload == "skew_join":
        dim = spark.range(1000).select(F.col("id").cast("int").alias("dim_key"), F.lit(1).alias("weight"))
        result = (
            df.join(dim, df.skew_key == dim.dim_key, "inner")
            .groupBy("skew_key")
            .agg(F.count("*").alias("records"), F.sum("value").alias("total_value"))
        )
    else:
        raise ValueError(f"Unsupported workload: {workload}")

    result.write.mode("overwrite").parquet(output_str)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workload", choices=["etl", "sql_joins", "heavy_shuffle", "skew_join"], required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--architecture", required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--config-json", type=Path)
    parser.add_argument("--skew-pct", type=int, default=70)
    args = parser.parse_args()

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise SystemExit("PySpark is required to execute the real benchmark workload.") from exc

    args.run_dir.mkdir(parents=True, exist_ok=True)
    event_dir = args.run_dir / "event-logs"
    output_dir = args.run_dir / "output"
    event_dir.mkdir(parents=True, exist_ok=True)
    config_path = args.config_json or Path("benchmarks/configurations") / f"{args.architecture}.json"
    config = load_config(config_path)

    builder = SparkSession.builder.appName(
        f"SparkEviTune-{args.workload}-{args.architecture}-{os.getenv('SPARKEVITUNE_BENCHMARK_RUN_ID', 'run')}"
    )
    master = os.getenv("SPARK_MASTER")
    if master:
        builder = builder.master(master)
    builder = (
        builder.config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.compress", "false")
        .config("spark.eventLog.dir", event_dir.resolve().as_uri())
        .config("spark.ui.enabled", "false")
    )
    for key, value in config.items():
        builder = builder.config(key, value)

    start = time.perf_counter()
    oom = 0
    spark = None
    try:
        spark = builder.getOrCreate()
        effective_partitions = int(spark.conf.get("spark.sql.shuffle.partitions", "200"))
        execute_workload(
            spark,
            args.workload,
            args.rows,
            output_dir,
            effective_partitions,
            args.skew_pct,
        )
        output_rows = None
    except Exception as exc:
        oom = int("OutOfMemory" in type(exc).__name__ or "OutOfMemory" in str(exc))
        raise
    finally:
        if spark is not None:
            spark.stop()
    wall_clock_s = time.perf_counter() - start

    # Import after execution so the package can be on PYTHONPATH or installed.
    from sparkevitune.parser import SparkLogParser

    event_log = latest_event_log(event_dir)
    app = SparkLogParser().parse(event_log)
    total_core_hours = wall_clock_s / 3600.0 * float(os.getenv("SPARKEVITUNE_TOTAL_CORES", "1"))
    total_gb_hours = wall_clock_s / 3600.0 * float(os.getenv("SPARKEVITUNE_TOTAL_MEMORY_GB", "1"))
    cost = total_core_hours * float(os.getenv("SPARKEVITUNE_COST_PER_CORE_HOUR", "0")) + total_gb_hours * float(
        os.getenv("SPARKEVITUNE_COST_PER_GB_HOUR", "0")
    )
    metrics = {
        "duration_s": app.duration_ms / 1000.0,
        "memory_spill_gib": app.total_memory_spill_gb,
        "disk_spill_gib": app.total_disk_spill_gb,
        "shuffle_write_gib": app.total_shuffle_write_gb,
        "shuffle_read_gib": app.total_shuffle_read_gb,
        "gc_ratio": app.avg_gc_ratio,
        "max_skew_ratio": app.max_skew_ratio,
        "num_stages": app.num_stages,
        "num_tasks": app.num_tasks,
        "oom": oom,
        "cost": cost,
        "wall_clock_s": wall_clock_s,
        "output_rows": output_rows,
        "event_log": str(event_log),
        "spark_config": app.spark_config,
        "workload": args.workload,
        "architecture": args.architecture,
        "input_rows": args.rows,
    }
    (args.run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
