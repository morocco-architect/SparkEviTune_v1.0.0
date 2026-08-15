#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def parse_memory_gb(value: str, default: float = 1.0) -> float:
    value = str(value).strip().lower()
    try:
        if value.endswith("g"):
            return float(value[:-1])
        if value.endswith("m"):
            return float(value[:-1]) / 1024.0
        return float(value) / 1024**3
    except ValueError:
        return default


def main() -> None:
    parser = argparse.ArgumentParser(description="Build a model-ready real corpus from benchmark run directories.")
    parser.add_argument("--runs-csv", default="benchmarks/results/benchmark_runs.csv")
    parser.add_argument("--output", default="data/real/multicluster_benchmark_runs.csv")
    args = parser.parse_args()
    runs = pd.read_csv(args.runs_csv)
    records = []
    for _, run in runs.iterrows():
        if int(run.get("return_code", 1)) != 0:
            continue
        metrics_path = Path(str(run["run_dir"])) / "metrics.json"
        if not metrics_path.exists():
            continue
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        cfg = metrics.get("spark_config", {})
        workload = str(run["workload"])
        structure = {
            "etl": (0, 1),
            "sql_joins": (2, 2),
            "heavy_shuffle": (0, 3),
            "skew_join": (1, 1),
        }
        joins, aggregations = structure.get(workload, (0, 0))
        records.append(
            {
                "run_id": run["run_id"],
                "app_id": run["run_id"],
                "workload": workload,
                "variant": run["architecture"],
                "cluster": run["cluster"],
                "repetition": int(run["repetition"]),
                "label_anomaly": 0,
                "source_kind": "real_repeated_benchmark",
                "duration_s": float(metrics["duration_s"]),
                "num_stages": float(metrics.get("num_stages", 0)),
                "num_tasks": float(metrics.get("num_tasks", 0)),
                "memory_spill_gb": float(metrics.get("memory_spill_gib", 0)),
                "disk_spill_gb": float(metrics.get("disk_spill_gib", 0)),
                "shuffle_write_gb": float(metrics.get("shuffle_write_gib", 0)),
                "shuffle_read_gb": float(metrics.get("shuffle_read_gib", 0)),
                "gc_ratio": float(metrics.get("gc_ratio", 0)),
                "max_skew_ratio": float(metrics.get("max_skew_ratio", 1)),
                "executor_memory_gb": parse_memory_gb(cfg.get("spark.executor.memory", "1g")),
                "executor_cores": float(cfg.get("spark.executor.cores", 1)),
                "executor_instances": float(cfg.get("spark.executor.instances", 1)),
                "driver_memory_gb": parse_memory_gb(cfg.get("spark.driver.memory", "1g")),
                "shuffle_partitions": float(cfg.get("spark.sql.shuffle.partitions", 200)),
                "aqe_enabled": float(str(cfg.get("spark.sql.adaptive.enabled", "false")).lower() == "true"),
                "skew_join_enabled": float(str(cfg.get("spark.sql.adaptive.skewJoin.enabled", "false")).lower() == "true"),
                "kryo_enabled": float("Kryo" in str(cfg.get("spark.serializer", ""))),
                "memory_fraction": float(cfg.get("spark.memory.fraction", 0.6)),
                "workers": float(cfg.get("sparkevitune.cluster.workers", 1)),
                "cores_per_worker": float(cfg.get("sparkevitune.cluster.cores_per_worker", 1)),
                "memory_per_worker_gb": float(cfg.get("sparkevitune.cluster.memory_per_worker_gb", 1)),
                "input_size_gb": float(metrics.get("input_rows", 0)) / 1_000_000.0,
                "num_joins": float(joins),
                "num_aggregations": float(aggregations),
                "streaming": 0.0,
                "target_duration_s": float(metrics["duration_s"]),
                "target_memory_spill_gb": float(metrics.get("memory_spill_gib", 0)),
                "target_cost": float(metrics.get("cost", 0)),
                "target_oom": float(metrics.get("oom", 0)),
            }
        )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(records).to_csv(output, index=False)
    print(f"Wrote {len(records)} real benchmark rows to {output}")


if __name__ == "__main__":
    main()
