#!/usr/bin/env python3
"""Build a publication feature store from real write-only benchmark runs.

This script never imports synthetic demo history and refuses to overwrite an
existing publication database. It groups repeated runs by workload, input rows
and effective mutable Spark configuration via a stable scenario identifier.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path

from sparkevitune.feature_store import FeatureStore
from sparkevitune.features import FeatureBuilder
from sparkevitune.models import ClusterProfile, WorkloadProfile
from sparkevitune.parser import SparkLogParser

RESULTS = Path("benchmarks/results")
OUTPUT_DB = Path("data/sparkevitune_history_publication.db")
CLUSTER = ClusterProfile(1, 4, 8.0)

MUTABLE_CONFIG_KEYS = (
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.driver.memory",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.serializer",
    "spark.memory.fraction",
)


def digest(value: object, length: int = 16) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def structural_counts(workload: str) -> tuple[int, int]:
    # Counts reflect the current benchmark DAG definitions, not physical-plan nodes.
    return {
        "etl": (0, 1),
        "sql_joins": (2, 1),
        "heavy_shuffle": (0, 3),
        "skew_join": (1, 1),
    }.get(workload, (0, 0))


def main() -> None:
    if OUTPUT_DB.exists():
        raise SystemExit(f"{OUTPUT_DB} already exists; refusing to overwrite it.")

    store = FeatureStore(OUTPUT_DB)
    parser = SparkLogParser()
    builder = FeatureBuilder()
    imported = 0
    skipped = Counter()

    for metrics_path in sorted(RESULTS.rglob("metrics.json")):
        metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
        if metrics.get("output_rows") is not None:
            skipped["legacy_or_double_action"] += 1
            continue

        run_dir = metrics_path.parent
        event_dir = run_dir / "event-logs"
        logs = [
            p for p in event_dir.rglob("*")
            if p.is_file() and not p.name.endswith(".inprogress")
        ] if event_dir.exists() else []
        if len(logs) != 1:
            skipped[f"event_logs_{len(logs)}"] += 1
            continue

        workload_type = str(metrics.get("workload") or "unknown")
        input_rows = int(metrics.get("input_rows") or 0)
        architecture = str(metrics.get("architecture") or "unknown")
        num_joins, num_aggregations = structural_counts(workload_type)

        app = parser.parse(logs[0])
        workload = WorkloadProfile(
            workload_type=workload_type,
            input_size_gb=0.0,
            input_rows=input_rows,
            num_joins=num_joins,
            num_aggregations=num_aggregations,
        )
        features = builder.build(app, CLUSTER, workload)

        effective_mutable_config = {key: app.spark_config.get(key) for key in MUTABLE_CONFIG_KEYS}
        config_id = digest(effective_mutable_config, 12)
        scenario_id = digest({
            "workload": workload_type,
            "input_rows": input_rows,
            "config": effective_mutable_config,
        }, 16)
        relative_run_dir = str(run_dir.relative_to(RESULTS))
        run_id = "real-" + digest({"run_dir": relative_run_dir}, 20)

        targets = {
            "duration_s": float(metrics.get("duration_s") or 0.0),
            "memory_spill_gb": float(metrics.get("memory_spill_gib") or 0.0),
            "cost": float(metrics.get("cost") or 0.0),
            "oom": float(metrics.get("oom") or 0.0),
        }
        metadata = {
            "source": "real_spark_eventlog",
            "synthetic": False,
            "methodology": "write_only",
            "split": "development",
            "workload": workload_type,
            "architecture": architecture,
            "input_rows": input_rows,
            "config_id": config_id,
            "scenario_id": scenario_id,
            "run_dir": relative_run_dir,
        }
        store.upsert_run(
            run_id=run_id,
            app_id=app.app_id,
            features=features,
            config=app.spark_config,
            targets=targets,
            metadata=metadata,
        )
        imported += 1

    print("=== PUBLICATION HISTORY BUILD ===")
    print("database =", OUTPUT_DB)
    print("imported =", imported)
    print("SKIPPED")
    for reason, count in sorted(skipped.items()):
        print(reason, "=", count)
    print("store.count() =", store.count())


if __name__ == "__main__":
    main()
