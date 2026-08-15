from __future__ import annotations

import argparse
import math
from pathlib import Path

import numpy as np
import pandas as pd

from sparkevitune.feature_store import FeatureStore
from sparkevitune.features import FEATURE_COLUMNS


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate clearly labeled synthetic demo history.")
    parser.add_argument("--rows", type=int, default=250)
    parser.add_argument("--db", default="data/sparkevitune_history.db")
    parser.add_argument("--csv", default="data/demo_history.csv")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    store = FeatureStore(args.db)
    exported: list[dict[str, float | str]] = []
    for index in range(args.rows):
        input_size = float(rng.uniform(0.2, 50.0))
        workers = int(rng.choice([1, 2, 4, 8]))
        cores_per_worker = int(rng.choice([2, 4, 8]))
        memory_per_worker = float(rng.choice([4, 8, 16, 32]))
        executor_memory = float(rng.choice([1, 2, 4, 6, 8, 12]))
        executor_cores = int(rng.choice([1, 2, 4]))
        executor_instances = max(1, min(workers * 2, int(rng.integers(1, workers * 2 + 1))))
        shuffle_partitions = int(rng.choice([10, 20, 50, 100, 200, 400, 800]))
        aqe = int(rng.integers(0, 2))
        skew_join = aqe * int(rng.integers(0, 2))
        kryo = int(rng.integers(0, 2))
        memory_fraction = float(rng.uniform(0.5, 0.75))
        joins = int(rng.integers(0, 6))
        aggregations = int(rng.integers(0, 8))
        tasks = max(10, int(input_size * rng.uniform(5, 18)))
        stages = max(2, joins * 2 + aggregations + int(rng.integers(1, 5)))
        shuffle = input_size * (0.15 + joins * 0.22 + aggregations * 0.05) * rng.uniform(0.75, 1.25)
        skew = 1.0 + joins * rng.uniform(0.2, 1.2)
        memory_need = input_size / max(executor_instances, 1) * (0.25 + 0.05 * joins)
        spill = max(0.0, memory_need - executor_memory * memory_fraction) * rng.uniform(0.6, 1.4)
        gc_ratio = min(0.45, 0.02 + spill * 0.02 + rng.uniform(0.0, 0.04))
        baseline_duration = (
            input_size * 2.2
            + shuffle * 1.5
            + spill * 9.0
            + skew * 1.8
            + tasks * 0.02
        ) / max(workers * cores_per_worker * 0.35, 1)
        config_factor = 1.0
        config_factor *= 0.82 if aqe else 1.08
        config_factor *= 0.92 if kryo else 1.0
        optimal_parts = max(10, int(math.ceil(shuffle * 1024 / 128 / 10) * 10))
        config_factor *= 1.0 + min(1.2, abs(shuffle_partitions - optimal_parts) / max(optimal_parts, 10) * 0.15)
        duration = max(0.2, baseline_duration * config_factor * rng.normal(1.0, 0.06))
        cost = duration / 3600.0 * (
            workers * cores_per_worker * 0.05 + workers * memory_per_worker * 0.005
        )
        oom = float(executor_memory + max(0.384, executor_memory * 0.1) > memory_per_worker * 0.9 or spill > 15)

        features = {
            "duration_s": duration,
            "num_stages": float(stages),
            "num_tasks": float(tasks),
            "memory_spill_gb": spill,
            "disk_spill_gb": spill * rng.uniform(0.8, 1.4),
            "shuffle_write_gb": shuffle * 0.55,
            "shuffle_read_gb": shuffle * 0.45,
            "gc_ratio": gc_ratio,
            "max_skew_ratio": skew,
            "executor_memory_gb": executor_memory,
            "executor_cores": float(executor_cores),
            "executor_instances": float(executor_instances),
            "driver_memory_gb": float(rng.choice([1, 2, 4, 8])),
            "shuffle_partitions": float(shuffle_partitions),
            "aqe_enabled": float(aqe),
            "skew_join_enabled": float(skew_join),
            "kryo_enabled": float(kryo),
            "memory_fraction": memory_fraction,
            "workers": float(workers),
            "cores_per_worker": float(cores_per_worker),
            "memory_per_worker_gb": memory_per_worker,
            "input_size_gb": input_size,
            "num_joins": float(joins),
            "num_aggregations": float(aggregations),
            "streaming": 0.0,
        }
        targets = {
            "duration_s": duration,
            "memory_spill_gb": spill,
            "cost": cost,
            "oom": oom,
        }
        run_id = f"synthetic-demo-{args.seed}-{index:05d}"
        store.upsert_run(
            run_id,
            app_id=run_id,
            features=features,
            config={},
            targets=targets,
            metadata={"synthetic": True, "purpose": "demo/testing only"},
        )
        exported.append({"run_id": run_id, **features, **{f"target_{k}": v for k, v in targets.items()}})

    output = Path(args.csv)
    output.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(exported).to_csv(output, index=False)
    print(f"Generated {args.rows} synthetic demo runs in {args.db} and {args.csv}.")
    print("Do not use synthetic demo metrics as scientific evidence.")


if __name__ == "__main__":
    main()
