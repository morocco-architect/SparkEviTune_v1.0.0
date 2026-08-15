#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REQUIRED_METRICS = {
    "duration_s",
    "memory_spill_gib",
    "shuffle_write_gib",
    "gc_ratio",
    "oom",
    "cost",
}


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if int(manifest.get("repetitions", 0)) < 1:
        raise ValueError("Manifest repetitions must be >= 1")
    if not manifest.get("clusters") or not manifest.get("workloads"):
        raise ValueError("Manifest requires clusters and workloads")
    return manifest


def build_plan(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for cluster in manifest["clusters"]:
        for workload in manifest["workloads"]:
            for size in workload["sizes"]:
                for architecture in manifest["architectures"]:
                    for repetition in range(1, int(manifest["repetitions"]) + 1):
                        plan.append(
                            {
                                "cluster": cluster,
                                "workload": workload,
                                "size": size,
                                "architecture": architecture,
                                "repetition": repetition,
                            }
                        )
    if manifest.get("randomized_order", True):
        random.Random(20260803).shuffle(plan)
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute a real, repeated Spark benchmark manifest.")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, default=Path("benchmarks/results"))
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    args.results_dir.mkdir(parents=True, exist_ok=True)
    csv_path = args.results_dir / "benchmark_runs.csv"
    plan = build_plan(manifest)
    fields = [
        "run_id",
        "timestamp_utc",
        "cluster",
        "workload",
        "size",
        "architecture",
        "repetition",
        "return_code",
        "wall_clock_s",
        *sorted(REQUIRED_METRICS),
        "run_dir",
    ]
    write_header = not csv_path.exists()

    with csv_path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        if write_header:
            writer.writeheader()
        for index, item in enumerate(plan, start=1):
            run_id = (
                f"{item['cluster']['name']}__{item['workload']['name']}__{item['size']}__"
                f"{item['architecture']}__r{item['repetition']:02d}"
            )
            run_dir = args.results_dir / "runs" / run_id
            run_dir.mkdir(parents=True, exist_ok=True)
            replacements = {
                "size": str(item["size"]),
                "architecture": str(item["architecture"]),
                "run_dir": str(run_dir),
                "repetition": str(item["repetition"]),
            }
            command = [str(part).format(**replacements) for part in item["workload"]["command"]]
            environment = os.environ.copy()
            environment.update({str(k): str(v) for k, v in item["cluster"].get("environment", {}).items()})
            environment["SPARKEVITUNE_BENCHMARK_RUN_ID"] = run_id
            (run_dir / "command.json").write_text(json.dumps(command, indent=2), encoding="utf-8")
            print(f"[{index}/{len(plan)}] {run_id}")
            start = time.perf_counter()
            completed = subprocess.run(
                command,
                env=environment,
                cwd=Path.cwd(),
                text=True,
                stdout=(run_dir / "stdout.log").open("w", encoding="utf-8"),
                stderr=(run_dir / "stderr.log").open("w", encoding="utf-8"),
                check=False,
            )
            wall_clock_s = time.perf_counter() - start
            metrics_path = run_dir / "metrics.json"
            metrics: dict[str, Any] = {}
            if metrics_path.exists():
                metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
            missing = REQUIRED_METRICS - set(metrics)
            if completed.returncode == 0 and missing:
                raise RuntimeError(f"{run_id} completed but metrics.json misses: {sorted(missing)}")
            row = {
                "run_id": run_id,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "cluster": item["cluster"]["name"],
                "workload": item["workload"]["name"],
                "size": item["size"],
                "architecture": item["architecture"],
                "repetition": item["repetition"],
                "return_code": completed.returncode,
                "wall_clock_s": round(wall_clock_s, 6),
                "run_dir": str(run_dir),
                **{key: metrics.get(key, "") for key in sorted(REQUIRED_METRICS)},
            }
            writer.writerow(row)
            handle.flush()

    print(f"Wrote {csv_path}")


if __name__ == "__main__":
    main()
