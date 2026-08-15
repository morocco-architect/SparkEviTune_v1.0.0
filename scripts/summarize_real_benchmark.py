#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

GROUPS = ["cluster", "workload", "size", "architecture"]
METRICS = ["duration_s", "memory_spill_gib", "shuffle_write_gib", "gc_ratio", "cost"]


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize completed real benchmark runs.")
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=Path("benchmarks/results/benchmark_summary.csv"))
    args = parser.parse_args()

    frame = pd.read_csv(args.input)
    frame = frame[frame["return_code"] == 0].copy()
    if frame.empty:
        raise SystemExit("No successful benchmark rows were found.")
    for metric in METRICS:
        frame[metric] = pd.to_numeric(frame[metric], errors="coerce")

    records = []
    for keys, group in frame.groupby(GROUPS, dropna=False):
        record = dict(zip(GROUPS, keys))
        record["n"] = len(group)
        for metric in METRICS:
            values = group[metric].dropna().to_numpy(dtype=float)
            if values.size == 0:
                continue
            record[f"{metric}_median"] = float(np.median(values))
            record[f"{metric}_q1"] = float(np.quantile(values, 0.25))
            record[f"{metric}_q3"] = float(np.quantile(values, 0.75))
            record[f"{metric}_mean"] = float(np.mean(values))
            record[f"{metric}_std"] = float(np.std(values, ddof=1)) if values.size > 1 else 0.0
        records.append(record)
    output = pd.DataFrame(records).sort_values(GROUPS)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(args.output, index=False)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
