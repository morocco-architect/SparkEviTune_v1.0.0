# Real repeated-execution benchmark

This directory is a **benchmark harness**, not a source of published results. The paper's ML and optimization findings must be generated from real Spark runs before submission.

## Required design

- At least four workload families: ETL, SQL joins, shuffle-intensive and skewed joins.
- At least three input sizes.
- At least two genuinely different cluster profiles, including one multi-worker cluster.
- At least five repetitions per condition, with randomized execution order.
- Architectures: Spark defaults, rules only, ML only, rules+ML, rules+ML+optimizer, full validated system.
- Cold/warm cache policy documented and applied consistently.
- Raw event logs, configuration files, model versions, random seeds and validator decisions archived.

## Run

1. Implement or point the manifest commands to the real workload runners.
2. Copy `benchmark_manifest.example.json` to `benchmark_manifest.json`.
3. Execute:

```bash
python scripts/run_real_benchmark.py --manifest benchmarks/benchmark_manifest.json
python scripts/summarize_real_benchmark.py --input benchmarks/results/benchmark_runs.csv
```

The runner never fabricates metrics. Each workload command must write a `metrics.json` file containing at least `duration_s`, `memory_spill_gib`, `shuffle_write_gib`, `gc_ratio`, `oom`, and `cost`.

## Publication gate

Do not replace the manuscript's `TBD` evaluation panels until the CSV is generated from completed real runs and independently checked against archived Spark event logs.
