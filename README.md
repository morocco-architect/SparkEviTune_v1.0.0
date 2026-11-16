# SparkEviTune

**Published release: v1.0.1 (2026-11-16).**

SparkEviTune is an evidence-aware, human-controlled software framework for Apache Spark configuration guidance. It combines deterministic configuration rules, event-log analysis, optional machine-learning components subject to data-readiness gates, constrained optimization, and optional explanations.

This corrective release passed 61 automated tests under Windows Python 3.12 and Python 3.14. These tests do **not** establish general multi-worker deployment correctness or unrestricted concurrency safety. Historical Spark benchmarks remain associated with their original implementations and environments. The publication ML gate is closed (16 distinct scenarios against 20 required); no general predictive accuracy or universal speedup is claimed. AQE-01 and Lot C are not validated. Suggested configurations are not automatically applied.

- [GitHub release v1.0.1](https://github.com/morocco-architect/SparkEviTune_v1.0.0/releases/tag/v1.0.1)
- [Zenodo record](https://zenodo.org/records/24293565)

## Architecture

```mermaid
flowchart LR
    L[Spark event log] --> P[Streaming parser]
    P --> D[Evidence-aware symptoms]
    P --> F[Feature builder]
    D --> R[Rules-only recommendations]
    F --> M[Optional historical ML]
    M --> O[Constrained Bayesian search]
    R --> U[Fusion]
    O --> U
    U --> V[Constraint and policy validator]
    V --> H[Human approval]
    H --> X[Controlled re-run]
    X --> S[Feature store / feedback]
```

## Quick start

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[ui,dev]"
pytest -q tests
```

Analyze a raw Spark event log:

```bash
sparkevitune-analyze tests/fixtures/minimal_event_log.jsonl \
  --workers 1 \
  --cores-per-worker 4 \
  --memory-per-worker-gb 8 \
  --input-size-gb 0 \
  --input-rows 1000000
```

The deterministic layer works without ML models. If no validated model exists, prediction and optimization remain unavailable rather than fabricating a result.

## Real benchmark runner

The publication protocol uses a single Spark action per workload: writing the result to Parquet. A post-write `count()` is intentionally absent.

```bash
export SPARK_MASTER='local[4]'
export SPARKEVITUNE_TOTAL_CORES='4'
export SPARKEVITUNE_TOTAL_MEMORY_GB='8'

python benchmarks/workloads/run_workload.py \
  --workload heavy_shuffle \
  --rows 1000000 \
  --architecture defaults \
  --run-dir benchmarks/results/example-heavy
```

Supported generated workloads are `etl`, `sql_joins`, `heavy_shuffle`, and `skew_join`.

## Publication evidence

Machine-readable summaries are shipped under `artifacts/softwarex_evidence/`:

- `rules_evaluation_summary.csv`
- `ml_readiness_summary.csv`
- `scenario_coverage.csv`

The current repeated evaluation shows a strong benefit when overpartitioning is genuinely observed (Heavy Shuffle and the deliberate skew-stress configuration), while SQL join cases under Spark 3.5 AQE provide important neutral results. These null results are part of the design evidence because they motivate abstention from universal serializer, skewJoin, and static-partition recommendations.

## ML data policy

Synthetic demo data can still be generated for UI/software demonstrations, but it must not be used as publication evidence.

```bash
python scripts/generate_demo_history.py --rows 250
```

For research history, use real write-only runs. `scripts/build_publication_history.py` creates a separate publication database and refuses to overwrite an existing one.

Training is gated by both row count and distinct scenario count. By default:

```text
SPARKEVITUNE_MIN_TRAINING_RUNS=20
SPARKEVITUNE_MIN_TRAINING_SCENARIOS=20
```

The current SoftwareX evidence has 81 clean real runs but only 16 distinct scenarios, so the publication trainer intentionally abstains. This release does not claim predictive ML accuracy, cost prediction, or OOM-risk calibration from that corpus.

## Core modules

| Module | Purpose |
|---|---|
| `parser.py` | streaming Spark JSON-lines parser and effective Spark 3.5 configuration |
| `detector.py` | evidence-aware symptom detection |
| `engine.py` | deterministic recommendations and abstention policy |
| `features.py` | leakage-controlled app/cluster/workload features including `input_rows` |
| `feature_store.py` | SQLite historical store |
| `ml.py` | gated model training, prediction, anomaly detection |
| `optimizer.py` | constrained Gaussian-process configuration search |
| `fusion.py` | deterministic/ML recommendation fusion |
| `validator.py` | memory/CPU/bounds/secret checks |
| `llm.py` | local retrieval and optional explanation layer |
| `pipeline.py` | end-to-end orchestration and feedback alignment |

## API and dashboard

```bash
uvicorn api.main:app --reload --port 8000
streamlit run dashboard/app.py
```

The API exposes health, analysis, report retrieval, model training/status, feedback, and explanation endpoints. The dashboard keeps rule evidence visible even when ML or an external LLM is unavailable.

## Safety and scope

- human approval is mandatory;
- no automatic deployment;
- local-mode executor sizing is not treated as a causal tuning action;
- secret-like fields are filtered before optional LLM use;
- rule compliance is not presented as proof of optimal performance;
- current repeated empirical evidence is `local[4]`; multi-worker generalization is future work.

## Reproducibility

Use the exact tagged source archive, preserve raw event logs outside the compact source distribution, and archive generated reports/checksums with any submission. The final paper source is distributed separately as an Overleaf-ready archive.

## Official repository

https://github.com/morocco-architect/SparkEviTune_v1.0.0

**Pre-submission requirement:** publish this exact source as GitHub release `v1.0.0` and archive that release in Zenodo (or an equivalent long-term repository). Then add the immutable release URL and DOI to `CITATION.cff` and the paper's code-metadata table before submission.

## License

MIT. See `LICENSE`.

## Citation

See `CITATION.cff`. For long-term citation, create a versioned GitHub release and archival DOI from the official repository.
