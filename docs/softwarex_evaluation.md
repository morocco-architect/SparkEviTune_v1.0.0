# SoftwareX evaluation snapshot - 2026-08-13

This file records the empirical scope used by the SoftwareX manuscript shipped with v1.0.0.

## Execution environment

- Apache Spark / PySpark 3.5.7
- Python 3.10.12
- Java 17
- WSL Ubuntu execution environment
- Spark master: `local[4]`
- resource accounting profile: 4 cores and 8 GiB total memory
- event logging enabled, uncompressed JSON-lines
- workload protocol: one Parquet write action; no post-write `count()` in publication runs

The local execution mode is a deliberate limitation. Executor-memory and executor-core changes are not treated as causal tuning levers in this environment.

## Evidence-aware rule policy

The current rules distinguish observations from actions:

- Spark 3.5.7 effective defaults are reconstructed when missing from the event log.
- static shuffle mismatch is suppressed when AQE/coalescing already reduces a small execution to a modest task count; observed large overpartitioning remains actionable.
- JavaSerializer remains diagnostic context; Rules-only does not recommend Kryo without workload-specific evidence.
- task-duration skew remains a runtime symptom; it does not automatically enable AQE skewJoin without partition-byte or plan evidence.
- spill remains observable in `local[*]`, but executor-memory escalation is suppressed there.

## Repeated-run results

- Heavy Shuffle 1M: baseline mean 12.071 s vs p10+Kryo 8.883 s, 26.41% gain. Paired mean gain 3.188 s, 95% CI [2.365, 4.012], t-test p=0.00017464, Wilcoxon p=0.03125. Tasks fell from 807 to 47. A separate Java-vs-Kryo ablation did not demonstrate a Kryo benefit.
- Skew stress 1M: baseline mean 11.984 s, p10-only 7.519 s (+37.26%), full configuration 7.707 s (+35.69%). p10-only vs full was not statistically distinguishable (paired CI [-0.706, 0.329] s), so the dominant demonstrated mechanism is overpartitioning correction rather than native skew splitting.
- SQL joins v2 1M: baseline 12.217 s vs p10+Kryo 12.294 s (-0.63%); neutral.
- SQL joins v2 10M: baseline 18.848 s vs p10-only 18.925 s (-0.41%); neutral despite physical spill/shuffle changes.
- ETL is retained as an observed-only regime (two clean runs), not as a repeated experiment; no inferential runtime claim is made from those two observations.

Negative and null results are retained because they directly motivated the evidence-aware abstention policy.

## ML readiness

The publication feature store contains 81 clean, non-synthetic write-only runs representing 16 distinct scenarios and 7 effective configurations. Ten scenarios have repeated measurements. Runtime spans 6.921--21.383 s with an overall pooled mean of 12.030 s and SD of 4.267 s. Cost and OOM are constant at zero, and spill has only three distinct values. The trainer requires at least 20 distinct training scenarios and therefore intentionally abstains from fitting publication prediction models at the current coverage level.

This is a software-safety result rather than a predictive-performance claim. The ML infrastructure, feature store, model registry, anomaly detector, predictors, Bayesian optimizer, feedback alignment, held-out split filter, and scenario-diversity gate are implemented, but the SoftwareX paper does not report unsupported predictive accuracy.

See `artifacts/softwarex_evidence/` for machine-readable summaries.
