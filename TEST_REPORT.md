# Validation report - v1.0.0 SoftwareX candidate

Date: 2026-08-13

## Automated validation

- Python byte-compilation: passed for the current source, tests and benchmark runner.
- Pytest package suite: **47 tests passed**.
- The test suite includes Spark event-log parsing, effective Spark 3.5.7 defaults, shuffle sizing, AQE-aware partition policy, evidence-aware serializer and skew semantics, local-mode memory policy, feedback feature alignment, workload/input-row features, conditional AQE/skewJoin optimizer dimensions, local optimizer constraints, ML leakage guards, scenario-diversity training gates, validation and LLM-security paths.

## Publication benchmark evidence

The final benchmark methodology executes exactly one Parquet write action per run. Runs from the earlier double-action protocol are excluded from the publication corpus.

- Raw `metrics.json` inventory: 138 runs.
- Clean write-only publication candidates: 81.
- Legacy/double-action runs excluded: 57.
- Distinct publication scenarios: 16.
- Distinct effective configurations: 7.

Repeated rules-only evaluations:

- Heavy Shuffle 1M: 26.41% mean runtime improvement for the validated p10+Kryo treatment; all six paired gains positive; 95% paired CI [2.365, 4.012] s; t-test p=0.00017464; Wilcoxon p=0.03125. Task count fell from 807 to 47.
- Heavy serializer ablation: no statistically detectable Kryo benefit (paired CI crosses zero; t-test p=0.07924).
- Skew stress 1M: p10-only improved mean runtime by 37.26%; the full treatment improved it by 35.69%; p10-only versus full was not statistically distinguishable. The demonstrated mechanism is overpartitioning correction, not native skew splitting.
- SQL joins v2 1M: p10+Kryo was neutral (-0.63%, p=0.824371).
- SQL joins v2 10M: p10-only was neutral (-0.41%, p=0.662767) despite changes in spill/shuffle metrics.

## ML readiness gate

The clean feature store contains 81 real non-synthetic runs but only 16 distinct scenarios. The trainer requires at least 20 distinct scenarios and therefore intentionally returns no publication prediction model. Cost and OOM targets are constant at zero; spill has only three distinct values. The SoftwareX manuscript therefore reports the ML layer as implemented software infrastructure with a data-readiness abstention, not as a validated predictive-performance contribution.

## Scope limitation

The current repeated benchmark is executed with Spark 3.5.7 in WSL Ubuntu using `local[4]`. Multi-worker and cloud-cost generalization are outside the empirical scope of this SoftwareX release. No configuration is automatically applied.
