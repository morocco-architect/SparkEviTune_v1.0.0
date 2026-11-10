## SoftwareX reviewer-correction qualification (local, not a release)

This addendum documents a separate local candidate, not the historical `v1.0.0` benchmark. At the qualified source revision, 52 automated Python tests and Ruff checks passed under Python 3.14. This does not establish parity with the reviewer's Python 3.11 environment or certification of multi-worker deployment.

A separate retrospective Learning01 audit verified the 90 comparison executions against the recorded plan; 90/90 application durations agreed with Spark start/end events, all 342 Spark jobs completed successfully, and 1,128 shared configuration-property comparisons had zero mismatches. These findings are local-run integrity evidence, not a claim of broad predictive generalization. Independent prediction metrics: MAE 8.9637 s, RMSE 11.9878 s, R² -0.0621, across 30 scenario-strategy predictions drawn from ten scenarios.

Original v1.0.0 validation information is retained below as historical evidence.

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


## SoftwareX consolidated local qualification - 2026-11-09 - fef33f8
Commit: fef33f82318228a5e3640c8ae39d37aee310d013. Windows Python 3.12.10; pytest 9.2.0. Dedicated M3 tests: 3 passed. Full regression suite: 61 passed, 1 Starlette/httpx deprecation warning. Ruff, pip check and git diff --check: PASS. Scope: tested Windows scenarios only. General multiprocessing safety, other Python versions and Streamlit rendering are not established. No new scientific workload was executed. PUBLIC_RELEASE=NO_GO.


## SoftwareX Python 3.14 qualification - 2026-11-10 - cf0cbe3
Qualified source: cf0cbe378ff33211e1aebda88d50b08c8ebedddf. Windows Python 3.14.0, pytest 9.1.1. Dedicated M3 tests: 3/3 PASS. Full suite: 61/61 PASS, one Starlette/httpx deprecation warning. Ruff PASS; git diff --check PASS. Scope: one local Windows run. Python 3.10/3.11/3.13, general concurrency safety and Streamlit rendering not established. PUBLIC_RELEASE=NO_GO.
