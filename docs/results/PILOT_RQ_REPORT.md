# SparkEviTune RQ1-RQ5 quantitative pilot

This report uses **eight historical real Spark runs** recovered from preserved notebook outputs. It is not the repeated multi-cluster benchmark required for submission.

## RQ1 — Runtime prediction

Extra Trees LOO: MAE **20.34 s**, RMSE **22.47 s**, MAPE **29.4%**, R² **0.445**.  
Median baseline LOO: MAE **23.65 s**. The model beats this baseline.

## RQ2 — Anomaly detection

Isolation Forest: precision **0.667**, recall **0.500**, F1 **0.571**, AP **0.729**.  
Deterministic configuration rules: F1 **1.000**. Labels denote deliberately bad versus optimized configurations, so this comparison is not an unknown-anomaly study.

## RQ3 — Optimization

Real before/after runs improved **3/4** workloads; median speedup **1.983×**, geometric mean **1.718×**. Bayesian optimization remains unvalidated because its candidates were not executed.

## RQ4 — Generalization

Leave-one-workload-out MAE **22.88 s**, MAPE **32.8%**, R² **0.338**. One cluster and one data size cannot establish cross-cluster robustness.

## RQ5 — Constraint validator

Generated candidates: **1100**; unsafe detection **100.0%**; unsafe accepted **0**; false rejection **0.0%**. This is a deterministic generated-candidate study, not a deployed-cluster safety proof.

## Submission decision

The implementation is executable and the historical pilot is quantified, but the real ≥5-repetition, multi-cluster experiment remains mandatory before presenting RQ1–RQ4 as validated findings.
