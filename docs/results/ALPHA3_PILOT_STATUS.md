# Alpha.3 pilot status

Alpha.3 implements the end-to-end architecture and quantifies what can be supported by the available
historical data. It does **not** claim completion of the required repeated multi-cluster study.

## Evidence available

- Eight real historical Spark runs from four workload families.
- Runtime model trained without post-run target leakage.
- Isolation Forest pilot evaluation.
- Four real before/after workload comparisons.
- Leave-one-workload-out pilot evaluation.
- Deterministic validation study on 1,100 generated candidates.

## Evidence unavailable in this execution environment

The environment used to build this release has Java but no Apache Spark/PySpark distribution and no
Docker daemon or remote Spark cluster. Consequently, it cannot execute the planned 360/720-run real
benchmark. The repository contains the complete runner and manifest for execution on a Spark-enabled
host. Results must not be described as multi-cluster findings until that run is completed.
