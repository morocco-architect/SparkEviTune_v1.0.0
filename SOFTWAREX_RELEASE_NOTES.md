# SparkEviTune v1.0.0 - SoftwareX release notes

SparkEviTune is the publication-ready release of an evidence-aware, human-controlled Apache Spark configuration advisor. The deterministic path remains available without ML; optional models are gated by scenario coverage and cannot bypass validation.

## Scientific scope

The included evidence was generated with Spark 3.5.7 under WSL Ubuntu in `local[4]`. It supports workload-specific claims about overpartitioning and explicit abstention; it does not establish multi-worker executor sizing, cloud-cost prediction, OOM calibration, native skew-splitting benefit, or general predictive-ML accuracy.

## Release integrity

Before journal submission, publish this exact source as GitHub release `v1.0.0`, archive it in Zenodo (or an equivalent repository), and insert the resulting immutable release URL/DOI in the manuscript and `CITATION.cff`.
