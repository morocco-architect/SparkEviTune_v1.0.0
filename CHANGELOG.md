# Changelog

## v1.0.0 - 2026-08-13

First publication-ready release under the **SparkEviTune** name.

- harmonized software/paper authorship: Moulay Youssef ICHAHANE, Ahmed Dourhri, Mohamed Hanine;
- renamed distribution, Python package, CLI commands, UI labels, and environment-variable prefix;
- documented Spark 3.5.7 effective defaults including `coalescePartitions.parallelismFirst=true`;
- canonicalized `skewJoin=true` to inactive/false whenever AQE is disabled;
- clarified that AQE advice is capability-level, not an isolated benchmark speedup claim;
- retained evidence-aware partition, serializer, skew, and local-memory abstention policies;
- retained scenario-diversity gating that prevents publication-model fitting at 16 scenarios;
- preserved the write-only benchmark protocol and reproducibility evidence.
