# GitHub upload instructions — SparkEviTune v1.0.0

Official repository:

https://github.com/morocco-architect/SparkEviTune_v1.0.0

Upload the **contents of this directory directly to the repository root** so that `README.md`, `pyproject.toml`, `src/`, `tests/`, `api/`, `dashboard/`, `benchmarks/`, `scripts/`, `docs/`, and `artifacts/` are visible at the top level.

After the push:

1. Clone the repository into a clean directory.
2. Create a Python environment and install `.[ui,dev]`.
3. Run `pytest -q tests` and confirm all 47 tests pass.
4. Create the immutable GitHub release/tag `v1.0.0` from the verified commit.
5. Archive that exact release in Zenodo (or an equivalent long-term archive).
6. Add the release date and DOI to `CITATION.cff` and the SoftwareX manuscript metadata before submission.

## Package validation

Before this GitHub-ready archive was created, the source was byte-compiled successfully and the test suite completed with **47/47 tests passing**.
