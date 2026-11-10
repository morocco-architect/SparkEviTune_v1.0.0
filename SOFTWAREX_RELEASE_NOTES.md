> Status: DRAFT_NOT_FROZEN;SMOKE_NOT_AUTHORIZED. Lot C: NO_GO. No public release or DOI for this revision.

# SparkEviTune — SoftwareX revision candidate (UNRELEASED)

The SoftwareX reviewer-correction branch is a local, unpublished candidate of the evidence-aware, human-controlled Apache Spark configuration advisor. The deterministic path remains available without ML; optional models are gated by scenario coverage and cannot bypass validation.

## Scientific scope

The included evidence was generated with Spark 3.5.7 under WSL Ubuntu in `local[4]`. It supports workload-specific claims about overpartitioning and explicit abstention; it does not establish multi-worker executor sizing, cloud-cost prediction, OOM calibration, native skew-splitting benefit, or general predictive-ML accuracy.

## Release integrity

The historical public `v1.0.0` tag must remain immutable. A separate revised release, archive, and DOI may be created only after final qualification and explicit publication approval; no such revised publication is asserted here.

## RC1 prepublication qualification - 2026-11-09

- Source commit: 4cc6ed42410d6c90cffa5ccb1462bd490f986fc0
- Integrated SoftwareX M2/M3/API and Profile Guard corrections.
- ModelRegistry M3 interprocess access correction.
- Python 3.12: 61/61 regression tests passed.
- Ruff: PASS.
- Git diff check: PASS.
- Windows M3 targeted multiprocessing tests: PASS.
- Source RC1 archive SHA-256:
  988368138895FEC48A85D048AC049DF0E90FD1D4C83D1A75D4DF7C0F7EA757DB.
- A-04: acceptance criterion not identified; unresolved.
- Python 3.11 and remaining scientific release checks: pending.
- Current package version: 1.0.0.
- Proposed candidate label: v1.1.0-rc1 (not a Git tag).
- Public release: NO-GO.
- AQE01_STATUS=DRAFT_NOT_FROZEN;SMOKE_NOT_AUTHORIZED
- LOT_C_EXECUTION_DECISION=NO_GO


## SoftwareX consolidated local qualification - 2026-11-09 - fef33f8
Qualified local commit: fef33f82318228a5e3640c8ae39d37aee310d013. Python 3.12.10 on Windows; M3 targeted 3/3 PASS, global pytest 61/61 PASS with one warning, Ruff PASS, pip check PASS. Streamlit rendering and general concurrency safety not established. RC1/RC2 archives do not represent this HEAD. A-03 LZ4 provenance remains undetermined; historical authorization traceability remains incomplete, without equating it to the separate RC1 A-04 wording. PUBLIC_RELEASE=NO_GO. LOT_C_EXECUTION_DECISION=NO_GO. AQE01_STATUS=DRAFT_NOT_FROZEN;SMOKE_NOT_AUTHORIZED.


## SoftwareX Python 3.14 qualification - 2026-11-10 - cf0cbe3
Windows Python 3.14.0 qualification on source commit cf0cbe378ff33211e1aebda88d50b08c8ebedddf: M3 3/3 PASS, full suite 61/61 PASS, Ruff PASS, git diff check PASS. One Starlette/httpx deprecation warning. Python 3.12.10 qualification remains documented separately. Python 3.10/3.11/3.13, general multiprocessing safety and Streamlit rendering remain unverified. Revised release UNRELEASED. PUBLIC_RELEASE=NO_GO. LOT_C_EXECUTION_DECISION=NO_GO. AQE01_STATUS=DRAFT_NOT_FROZEN;SMOKE_NOT_AUTHORIZED.
