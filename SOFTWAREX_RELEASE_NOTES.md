# SparkEviTune v1.0.1 — SoftwareX corrective release candidate

**Status:** Prepared, not yet published
**Preparation date:** 2026-11-16
**Source branch:** `softwarex-major-revision-from-main`
**Verified source commit:** `7ed7565dbf972d75ced26189026afdfd443f72e3`

## Software improvements

- Added support for workload input-row counts in the FastAPI upload endpoint.
- Integrated Profile Guard functionality and corresponding regression tests.
- Integrated ModelRegistry multiprocessing-related corrections and targeted tests.
- Updated dashboard, detection, pipeline, machine-learning and validation components.

## Verification

- Windows Python 3.12: 61/61 automated tests passed; Ruff and dependency checks passed.
- Windows Python 3.14: 61/61 automated tests passed; Ruff passed.
- The tests do not establish general multiprocessing safety or deployment correctness across all supported environments.
- End-to-end Streamlit–FastAPI verification is not yet confirmed.

## Scientific scope and limitations

- Historical experimental results remain associated with their original software versions and execution environments.
- The independent prediction assessment does not establish reliable generalization.
- The ML publication-readiness gate remains closed because the historical corpus contains 16 distinct scenarios against a threshold of 20.
- Historical Table 3 reconstruction and A-03/A-04 traceability limitations remain documented.
- Multi-worker Spark performance and cloud-cost estimation are not validated.

## Release integrity

- The historical `v1.0.0` release and tag remain unchanged.
- `v1.0.1` is a corrective release candidate, not a new experimental benchmark.
- No revised DOI or public release is claimed.
- Public publication remains subject to explicit approval and resolution or documented acceptance of applicable release restrictions.

---

## Historical qualification records

The sections below preserve the original prepublication qualification reports and their historical GO/NO-GO decisions.


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
