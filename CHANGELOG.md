# Changelog

## Unreleased — SoftwareX reviewer-correction candidate

- M2/M3/API reviewer fixes integrated locally; this is **not** the published `v1.0.0` tag.
- Local source candidate: code commit `70f0ecb1a5bd8a3620d140c729ca371b0aa3d41c`; integrity-manifest commit `c75cb04ba19c8d9f2ae09b11be537cb5205c2cee`.
- Local qualification: 52 Python tests and Ruff passed (Python 3.14); Python 3.11 parity and multi-worker applicability are not established by these checks.
- Learning01: 90 comparative Spark runs, 4 ACT (two `policy`, two `static`), 6 ABSTAIN; independent duration-prediction R² = -0.0621. Retrospective integrity audit: 90/90 event-log-derived durations matched, 342/342 Spark jobs succeeded, and 0 mismatches across 1,128 shared configuration properties.
- Historical Table 3 still lacks complete independently reconstructed primary run evidence. No new public release or DOI is claimed.


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
Local dashboard availability-display fix committed at fef33f8 after RC1/RC2. Consolidated Windows Python 3.12.10 qualification: 3 dedicated M3 tests and 61 full-suite tests passed; Ruff and pip check passed. Historical validation and archives remain unchanged. Revised release remains UNRELEASED; no tag or DOI is claimed.
