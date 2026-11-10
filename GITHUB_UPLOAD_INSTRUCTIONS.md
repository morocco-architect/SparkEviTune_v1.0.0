# SparkEviTune — GitHub procedure for an UNRELEASED SoftwareX correction branch

This file applies only to `softwarex-major-revision`. The public `main` / `v1.0.0` baseline must remain intact. The Git remote was originally absent in this worktree; verify its current configuration before any approved publication action.

## Pre-publication requirements

1. Verify the final source commit and clean worktree; review Python 3.11 compatibility, M3 publication/concurrency limitations, pending software probes, and all manuscript/figure discrepancies.
2. Verify documentation, tracked assets, dependency declarations, licensing, possible secrets, and the reproducibility package. Pattern-based secret checks are not exhaustive.
3. Obtain explicit permission for a public branch/release. **Do not push, create a tag, change `main`, or mint a DOI as part of documentation cleanup.**
4. Once permission is granted, publish a separately identified corrected version without moving or replacing the historical `v1.0.0` tag; perform an external clean-clone qualification.
5. Archive that exact published revision and update `CITATION.cff`, manuscript metadata and the response-to-reviewers letter with the actual immutable URL and DOI.

## Current evidence

- Local revision branch: `softwarex-major-revision` at `c75cb04ba19c8d9f2ae09b11be537cb5205c2cee` before documentation changes.
- Historical public `v1.0.0` baseline main commit: `53f8332b85de789e17b8ca4a3126fe1ad48daa43`.
- Learning01: 90/90 duration matches; 342/342 successful Spark jobs; 1,128 compared properties with no mismatches. These checks do not establish distributed-cluster effectiveness.
- Historical Table 3 provenance and figure 3 / Table 2 mismatch are outstanding.

AQE01_STATUS=DRAFT_NOT_FROZEN;SMOKE_NOT_AUTHORIZED
LOT_C_EXECUTION_DECISION=NO_GO