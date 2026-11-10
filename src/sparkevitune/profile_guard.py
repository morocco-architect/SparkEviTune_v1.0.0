"""Conservative compatibility guard for recorded local Spark event logs.

This checks observed local executor *thread count*, not remote-cluster equivalence.
Non-local masters are not certified compatible by this function.
"""

from __future__ import annotations

import re

from .models import ClusterProfile


def check_profile_compatibility(spark_master: str, cluster: ClusterProfile) -> tuple[bool, str]:
    master = str(spark_master or "").strip()
    normalized = master.lower()
    if not normalized.startswith("local"):
        return False, ""  # Not assessed: remote masters require independent provenance checks.

    match = re.fullmatch(r"local\[(\d+)\]", normalized)
    if match is None:
        return True, (
            f"Observed Spark master {master!r} cannot be mapped to an exact local "
            "thread count by this guard. Partition recommendations based on the "
            "supplied target profile are withheld."
        )

    threads = int(match.group(1))
    if threads < 1:
        return True, f"Invalid local Spark thread count in {master!r}; partition recommendation withheld."
    if cluster.workers == 1 and cluster.cores_per_worker == threads:
        return False, ""

    return True, (
        f"Cluster profile mismatch: event log uses {master} ({threads} local "
        f"threads), but the supplied profile declares {cluster.workers} worker(s) "
        f"with {cluster.cores_per_worker} cores each. Partition recommendations "
        "based on this target profile are withheld; use a separately qualified "
        "target-cluster simulation for hypothetical sizing."
    )
