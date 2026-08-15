from __future__ import annotations

from copy import deepcopy
from typing import Any

from .models import ClusterProfile, ValidationResult
from .utils import parse_memory_gb


class ConstraintValidator:
    """Deterministic guardrail for candidate Spark configurations.

    The validator may repair bounded numeric violations. Parse errors and
    unsupported/secret-bearing parameters make the candidate invalid.
    """

    FORBIDDEN_PARAMETERS = {  # noqa: RUF012
        "spark.authenticate.secret",
        "spark.hadoop.fs.s3a.secret.key",
        "spark.hadoop.fs.azure.account.key",
    }
    SENSITIVE_TOKENS = ("password", "secret", "token", "access.key", "api.key", "credential")
    ALLOWED_PREFIXES = (
        "spark.executor.",
        "spark.driver.",
        "spark.sql.",
        "spark.serializer",
        "spark.memory.",
        "spark.default.parallelism",
    )

    def validate(
        self,
        base_config: dict[str, str],
        candidate: dict[str, Any],
        cluster: ClusterProfile,
    ) -> ValidationResult:
        merged: dict[str, Any] = deepcopy(base_config)
        violations: list[str] = []
        adjustments: list[str] = []

        # Candidate fields are explicitly inspected before merge. A secret or
        # unsupported key is removed and recorded as a violation.
        for parameter, value in candidate.items():
            lowered = parameter.lower()
            if parameter in self.FORBIDDEN_PARAMETERS or any(token in lowered for token in self.SENSITIVE_TOKENS):
                adjustments.append(f"Removed sensitive/forbidden parameter: {parameter}")
                violations.append(f"Forbidden parameter proposed: {parameter}")
                continue
            if not parameter.startswith(self.ALLOWED_PREFIXES):
                violations.append(f"Unsupported parameter proposed: {parameter}")
                continue
            merged[parameter] = value

        # Remove pre-existing sensitive fields from the returned configuration,
        # but do not attribute them to the candidate unless they were proposed.
        for parameter in list(merged):
            lowered = parameter.lower()
            if parameter in self.FORBIDDEN_PARAMETERS or any(token in lowered for token in self.SENSITIVE_TOKENS):
                merged.pop(parameter, None)
                adjustments.append(f"Removed sensitive/forbidden parameter: {parameter}")

        try:
            heap_gb = parse_memory_gb(merged.get("spark.executor.memory", "1g"))
        except (TypeError, ValueError):
            heap_gb = 1.0
            merged["spark.executor.memory"] = "1g"
            violations.append("spark.executor.memory is not parseable.")

        overhead_value = merged.get("spark.executor.memoryOverhead")
        if overhead_value is None:
            overhead_gb = max(0.384, heap_gb * 0.10)
        else:
            try:
                overhead_gb = parse_memory_gb(overhead_value)
            except (TypeError, ValueError):
                overhead_gb = max(0.384, heap_gb * 0.10)
                merged["spark.executor.memoryOverhead"] = f"{round(overhead_gb * 1024)}m"
                violations.append("spark.executor.memoryOverhead is not parseable.")

        usable_worker_memory = cluster.memory_per_worker_gb * 0.90
        if heap_gb + overhead_gb > usable_worker_memory:
            max_heap = max(1.0, usable_worker_memory - overhead_gb)
            adjusted_heap = max(1, int(max_heap))
            merged["spark.executor.memory"] = f"{adjusted_heap}g"
            heap_gb = float(adjusted_heap)
            adjustments.append(
                f"Reduced executor heap so heap+overhead fits within 90% of a "
                f"{cluster.memory_per_worker_gb:.2f} GB worker."
            )

        try:
            cores = int(merged.get("spark.executor.cores", 1))
        except (TypeError, ValueError):
            cores = 1
            merged["spark.executor.cores"] = 1
            violations.append("spark.executor.cores is not an integer.")
        if cores > cluster.cores_per_worker:
            merged["spark.executor.cores"] = cluster.cores_per_worker
            cores = cluster.cores_per_worker
            adjustments.append("Reduced executor cores to the worker limit.")
        if cores < 1:
            merged["spark.executor.cores"] = 1
            cores = 1
            adjustments.append("Raised executor cores to 1.")

        try:
            instances = int(merged.get("spark.executor.instances", cluster.workers))
        except (TypeError, ValueError):
            instances = cluster.workers
            merged["spark.executor.instances"] = instances
            violations.append("spark.executor.instances is not an integer.")
        instances = max(1, instances)
        max_by_cpu = max(1, cluster.total_cores // max(cores, 1))
        max_by_memory = max(1, int((cluster.total_memory_gb * 0.90) // max(heap_gb + overhead_gb, 0.001)))
        max_instances = max(1, min(max_by_cpu, max_by_memory))
        if instances > max_instances:
            merged["spark.executor.instances"] = max_instances
            adjustments.append(
                f"Reduced executor instances from {instances} to {max_instances} to satisfy total CPU/memory limits."
            )
            instances = max_instances
        elif str(merged.get("spark.executor.instances", "")) != str(instances):
            merged["spark.executor.instances"] = instances

        try:
            partitions = int(merged.get("spark.sql.shuffle.partitions", 200))
        except (TypeError, ValueError):
            partitions = 200
            merged["spark.sql.shuffle.partitions"] = partitions
            violations.append("spark.sql.shuffle.partitions is not an integer.")
        bounded_partitions = min(20000, max(2, partitions))
        if bounded_partitions != partitions:
            merged["spark.sql.shuffle.partitions"] = bounded_partitions
            adjustments.append(f"Clamped shuffle partitions to {bounded_partitions}.")

        try:
            memory_fraction = float(merged.get("spark.memory.fraction", 0.6))
        except (TypeError, ValueError):
            memory_fraction = 0.6
            merged["spark.memory.fraction"] = memory_fraction
            violations.append("spark.memory.fraction is not numeric.")
        bounded_fraction = min(0.8, max(0.4, memory_fraction))
        if bounded_fraction != memory_fraction:
            merged["spark.memory.fraction"] = bounded_fraction
            adjustments.append(f"Clamped spark.memory.fraction to {bounded_fraction}.")

        for boolean_parameter in (
            "spark.sql.adaptive.enabled",
            "spark.sql.adaptive.skewJoin.enabled",
            "spark.sql.adaptive.coalescePartitions.enabled",
        ):
            if boolean_parameter not in merged:
                continue
            normalized = str(merged[boolean_parameter]).lower()
            if normalized not in {"true", "false"}:
                violations.append(f"{boolean_parameter} must be true or false.")

        return ValidationResult(
            valid=not violations,
            configuration=merged,
            violations=violations,
            adjustments=adjustments,
        )
