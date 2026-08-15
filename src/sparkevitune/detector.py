from __future__ import annotations

from .models import AppProfile, ClusterProfile, WorkloadProfile
from .utils import (
    BYTES_PER_GIB,
    BYTES_PER_MIB,
    DEFAULT_ADVISORY_PARTITION_BYTES,
    recommended_shuffle_partitions,
    safe_memory_gb,
    safe_size_bytes,
)


class SymptomDetector:
    """Evidence-aware deterministic safety net.

    Symptoms are observations. The recommendation layer decides whether an
    observation is actionable in the current Spark execution context.
    """

    SPILL_THRESHOLD_GB = 0.1
    SKEW_RATIO_THRESHOLD = 3.0
    GC_RATIO_THRESHOLD = 0.10
    EXECUTOR_MEMORY_MIN_GB = 2.0
    DRIVER_MEMORY_MIN_GB = 1.0

    def detect(
        self,
        profile: AppProfile,
        cluster: ClusterProfile,
        workload: WorkloadProfile,
    ) -> AppProfile:
        del workload  # Reserved for future workload-conditioned rules.
        config = profile.spark_config
        warnings = profile.parse_warnings
        symptoms: dict[str, bool] = {}
        details: dict[str, str] = {}

        spark_master = str(config.get("spark.master", "")).strip()
        local_execution = spark_master.lower().startswith("local")

        symptoms["spilling"] = profile.total_memory_spill_gb > self.SPILL_THRESHOLD_GB
        if symptoms["spilling"]:
            details["spilling"] = (
                f"Observed {profile.total_memory_spill_gb:.3f} GiB of memory spill "
                f"and {profile.total_disk_spill_gb:.3f} GiB of disk spill."
            )
            if local_execution:
                details["spilling"] += (
                    f" Spark master is {spark_master}; executor-memory escalation "
                    "is therefore not inferred from this local run."
                )

        executor_gb = safe_memory_gb(
            config.get("spark.executor.memory", "1g"),
            1.0,
            warnings,
            "executor.memory",
        )
        executor_below_guideline = executor_gb < self.EXECUTOR_MEMORY_MIN_GB
        runtime_memory_pressure = symptoms["spilling"] or profile.avg_gc_ratio > self.GC_RATIO_THRESHOLD
        symptoms["low_executor_memory"] = (
            executor_below_guideline
            and runtime_memory_pressure
            and not local_execution
        )
        if symptoms["low_executor_memory"]:
            details["low_executor_memory"] = (
                f"Executor heap is {executor_gb:.2f} GiB and runtime memory pressure was observed."
            )

        driver_gb = safe_memory_gb(
            config.get("spark.driver.memory", "1g"),
            1.0,
            warnings,
            "driver.memory",
        )
        symptoms["low_driver_memory"] = driver_gb < self.DRIVER_MEMORY_MIN_GB
        if symptoms["low_driver_memory"]:
            details["low_driver_memory"] = f"Driver heap is {driver_gb:.2f} GiB."

        aqe = str(config.get("spark.sql.adaptive.enabled", "false")).lower() == "true"
        coalesce_partitions = (
            str(config.get("spark.sql.adaptive.coalescePartitions.enabled", "false")).lower() == "true"
        )
        parallelism_first = (
            str(config.get("spark.sql.adaptive.coalescePartitions.parallelismFirst", "true")).lower() == "true"
        )

        symptoms["skew"] = profile.max_skew_ratio > self.SKEW_RATIO_THRESHOLD
        if symptoms["skew"]:
            details["skew"] = (
                f"Maximum task-duration skew is {profile.max_skew_ratio:.2f}x. "
                "This is an execution-time imbalance signal, not direct evidence "
                "of skewed join-partition bytes. AQE skew-join tuning therefore "
                "requires additional partition-level or physical-plan evidence."
            )

        total_shuffle_bytes = profile.total_shuffle_read_bytes + profile.total_shuffle_write_bytes
        total_shuffle_gib = total_shuffle_bytes / BYTES_PER_GIB
        symptoms["shuffle_heavy"] = total_shuffle_bytes > BYTES_PER_GIB
        if symptoms["shuffle_heavy"]:
            details["shuffle_heavy"] = (
                f"Descriptive cumulative shuffle read+write is {total_shuffle_gib:.3f} GiB. "
                "This metric is not used for partition sizing."
            )

        try:
            current_partitions = int(config.get("spark.sql.shuffle.partitions", "200"))
        except (TypeError, ValueError):
            current_partitions = 200
            warnings.append("Invalid spark.sql.shuffle.partitions; using 200 for diagnosis.")

        target_bytes = safe_size_bytes(
            config.get(
                "spark.sql.adaptive.advisoryPartitionSizeInBytes",
                str(DEFAULT_ADVISORY_PARTITION_BYTES),
            ),
            DEFAULT_ADVISORY_PARTITION_BYTES,
            warnings,
            "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        )
        minimum_partitions = max(10, cluster.total_cores * 2)
        target_partitions = recommended_shuffle_partitions(
            profile.reference_shuffle_write_bytes,
            target_bytes,
            minimum_partitions,
        )
        profile.target_partition_size_bytes = target_bytes
        profile.recommended_shuffle_partitions = target_partitions

        static_too_many = current_partitions > target_partitions * 2
        static_too_few = current_partitions < max(2, target_partitions // 2)
        static_mismatch = profile.reference_shuffle_write_bytes > 0 and (static_too_many or static_too_few)
        max_stage_tasks = max(
            (stage.num_tasks for stage in profile.stages if stage.successful),
            default=0,
        )
        observed_overpartitioning = max_stage_tasks > target_partitions * 2

        if static_too_many and aqe and coalesce_partitions:
            symptoms["bad_partitions"] = static_mismatch and observed_overpartitioning
        else:
            symptoms["bad_partitions"] = static_mismatch

        if symptoms["bad_partitions"]:
            details["bad_partitions"] = (
                f"Configured {current_partitions} shuffle partitions; the byte-derived baseline is "
                f"{target_partitions}, using the maximum successful-stage shuffle write "
                f"({profile.reference_shuffle_write_bytes / BYTES_PER_GIB:.3f} GiB), a target of "
                f"{target_bytes / BYTES_PER_MIB:.1f} MiB, minimum parallelism {minimum_partitions}, "
                f"and maximum observed successful-stage task count {max_stage_tasks}."
            )
            if aqe and coalesce_partitions and parallelism_first:
                details["bad_partitions"] += (
                    " Spark's coalescePartitions.parallelismFirst=true means the advisory size is only a "
                    "heuristic starting reference; AQE may choose a smaller target from cluster parallelism."
                )
            if static_too_many and aqe and coalesce_partitions:
                details["bad_partitions"] += (
                    " AQE coalescing is enabled, but substantial executed-task overpartitioning remains."
                )

        symptoms["gc_pressure"] = profile.avg_gc_ratio > self.GC_RATIO_THRESHOLD
        if symptoms["gc_pressure"]:
            details["gc_pressure"] = f"GC consumed {profile.avg_gc_ratio * 100:.1f}% of task time."

        serializer = str(config.get("spark.serializer", ""))
        symptoms["java_serializer"] = not serializer or "JavaSerializer" in serializer
        if symptoms["java_serializer"]:
            details["java_serializer"] = (
                "Java serialization is active. It is retained as diagnostic context only; "
                "the rules layer does not recommend Kryo without workload-specific evidence."
            )

        symptoms["aqe_disabled"] = not aqe
        if symptoms["aqe_disabled"]:
            details["aqe_disabled"] = "Adaptive Query Execution is disabled."

        profile.symptoms = symptoms
        profile.symptom_details = details
        return profile
