from __future__ import annotations

from .models import (
    AppProfile,
    ClusterProfile,
    Priority,
    PRIORITY_PENALTY,
    Recommendation,
    RuleReport,
)
from .utils import BYTES_PER_GIB, BYTES_PER_MIB, ceil_practical_memory, safe_memory_gb


class RuleEngine:
    def build_report(self, profile: AppProfile, cluster: ClusterProfile) -> RuleReport:
        recommendations: list[Recommendation] = []
        config = profile.spark_config

        spark_master = str(config.get("spark.master", "")).strip()
        local_execution = spark_master.lower().startswith("local")
        executor_memory_actionable = (
            (profile.symptoms.get("low_executor_memory") or profile.symptoms.get("spilling"))
            and not local_execution
        )

        if executor_memory_actionable:
            current = config.get("spark.executor.memory", "1g")
            current_gb = safe_memory_gb(current, 1.0, profile.parse_warnings, "executor.memory")
            required = max(2.0, current_gb + 1.2 * profile.total_memory_spill_gb)
            target = ceil_practical_memory(required)
            recommendations.append(
                Recommendation(
                    source="rule",
                    symptom="executor_memory_pressure",
                    priority=Priority.CRITICAL.value if profile.symptoms.get("spilling") else Priority.HIGH.value,
                    parameter="spark.executor.memory",
                    current_value=current,
                    recommended_value=f"{target}g",
                    explanation=(
                        f"The target is the smallest practical heap not below {required:.2f} GB, "
                        "computed from current heap and observed spill. The constraint validator may lower "
                        "or reject it if cluster memory is insufficient."
                    ),
                    evidence=[f"memory_spill_gb={profile.total_memory_spill_gb:.3f}"],
                )
            )

        if profile.symptoms.get("low_driver_memory"):
            current = config.get("spark.driver.memory", "1g")
            current_gb = safe_memory_gb(current, 1.0, profile.parse_warnings, "driver.memory")
            target = ceil_practical_memory(max(1.0, current_gb * 2.0))
            recommendations.append(
                Recommendation(
                    source="rule",
                    symptom="low_driver_memory",
                    priority=Priority.HIGH.value,
                    parameter="spark.driver.memory",
                    current_value=current,
                    recommended_value=f"{target}g",
                    explanation="Increase driver heap conservatively; validate against deployment limits.",
                )
            )

        if profile.symptoms.get("bad_partitions"):
            reference_gib = profile.reference_shuffle_write_bytes / BYTES_PER_GIB
            target_mib = profile.target_partition_size_bytes / BYTES_PER_MIB
            target = profile.recommended_shuffle_partitions
            recommendations.append(
                Recommendation(
                    source="rule",
                    symptom="bad_partitions",
                    priority=Priority.MEDIUM.value,
                    parameter="spark.sql.shuffle.partitions",
                    current_value=config.get("spark.sql.shuffle.partitions", "200"),
                    recommended_value=str(target),
                    explanation=(
                        f"Use {target} as a starting value. It is computed from the maximum shuffle-write "
                        f"volume of one successful stage attempt ({reference_gib:.3f} GiB), divided by the "
                        f"advisory-size heuristic ({target_mib:.1f} MiB), bounded by cluster parallelism and rounded "
                        "upward. Shuffle reads are not added to writes and different stages are not accumulated. "
                        "With AQE coalescing, parallelismFirst may override this advisory-size reference at runtime."
                    ),
                    evidence=[
                        f"reference_shuffle_write_bytes={profile.reference_shuffle_write_bytes}",
                        f"target_partition_bytes={profile.target_partition_size_bytes}",
                        "aggregation_policy=max_successful_stage_shuffle_write",
                    ],
                )
            )

        # Task-duration skew remains an observed runtime symptom. It does
        # not by itself prove skewed join-partition bytes, so the rules layer
        # does not automatically enable AQE skewJoin.

        if profile.symptoms.get("aqe_disabled"):
            recommendations.append(
                Recommendation(
                    source="rule",
                    symptom="aqe_disabled",
                    priority=Priority.HIGH.value,
                    parameter="spark.sql.adaptive.enabled",
                    current_value=config.get("spark.sql.adaptive.enabled", "false"),
                    recommended_value="true",
                    explanation=(
                        "Enable Spark's adaptive execution capability so runtime statistics can support "
                        "partition coalescing and eligible plan rewrites. This is a capability-level "
                        "recommendation; SparkEviTune does not claim an isolated AQE runtime benefit from "
                        "the current publication benchmark."
                    ),
                )
            )

        # JavaSerializer remains diagnostic context only. The rules layer
        # does not recommend Kryo without workload-specific evidence.

        if profile.symptoms.get("gc_pressure"):
            recommendations.append(
                Recommendation(
                    source="rule",
                    symptom="gc_pressure",
                    priority=Priority.HIGH.value,
                    parameter="spark.executor.extraJavaOptions",
                    current_value=config.get("spark.executor.extraJavaOptions", ""),
                    recommended_value="-XX:+UseG1GC",
                    explanation=(
                        "G1GC is a candidate for large heaps. Treat this as an experiment and compare pause time, "
                        "throughput and total runtime before adopting it."
                    ),
                    evidence=[f"gc_ratio={profile.avg_gc_ratio:.4f}"],
                )
            )

        order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        recommendations.sort(key=lambda rec: order.get(rec.priority, 9))
        score = max(
            0,
            100
            - sum(PRIORITY_PENALTY[Priority(rec.priority)] for rec in recommendations),
        )

        metrics = {
            "duration_s": round(profile.duration_ms / 1000.0, 3),
            "memory_spill_gb": profile.total_memory_spill_gb,
            "disk_spill_gb": profile.total_disk_spill_gb,
            "shuffle_write_gb": profile.total_shuffle_write_gb,
            "shuffle_read_gb": profile.total_shuffle_read_gb,
            "reference_shuffle_write_gb": round(profile.reference_shuffle_write_bytes / BYTES_PER_GIB, 6),
            "target_partition_size_mib": round(profile.target_partition_size_bytes / BYTES_PER_MIB, 3),
            "recommended_shuffle_partitions": float(profile.recommended_shuffle_partitions),
            "gc_time_s": profile.total_gc_time_s,
            "gc_ratio": profile.avg_gc_ratio,
            "max_skew_ratio": profile.max_skew_ratio,
            "num_stages": float(profile.num_stages),
            "num_tasks": float(profile.num_tasks),
        }
        stages = [
            {
                "stage_id": stage.stage_id,
                "stage_attempt_id": stage.stage_attempt_id,
                "successful": stage.successful,
                "num_tasks": stage.num_tasks,
                "duration_ms": stage.duration_ms,
                "memory_spill_gb": stage.total_memory_spill / BYTES_PER_GIB,
                "shuffle_write_gb": stage.total_shuffle_write / BYTES_PER_GIB,
                "successful_shuffle_write_gb": stage.total_successful_shuffle_write / BYTES_PER_GIB,
                "shuffle_read_gb": stage.total_shuffle_read / BYTES_PER_GIB,
                "skew_ratio": stage.skew_ratio,
                "gc_ratio": stage.gc_ratio,
            }
            for stage in profile.stages
        ]
        return RuleReport(
            app_id=profile.app_id,
            app_name=profile.app_name,
            duration_s=metrics["duration_s"],
            rule_compliance_score=score,
            spark_config=config,
            symptoms=profile.symptoms,
            symptom_details=profile.symptom_details,
            recommendations=recommendations,
            metrics=metrics,
            stages=stages,
            parse_warnings=profile.parse_warnings,
        )
