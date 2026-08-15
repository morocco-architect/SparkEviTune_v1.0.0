from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import numpy as np


class Priority(str, Enum):
    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


PRIORITY_PENALTY = {
    Priority.CRITICAL: 25,
    Priority.HIGH: 15,
    Priority.MEDIUM: 8,
    Priority.LOW: 3,
}

CONFIG_KEYS = [
    "spark.executor.memory",
    "spark.executor.memoryOverhead",
    "spark.executor.cores",
    "spark.executor.instances",
    "spark.driver.memory",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes",
    "spark.sql.autoBroadcastJoinThreshold",
    "spark.serializer",
    "spark.memory.fraction",
    "spark.executor.extraJavaOptions",
    "spark.version",
]


@dataclass
class StageProfile:
    stage_id: int = 0
    stage_attempt_id: int = 0
    successful: bool = True
    num_tasks: int = 0
    duration_ms: int = 0
    total_memory_spill: int = 0
    total_disk_spill: int = 0
    total_shuffle_write: int = 0
    total_successful_shuffle_write: int = 0
    total_shuffle_read: int = 0
    total_gc_time: int = 0
    task_durations: list[int] = field(default_factory=list)
    skew_ratio: float = 1.0
    gc_ratio: float = 0.0

    def compute(self) -> None:
        if self.task_durations:
            median = float(np.median(self.task_durations))
            self.skew_ratio = round(max(self.task_durations) / max(median, 1.0), 3)
        total_task_time = sum(self.task_durations) or 1
        self.gc_ratio = round(self.total_gc_time / total_task_time, 4)


@dataclass
class AppProfile:
    app_id: str = ""
    app_name: str = ""
    duration_ms: int = 0
    spark_config: dict[str, str] = field(default_factory=dict)
    stages: list[StageProfile] = field(default_factory=list)
    total_memory_spill_bytes: int = 0
    total_disk_spill_bytes: int = 0
    total_shuffle_write_bytes: int = 0
    total_shuffle_read_bytes: int = 0
    reference_shuffle_write_bytes: int = 0
    target_partition_size_bytes: int = 64 * 1024**2
    recommended_shuffle_partitions: int = 10
    total_memory_spill_gb: float = 0.0
    total_disk_spill_gb: float = 0.0
    total_shuffle_write_gb: float = 0.0
    total_shuffle_read_gb: float = 0.0
    total_gc_time_s: float = 0.0
    max_skew_ratio: float = 1.0
    avg_gc_ratio: float = 0.0
    num_stages: int = 0
    num_tasks: int = 0
    symptoms: dict[str, bool] = field(default_factory=dict)
    symptom_details: dict[str, str] = field(default_factory=dict)
    parse_warnings: list[str] = field(default_factory=list)

    def aggregate(self) -> None:
        if not self.stages:
            return
        gib = float(1024**3)
        self.total_memory_spill_bytes = sum(s.total_memory_spill for s in self.stages)
        self.total_disk_spill_bytes = sum(s.total_disk_spill for s in self.stages)
        self.total_shuffle_write_bytes = sum(s.total_shuffle_write for s in self.stages)
        self.total_shuffle_read_bytes = sum(s.total_shuffle_read for s in self.stages)
        self.reference_shuffle_write_bytes = max(
            (s.total_successful_shuffle_write for s in self.stages if s.successful),
            default=0,
        )
        self.total_memory_spill_gb = round(self.total_memory_spill_bytes / gib, 6)
        self.total_disk_spill_gb = round(self.total_disk_spill_bytes / gib, 6)
        self.total_shuffle_write_gb = round(self.total_shuffle_write_bytes / gib, 6)
        self.total_shuffle_read_gb = round(self.total_shuffle_read_bytes / gib, 6)
        self.total_gc_time_s = round(sum(s.total_gc_time for s in self.stages) / 1000.0, 3)
        self.max_skew_ratio = round(max((s.skew_ratio for s in self.stages), default=1.0), 3)
        task_time = sum(sum(s.task_durations) for s in self.stages)
        self.avg_gc_ratio = round(sum(s.total_gc_time for s in self.stages) / max(task_time, 1), 4)
        self.num_stages = len(self.stages)
        self.num_tasks = sum(s.num_tasks for s in self.stages)


@dataclass
class ClusterProfile:
    workers: int = 1
    cores_per_worker: int = 4
    memory_per_worker_gb: float = 4.0
    spark_version: str = "3.5.0"
    deployment_mode: str = "standalone"
    cost_per_core_hour: float = 0.0
    cost_per_gb_hour: float = 0.0

    @property
    def total_cores(self) -> int:
        return self.workers * self.cores_per_worker

    @property
    def total_memory_gb(self) -> float:
        return self.workers * self.memory_per_worker_gb


@dataclass
class WorkloadProfile:
    workload_type: str = "unknown"
    input_size_gb: float = 0.0
    input_rows: int = 0
    num_joins: int = 0
    num_aggregations: int = 0
    streaming: bool = False


@dataclass
class Recommendation:
    source: str
    symptom: str
    priority: str
    parameter: str
    current_value: str
    recommended_value: str
    explanation: str
    expected_gain: str = "Workload dependent; validate by re-running the job."
    confidence: float = 1.0
    evidence: list[str] = field(default_factory=list)


@dataclass
class Prediction:
    available: bool = False
    duration_s: float | None = None
    memory_spill_gb: float | None = None
    cost: float | None = None
    oom_risk: float | None = None
    uncertainty: dict[str, float] = field(default_factory=dict)
    model_versions: dict[str, str] = field(default_factory=dict)
    warning: str = ""


@dataclass
class AnomalyResult:
    available: bool = False
    is_anomaly: bool = False
    score: float = 0.0
    explanation: str = ""
    model_version: str = ""


@dataclass
class CandidateConfiguration:
    values: dict[str, Any] = field(default_factory=dict)
    objective: float | None = None
    prediction: Prediction = field(default_factory=Prediction)
    method: str = "none"


@dataclass
class ValidationResult:
    valid: bool
    configuration: dict[str, Any]
    violations: list[str] = field(default_factory=list)
    adjustments: list[str] = field(default_factory=list)


@dataclass
class RuleReport:
    app_id: str
    app_name: str
    duration_s: float
    rule_compliance_score: int
    spark_config: dict[str, str]
    symptoms: dict[str, bool]
    symptom_details: dict[str, str]
    recommendations: list[Recommendation]
    metrics: dict[str, float]
    stages: list[dict[str, Any]]
    parse_warnings: list[str] = field(default_factory=list)


@dataclass
class HybridReport:
    run_id: str
    rule_report: RuleReport
    cluster_profile: ClusterProfile
    workload_profile: WorkloadProfile
    features: dict[str, float]
    anomaly: AnomalyResult
    baseline_prediction: Prediction
    optimized_candidate: CandidateConfiguration | None
    fused_recommendations: list[Recommendation]
    validation: ValidationResult
    explanation: str = ""
    audit: dict[str, Any] = field(default_factory=dict)
