from __future__ import annotations

from .models import AppProfile, ClusterProfile, WorkloadProfile
from .utils import safe_memory_gb

# Features available before a candidate configuration is executed. These are
# used by supervised performance models and deliberately exclude post-run
# targets such as duration, spill and shuffle to avoid target leakage.
PREDICTION_FEATURE_COLUMNS = [
    "executor_memory_gb",
    "executor_cores",
    "executor_instances",
    "driver_memory_gb",
    "shuffle_partitions",
    "aqe_enabled",
    "skew_join_enabled",
    "kryo_enabled",
    "memory_fraction",
    "workers",
    "cores_per_worker",
    "memory_per_worker_gb",
    "input_size_gb",
    "input_rows",
    "workload_etl",
    "workload_sql_joins",
    "workload_heavy_shuffle",
    "workload_skew_join",
    "num_joins",
    "num_aggregations",
    "streaming",
]

# Features used only for post-run anomaly detection and diagnosis.
ANOMALY_FEATURE_COLUMNS = [
    "duration_s",
    "num_stages",
    "num_tasks",
    "memory_spill_gb",
    "disk_spill_gb",
    "shuffle_write_gb",
    "shuffle_read_gb",
    "gc_ratio",
    "max_skew_ratio",
    *PREDICTION_FEATURE_COLUMNS,
]

# Backward-compatible alias used by the feature store and import scripts.
FEATURE_COLUMNS = ANOMALY_FEATURE_COLUMNS

CONFIG_MUTABLE_FEATURES = {
    "spark.executor.memory": "executor_memory_gb",
    "spark.executor.cores": "executor_cores",
    "spark.executor.instances": "executor_instances",
    "spark.driver.memory": "driver_memory_gb",
    "spark.sql.shuffle.partitions": "shuffle_partitions",
    "spark.sql.adaptive.enabled": "aqe_enabled",
    "spark.sql.adaptive.skewJoin.enabled": "skew_join_enabled",
    "spark.serializer": "kryo_enabled",
    "spark.memory.fraction": "memory_fraction",
}


class FeatureBuilder:
    def build(
        self,
        app: AppProfile,
        cluster: ClusterProfile,
        workload: WorkloadProfile,
    ) -> dict[str, float]:
        cfg = app.spark_config
        warnings = app.parse_warnings
        try:
            executor_cores = float(cfg.get("spark.executor.cores", 1))
        except (TypeError, ValueError):
            executor_cores = 1.0
            warnings.append("Invalid spark.executor.cores; using 1.")
        try:
            executor_instances = float(cfg.get("spark.executor.instances", cluster.workers))
        except (TypeError, ValueError):
            executor_instances = float(cluster.workers)
            warnings.append("Invalid spark.executor.instances; using worker count.")
        try:
            partitions = float(cfg.get("spark.sql.shuffle.partitions", 200))
        except (TypeError, ValueError):
            partitions = 200.0
            warnings.append("Invalid spark.sql.shuffle.partitions; using 200.")
        try:
            memory_fraction = float(cfg.get("spark.memory.fraction", 0.6))
        except (TypeError, ValueError):
            memory_fraction = 0.6
            warnings.append("Invalid spark.memory.fraction; using 0.6.")

        workload_type = str(workload.workload_type or "unknown").strip().lower()

        features = {
            "duration_s": app.duration_ms / 1000.0,
            "num_stages": float(app.num_stages),
            "num_tasks": float(app.num_tasks),
            "memory_spill_gb": app.total_memory_spill_gb,
            "disk_spill_gb": app.total_disk_spill_gb,
            "shuffle_write_gb": app.total_shuffle_write_gb,
            "shuffle_read_gb": app.total_shuffle_read_gb,
            "gc_ratio": app.avg_gc_ratio,
            "max_skew_ratio": app.max_skew_ratio,
            "executor_memory_gb": safe_memory_gb(
                cfg.get("spark.executor.memory", "1g"), 1.0, warnings, "executor.memory"
            ),
            "executor_cores": executor_cores,
            "executor_instances": executor_instances,
            "driver_memory_gb": safe_memory_gb(
                cfg.get("spark.driver.memory", "1g"), 1.0, warnings, "driver.memory"
            ),
            "shuffle_partitions": partitions,
            "aqe_enabled": float(str(cfg.get("spark.sql.adaptive.enabled", "false")).lower() == "true"),
            "skew_join_enabled": float(
                str(cfg.get("spark.sql.adaptive.skewJoin.enabled", "false")).lower() == "true"
            ),
            "kryo_enabled": float("KryoSerializer" in str(cfg.get("spark.serializer", ""))),
            "memory_fraction": memory_fraction,
            "workers": float(cluster.workers),
            "cores_per_worker": float(cluster.cores_per_worker),
            "memory_per_worker_gb": cluster.memory_per_worker_gb,
            "input_size_gb": workload.input_size_gb,
            "input_rows": float(workload.input_rows),
            "workload_etl": float(workload_type == "etl"),
            "workload_sql_joins": float(workload_type == "sql_joins"),
            "workload_heavy_shuffle": float(workload_type == "heavy_shuffle"),
            "workload_skew_join": float(workload_type == "skew_join"),
            "num_joins": float(workload.num_joins),
            "num_aggregations": float(workload.num_aggregations),
            "streaming": float(workload.streaming),
        }
        return {name: float(features.get(name, 0.0)) for name in FEATURE_COLUMNS}

    def apply_candidate(self, base: dict[str, float], config: dict[str, object]) -> dict[str, float]:
        result = dict(base)
        for parameter, feature in CONFIG_MUTABLE_FEATURES.items():
            if parameter not in config:
                continue
            value = config[parameter]
            if feature in {"aqe_enabled", "skew_join_enabled"}:
                result[feature] = float(str(value).lower() == "true" or value is True)
            elif feature == "kryo_enabled":
                result[feature] = float("Kryo" in str(value))
            elif "memory_gb" in feature:
                result[feature] = safe_memory_gb(value, result.get(feature, 1.0), [], feature)
            else:
                result[feature] = float(value)
        return result
