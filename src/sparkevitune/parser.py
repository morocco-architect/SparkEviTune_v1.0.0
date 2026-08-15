from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from .models import AppProfile, CONFIG_KEYS, StageProfile


_SPARK_35_EFFECTIVE_DEFAULTS = {
    "spark.executor.memory": "1g",
    "spark.driver.memory": "1g",
    "spark.sql.shuffle.partitions": "200",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.parallelismFirst": "true",
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": "67108864b",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.skewJoin.skewedPartitionFactor": "5.0",
    "spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes": "268435456b",
    "spark.sql.autoBroadcastJoinThreshold": "10485760b",
    "spark.serializer": "org.apache.spark.serializer.JavaSerializer",
    "spark.memory.fraction": "0.6",
}


class InvalidSparkLogError(ValueError):
    pass


def _mapping(value: Any) -> dict[str, Any]:
    """Return a mapping for common Spark encodings, otherwise an empty mapping.

    Some log collectors serialize nested Spark event objects as JSON strings.
    Accept that representation without allowing a scalar value to reach `.get()`.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _event_mapping(value: Any) -> dict[str, Any] | None:
    """Normalize a decoded JSON line to a Spark event mapping.

    A JSON line may itself be a quoted JSON object when exported through an
    intermediate collector. Scalar JSON values are rejected instead of raising
    an AttributeError such as ``'str' object has no attribute 'get'``.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
        return decoded if isinstance(decoded, dict) else None
    return None


def _task_succeeded(reason: Any) -> bool:
    """Return True for successful or unspecified task-end reasons.

    Spark event-log encodings vary slightly across versions and listeners. The
    parser accepts the common string and object forms. An absent reason is kept
    for backward compatibility with minimal/synthetic fixtures.
    """
    if reason in (None, "", {}):
        return True
    if isinstance(reason, str):
        return reason.strip().lower() == "success"
    if isinstance(reason, dict):
        label = reason.get("Reason") or reason.get("reason") or reason.get("Class Name")
        return str(label or "").strip().lower() == "success"
    return False


class SparkLogParser:
    """Streaming parser for Spark JSON-lines event logs."""

    _REPORT_PREFLIGHT_LIMIT = 10 * 1024 * 1024

    @classmethod
    def _reject_sparkevitune_report(cls, path: Path) -> None:
        """Give a precise error when the dashboard's output report is re-uploaded.

        SparkEviTune reports are ordinary pretty-printed JSON documents, whereas a
        Spark event log is a JSON-lines stream containing ``SparkListener...``
        events. This preflight is intentionally limited to small files so large
        event logs remain streaming-only.
        """
        try:
            if path.stat().st_size > cls._REPORT_PREFLIGHT_LIMIT:
                return
            document = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        except (OSError, json.JSONDecodeError, UnicodeError):
            return
        if isinstance(document, dict) and {"run_id", "rule_report"}.issubset(document):
            raise InvalidSparkLogError(
                "This file is a SparkEviTune analysis report, not a raw Spark event log. "
                "Upload the original Spark event-log file containing "
                "SparkListenerApplicationStart / SparkListenerEnvironmentUpdate events."
            )

    def parse(self, filepath: str | Path) -> AppProfile:
        path = Path(filepath)
        self._reject_sparkevitune_report(path)

        profile = AppProfile()
        stages: dict[tuple[int, int], StageProfile] = {}
        start_ms: int | None = None
        seen_start = seen_env = seen_task = False
        total = malformed = non_object = 0

        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                line = raw.strip()
                if not line:
                    continue
                total += 1
                try:
                    decoded = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue

                event = _event_mapping(decoded)
                if event is None:
                    non_object += 1
                    continue

                event_type = str(event.get("Event", ""))
                if event_type == "SparkListenerLogStart":
                    spark_version = event.get("Spark Version")
                    if spark_version:
                        profile.spark_config["spark.version"] = str(spark_version)
                elif event_type == "SparkListenerApplicationStart":
                    seen_start = True
                    profile.app_id = str(event.get("App ID", ""))
                    profile.app_name = str(event.get("App Name", ""))
                    start_ms = int(event.get("Timestamp", 0) or 0)
                elif event_type == "SparkListenerEnvironmentUpdate":
                    seen_env = True
                    props = _mapping(event.get("Spark Properties", {}))
                    profile.spark_config.update({k: str(props[k]) for k in CONFIG_KEYS if k in props})

                    # Preserve execution context needed for deployment-aware policy.
                    for execution_key in ("spark.master", "spark.submit.deployMode"):
                        if execution_key in props:
                            profile.spark_config[execution_key] = str(props[execution_key])

                    runtime = _mapping(event.get("JVM Information", {}))
                    if "Java Version" in runtime:
                        profile.spark_config["java.version"] = str(runtime["Java Version"])
                elif event_type == "SparkListenerApplicationEnd":
                    end_ms = int(event.get("Timestamp", 0) or 0)
                    if start_ms:
                        profile.duration_ms = max(0, end_ms - start_ms)
                elif event_type == "SparkListenerTaskEnd":
                    seen_task = True
                    stage_id = int(event.get("Stage ID", 0) or 0)
                    attempt_id = int(event.get("Stage Attempt ID", 0) or 0)
                    key = (stage_id, attempt_id)
                    stage = stages.setdefault(
                        key,
                        StageProfile(stage_id=stage_id, stage_attempt_id=attempt_id),
                    )
                    stage.num_tasks += 1
                    task_info = _mapping(event.get("Task Info", {}))
                    metrics = _mapping(event.get("Task Metrics", {}))
                    duration = int(task_info.get("Duration", 0) or 0)
                    if duration <= 0:
                        duration = max(
                            0,
                            int(task_info.get("Finish Time", 0) or 0)
                            - int(task_info.get("Launch Time", 0) or 0),
                        )
                    stage.task_durations.append(duration)
                    stage.total_memory_spill += int(metrics.get("Memory Bytes Spilled", 0) or 0)
                    stage.total_disk_spill += int(metrics.get("Disk Bytes Spilled", 0) or 0)
                    stage.total_gc_time += int(metrics.get("GC Time", 0) or 0)
                    write = _mapping(metrics.get("Shuffle Write Metrics", {}))
                    read = _mapping(metrics.get("Shuffle Read Metrics", {}))
                    written = int(write.get("Shuffle Bytes Written", 0) or 0)
                    stage.total_shuffle_write += written
                    if _task_succeeded(event.get("Task End Reason")):
                        stage.total_successful_shuffle_write += written
                    stage.total_shuffle_read += int(read.get("Remote Bytes Read", 0) or 0)
                    stage.total_shuffle_read += int(read.get("Local Bytes Read", 0) or 0)
                elif event_type == "SparkListenerStageCompleted":
                    info = _mapping(event.get("Stage Info", {}))
                    stage_id = int(info.get("Stage ID", 0) or 0)
                    attempt_id = int(info.get("Stage Attempt ID", 0) or 0)
                    key = (stage_id, attempt_id)
                    stage = stages.setdefault(
                        key,
                        StageProfile(stage_id=stage_id, stage_attempt_id=attempt_id),
                    )
                    stage.successful = not bool(info.get("Failure Reason"))
                    stage.duration_ms = int(info.get("Stage Duration", 0) or 0)
                    if stage.duration_ms <= 0:
                        stage.duration_ms = max(
                            0,
                            int(info.get("Completion Time", 0) or 0)
                            - int(info.get("Submission Time", 0) or 0),
                        )

        if total == 0:
            raise InvalidSparkLogError("The event log is empty.")
        if not seen_start and not seen_env:
            detail = ""
            if non_object:
                detail = f" {non_object} JSON line(s) decoded to scalar values rather than event objects."
            raise InvalidSparkLogError(
                "No SparkListenerApplicationStart or SparkListenerEnvironmentUpdate event was found."
                + detail
                + " Upload the original raw Spark event log, not a SparkEviTune JSON report."
            )
        invalid_lines = malformed + non_object
        if invalid_lines >= max(1, total // 2):
            raise InvalidSparkLogError(
                f"Too many invalid event-log lines: {invalid_lines}/{total} "
                f"({malformed} malformed JSON, {non_object} non-object JSON)."
            )
        if malformed:
            profile.parse_warnings.append(f"Skipped {malformed}/{total} malformed JSON lines.")
        if non_object:
            profile.parse_warnings.append(f"Skipped {non_object}/{total} non-object JSON lines.")
        if not seen_env:
            profile.parse_warnings.append(
                "The effective Spark configuration was not found; configuration-dependent rules may use defaults."
            )
        if not seen_task:
            profile.parse_warnings.append(
                "No task events were found; task-level spill, skew, shuffle and GC metrics are unavailable."
            )

        spark_version = str(profile.spark_config.get("spark.version", ""))
        if spark_version.startswith("3.5"):
            for key, value in _SPARK_35_EFFECTIVE_DEFAULTS.items():
                profile.spark_config.setdefault(key, value)

        for stage in stages.values():
            stage.compute()
        profile.stages = sorted(
            stages.values(),
            key=lambda item: (item.stage_id, item.stage_attempt_id),
        )
        profile.aggregate()
        if not profile.app_id:
            digest = hashlib.sha1(str(path.resolve()).encode()).hexdigest()[:10]
            profile.app_id = f"unknown-{path.name}-{digest}"
        if not profile.app_name:
            profile.app_name = path.stem
        return profile
