from pathlib import Path

from sparkevitune.parser import SparkLogParser


def test_parser_extracts_configuration_and_metrics():
    path = Path(__file__).parent / "fixtures" / "minimal_event_log.jsonl"
    profile = SparkLogParser().parse(path)
    assert profile.app_id == "app-demo-001"
    assert profile.spark_config["spark.executor.memory"] == "512m"
    assert profile.num_tasks == 2
    assert profile.total_memory_spill_bytes == 500000000
    assert profile.total_memory_spill_gb == round(500000000 / (1024**3), 6)
    assert profile.reference_shuffle_write_bytes == 1100000000
    assert profile.max_skew_ratio == 1.667


def test_parser_rejects_downloaded_sparkevitune_report(tmp_path):
    import json

    import pytest

    from sparkevitune.parser import InvalidSparkLogError

    report_path = tmp_path / "sparkevitune-report.json"
    report_path.write_text(
        json.dumps({"run_id": "abc123", "rule_report": {"app_id": "app-1"}}, indent=2),
        encoding="utf-8",
    )
    with pytest.raises(InvalidSparkLogError, match="SparkEviTune analysis report"):
        SparkLogParser().parse(report_path)


def test_parser_rejects_scalar_json_without_attribute_error(tmp_path):
    import pytest

    from sparkevitune.parser import InvalidSparkLogError

    path = tmp_path / "scalar-lines.jsonl"
    path.write_text('"not an event"\n', encoding="utf-8")
    with pytest.raises(InvalidSparkLogError, match="scalar values rather than event objects"):
        SparkLogParser().parse(path)


def test_parser_accepts_quoted_json_event_lines(tmp_path):
    import json

    path = tmp_path / "quoted-events.jsonl"
    events = [
        {
            "Event": "SparkListenerApplicationStart",
            "App ID": "quoted-app",
            "App Name": "quoted",
            "Timestamp": 1,
        },
        {
            "Event": "SparkListenerEnvironmentUpdate",
            "Spark Properties": {"spark.executor.memory": "2g"},
        },
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 1001},
    ]
    path.write_text("\n".join(json.dumps(json.dumps(item)) for item in events), encoding="utf-8")
    profile = SparkLogParser().parse(path)
    assert profile.app_id == "quoted-app"
    assert profile.spark_config["spark.executor.memory"] == "2g"


def test_spark35_effective_defaults_include_parallelism_first(tmp_path):
    import json

    path = tmp_path / "defaults.jsonl"
    events = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.7"},
        {"Event": "SparkListenerApplicationStart", "App ID": "defaults-app", "Timestamp": 1},
        {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {"spark.master": "local[4]"}},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 1001},
    ]
    path.write_text("\n".join(json.dumps(item) for item in events), encoding="utf-8")
    profile = SparkLogParser().parse(path)
    assert profile.spark_config["spark.sql.adaptive.enabled"] == "true"
    assert profile.spark_config["spark.sql.adaptive.coalescePartitions.enabled"] == "true"
    assert profile.spark_config["spark.sql.adaptive.coalescePartitions.parallelismFirst"] == "true"
    assert profile.spark_config["spark.sql.adaptive.advisoryPartitionSizeInBytes"] == "67108864b"
    assert profile.spark_config["spark.sql.adaptive.skewJoin.enabled"] == "true"
