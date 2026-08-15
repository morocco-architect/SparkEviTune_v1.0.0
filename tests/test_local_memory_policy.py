import json

from sparkevitune.detector import SymptomDetector
from sparkevitune.engine import RuleEngine
from sparkevitune.models import AppProfile, ClusterProfile, WorkloadProfile
from sparkevitune.parser import SparkLogParser

KRYO = "org.apache.spark.serializer.KryoSerializer"


def _pressure_profile(master: str) -> AppProfile:
    return AppProfile(
        app_id="memory-policy",
        app_name="memory-policy",
        spark_config={
            "spark.master": master,
            "spark.executor.memory": "1g",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "10",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": KRYO,
        },
        total_memory_spill_gb=0.8,
        total_disk_spill_gb=0.2,
    )


def test_parser_preserves_execution_context(tmp_path):
    path = tmp_path / "eventlog"
    events = [
        {"Event": "SparkListenerLogStart", "Spark Version": "3.5.7"},
        {"Event": "SparkListenerApplicationStart", "App ID": "local-test", "App Name": "local-test", "Timestamp": 1000},
        {"Event": "SparkListenerEnvironmentUpdate", "Spark Properties": {"spark.master": "local[4]", "spark.submit.deployMode": "client"}},
        {"Event": "SparkListenerApplicationEnd", "Timestamp": 2000},
    ]
    path.write_text("\n".join(json.dumps(event) for event in events) + "\n", encoding="utf-8")
    app = SparkLogParser().parse(path)
    assert app.spark_config["spark.master"] == "local[4]"
    assert app.spark_config["spark.submit.deployMode"] == "client"
    assert app.spark_config["spark.sql.adaptive.enabled"] == "true"


def test_local_spill_is_observed_without_executor_memory_escalation():
    cluster = ClusterProfile()
    app = SymptomDetector().detect(_pressure_profile("local[4]"), cluster, WorkloadProfile())
    assert app.symptoms["spilling"] is True
    assert app.symptoms["low_executor_memory"] is False
    report = RuleEngine().build_report(app, cluster)
    assert not any(rec.parameter == "spark.executor.memory" for rec in report.recommendations)


def test_nonlocal_spill_keeps_executor_memory_recommendation():
    cluster = ClusterProfile()
    app = SymptomDetector().detect(_pressure_profile("spark://cluster.example:7077"), cluster, WorkloadProfile())
    assert app.symptoms["spilling"] is True
    assert app.symptoms["low_executor_memory"] is True
    report = RuleEngine().build_report(app, cluster)
    memory_recs = [rec for rec in report.recommendations if rec.parameter == "spark.executor.memory"]
    assert len(memory_recs) == 1
    assert memory_recs[0].recommended_value == "2g"
    assert memory_recs[0].priority == "CRITICAL"
