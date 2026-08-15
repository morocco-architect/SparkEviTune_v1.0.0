from sparkevitune.detector import SymptomDetector
from sparkevitune.engine import RuleEngine
from sparkevitune.models import AppProfile, ClusterProfile, WorkloadProfile

KRYO = "org.apache.spark.serializer.KryoSerializer"


def make_profile(aqe: bool, skew_join: bool, skew_ratio: float) -> AppProfile:
    app = AppProfile(
        app_id="skew-policy",
        app_name="skew-policy",
        spark_config={
            "spark.master": "local[4]",
            "spark.executor.memory": "2g",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "10",
            "spark.sql.adaptive.enabled": str(aqe).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": str(skew_join).lower(),
            "spark.serializer": KRYO,
        },
    )
    app.max_skew_ratio = skew_ratio
    return app


def test_task_duration_skew_is_observation_only():
    cluster = ClusterProfile()
    app = SymptomDetector().detect(make_profile(True, False, 8.0), cluster, WorkloadProfile())
    assert app.symptoms["skew"] is True
    report = RuleEngine().build_report(app, cluster)
    assert not any(rec.parameter == "spark.sql.adaptive.skewJoin.enabled" for rec in report.recommendations)


def test_aqe_disabled_remains_actionable():
    cluster = ClusterProfile()
    app = SymptomDetector().detect(make_profile(False, False, 8.0), cluster, WorkloadProfile())
    assert app.symptoms["skew"] is True
    assert app.symptoms["aqe_disabled"] is True
    parameters = {rec.parameter for rec in RuleEngine().build_report(app, cluster).recommendations}
    assert "spark.sql.adaptive.enabled" in parameters
    assert "spark.sql.adaptive.skewJoin.enabled" not in parameters


def test_skew_below_threshold_is_false():
    app = SymptomDetector().detect(make_profile(True, False, 2.0), ClusterProfile(), WorkloadProfile())
    assert app.symptoms["skew"] is False
