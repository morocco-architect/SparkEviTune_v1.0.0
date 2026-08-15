from sparkevitune.detector import SymptomDetector
from sparkevitune.engine import RuleEngine
from sparkevitune.models import AppProfile, ClusterProfile, WorkloadProfile

KRYO = "org.apache.spark.serializer.KryoSerializer"
JAVA = "org.apache.spark.serializer.JavaSerializer"


def _clean_profile(serializer: str) -> AppProfile:
    return AppProfile(
        app_id="serializer-policy",
        app_name="serializer-policy",
        spark_config={
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "10",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.serializer": serializer,
        },
    )


def test_java_serializer_is_observed_without_unconditional_rule_recommendation():
    cluster = ClusterProfile()
    app = SymptomDetector().detect(_clean_profile(JAVA), cluster, WorkloadProfile())
    assert app.symptoms["java_serializer"] is True
    report = RuleEngine().build_report(app, cluster)
    assert not any(rec.parameter == "spark.serializer" for rec in report.recommendations)
    assert report.rule_compliance_score == 100


def test_kryo_serializer_is_not_flagged():
    cluster = ClusterProfile()
    app = SymptomDetector().detect(_clean_profile(KRYO), cluster, WorkloadProfile())
    assert app.symptoms["java_serializer"] is False
    report = RuleEngine().build_report(app, cluster)
    assert not any(rec.parameter == "spark.serializer" for rec in report.recommendations)
    assert report.rule_compliance_score == 100
