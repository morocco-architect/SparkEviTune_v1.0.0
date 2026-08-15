from types import SimpleNamespace

from sparkevitune.pipeline import SparkEviTunePipeline


def test_feedback_targets_use_validated_candidate_features(tmp_path):
    pipeline = SparkEviTunePipeline(history_db=tmp_path / "history.db", model_dir=tmp_path / "models")
    report = SimpleNamespace(
        run_id="feedback-alignment",
        rule_report=SimpleNamespace(app_id="app-feedback"),
        features={
            "executor_memory_gb": 1.0,
            "shuffle_partitions": 200.0,
            "aqe_enabled": 0.0,
            "skew_join_enabled": 0.0,
            "kryo_enabled": 0.0,
        },
        validation=SimpleNamespace(
            configuration={
                "spark.executor.memory": "2g",
                "spark.sql.shuffle.partitions": "10",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            }
        ),
    )
    pipeline.record_feedback(report, {"duration_s": 8.0, "memory_spill_gb": 0.0, "cost": 0.0, "oom": 0.0})
    row = pipeline.store.dataframe().iloc[0]
    assert row["executor_memory_gb"] == 2.0
    assert row["shuffle_partitions"] == 10.0
    assert row["aqe_enabled"] == 1.0
    assert row["skew_join_enabled"] == 1.0
    assert row["kryo_enabled"] == 1.0
    assert row["target_duration_s"] == 8.0
