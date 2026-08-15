from sparkevitune.features import FeatureBuilder
from sparkevitune.models import AppProfile, ClusterProfile, WorkloadProfile


def test_generated_workload_rows_and_type_are_prediction_features():
    app = AppProfile(app_id="workload-features", app_name="workload-features", spark_config={
        "spark.executor.memory": "1g", "spark.driver.memory": "1g", "spark.sql.shuffle.partitions": "200"
    })
    workload = WorkloadProfile(workload_type="heavy_shuffle", input_rows=10_000_000)
    features = FeatureBuilder().build(app, ClusterProfile(), workload)
    assert features["input_rows"] == 10_000_000.0
    assert features["input_size_gb"] == 0.0
    assert features["workload_heavy_shuffle"] == 1.0
    assert features["workload_etl"] == 0.0
    assert features["workload_sql_joins"] == 0.0
    assert features["workload_skew_join"] == 0.0
