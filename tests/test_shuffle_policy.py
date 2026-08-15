from sparkevitune.detector import SymptomDetector
from sparkevitune.models import AppProfile, ClusterProfile, StageProfile, WorkloadProfile
from sparkevitune.utils import BYTES_PER_GIB, BYTES_PER_MIB, recommended_shuffle_partitions


def _profile(stages: list[StageProfile]) -> AppProfile:
    app = AppProfile(
        app_id="shuffle-test",
        app_name="shuffle-test",
        spark_config={
            "spark.executor.memory": "4g",
            "spark.driver.memory": "2g",
            "spark.sql.shuffle.partitions": "200",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.advisoryPartitionSizeInBytes": "64MB",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        },
        stages=stages,
    )
    for stage in app.stages:
        stage.compute()
    app.aggregate()
    return app


def test_multiple_stages_are_not_summed_for_partition_sizing():
    stages = [
        StageProfile(
            stage_id=0,
            total_shuffle_write=BYTES_PER_GIB,
            total_successful_shuffle_write=BYTES_PER_GIB,
        ),
        StageProfile(
            stage_id=1,
            total_shuffle_write=BYTES_PER_GIB,
            total_successful_shuffle_write=BYTES_PER_GIB,
        ),
    ]
    app = _profile(stages)
    assert app.total_shuffle_write_bytes == 2 * BYTES_PER_GIB
    assert app.reference_shuffle_write_bytes == BYTES_PER_GIB
    detected = SymptomDetector().detect(app, ClusterProfile(), WorkloadProfile())
    assert detected.recommended_shuffle_partitions == 20


def test_shuffle_read_is_not_added_to_partition_sizing_volume():
    stage = StageProfile(
        stage_id=0,
        total_shuffle_write=BYTES_PER_GIB,
        total_successful_shuffle_write=BYTES_PER_GIB,
        total_shuffle_read=4 * BYTES_PER_GIB,
    )
    app = _profile([stage])
    assert app.reference_shuffle_write_bytes == BYTES_PER_GIB
    assert recommended_shuffle_partitions(
        app.reference_shuffle_write_bytes,
        64 * BYTES_PER_MIB,
        10,
    ) == 20


def test_failed_stage_attempt_is_excluded_from_reference_volume():
    stages = [
        StageProfile(
            stage_id=0,
            stage_attempt_id=0,
            successful=False,
            total_shuffle_write=8 * BYTES_PER_GIB,
            total_successful_shuffle_write=8 * BYTES_PER_GIB,
        ),
        StageProfile(
            stage_id=0,
            stage_attempt_id=1,
            successful=True,
            total_shuffle_write=BYTES_PER_GIB,
            total_successful_shuffle_write=BYTES_PER_GIB,
        ),
    ]
    app = _profile(stages)
    assert app.reference_shuffle_write_bytes == BYTES_PER_GIB


def _partition_profile(*, current: int, tasks: int, aqe: bool = True, coalesce: bool = True, write_bytes: int = 64 * BYTES_PER_MIB):
    stage = StageProfile(
        stage_id=0,
        successful=True,
        num_tasks=tasks,
        total_shuffle_write=write_bytes,
        total_successful_shuffle_write=write_bytes,
    )
    app = _profile([stage])
    app.spark_config["spark.sql.shuffle.partitions"] = str(current)
    app.spark_config["spark.sql.adaptive.enabled"] = str(aqe).lower()
    app.spark_config["spark.sql.adaptive.coalescePartitions.enabled"] = str(coalesce).lower()
    return app


def test_aqe_coalescing_suppresses_small_static_overpartitioning():
    app = _partition_profile(current=200, tasks=4)
    detected = SymptomDetector().detect(app, ClusterProfile(), WorkloadProfile())
    assert detected.recommended_shuffle_partitions == 10
    assert detected.symptoms["bad_partitions"] is False


def test_aqe_coalescing_keeps_observed_large_overpartitioning():
    app = _partition_profile(current=200, tasks=40)
    detected = SymptomDetector().detect(app, ClusterProfile(), WorkloadProfile())
    assert detected.symptoms["bad_partitions"] is True


def test_aqe_disabled_keeps_static_overpartitioning_actionable():
    app = _partition_profile(current=200, tasks=4, aqe=False)
    detected = SymptomDetector().detect(app, ClusterProfile(), WorkloadProfile())
    assert detected.symptoms["bad_partitions"] is True


def test_underpartitioning_is_not_suppressed_by_aqe_coalescing():
    app = _partition_profile(current=5, tasks=4, write_bytes=BYTES_PER_GIB)
    detected = SymptomDetector().detect(app, ClusterProfile(), WorkloadProfile())
    assert detected.recommended_shuffle_partitions == 20
    assert detected.symptoms["bad_partitions"] is True
