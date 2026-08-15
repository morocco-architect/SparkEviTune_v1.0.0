from sparkevitune.models import ClusterProfile
from sparkevitune.validator import ConstraintValidator


def test_validator_keeps_executor_within_worker_memory():
    result = ConstraintValidator().validate(
        {},
        {"spark.executor.memory": "8g", "spark.executor.cores": 8},
        ClusterProfile(workers=1, cores_per_worker=4, memory_per_worker_gb=4),
    )
    assert result.configuration["spark.executor.memory"] != "8g"
    assert result.configuration["spark.executor.cores"] == 4
    assert result.adjustments


def test_validator_limits_total_executor_instances():
    result = ConstraintValidator().validate(
        {},
        {
            "spark.executor.memory": "2g",
            "spark.executor.cores": 2,
            "spark.executor.instances": 20,
        },
        ClusterProfile(workers=2, cores_per_worker=4, memory_per_worker_gb=8),
    )
    assert int(result.configuration["spark.executor.instances"]) <= 4
    assert result.adjustments


def test_validator_rejects_unsupported_candidate_parameter():
    result = ConstraintValidator().validate(
        {},
        {"spark.evil.command": "rm -rf /"},
        ClusterProfile(),
    )
    assert not result.valid
    assert "spark.evil.command" not in result.configuration
