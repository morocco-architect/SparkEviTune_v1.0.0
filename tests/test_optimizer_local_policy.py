import numpy as np

from sparkevitune.features import FeatureBuilder
from sparkevitune.optimizer import BayesianConfigOptimizer


def _optimizer():
    return BayesianConfigOptimizer(predictor=None, feature_builder=FeatureBuilder())

VECTOR = np.array([6.0, 4.0, 100.0, 1.0, 0.0, 1.0, 0.65])


def test_local_optimizer_does_not_propose_executor_sizing():
    cfg = _optimizer()._candidate_config(VECTOR, {
        "spark.master": "local[4]", "spark.executor.memory": "1g", "spark.executor.cores": "1"
    })
    assert "spark.executor.memory" not in cfg
    assert "spark.executor.cores" not in cfg
    assert cfg["spark.sql.shuffle.partitions"] == 100
    assert cfg["spark.sql.adaptive.enabled"] == "true"


def test_cluster_optimizer_keeps_executor_sizing():
    cfg = _optimizer()._candidate_config(VECTOR, {"spark.master": "spark://cluster.example:7077"})
    assert cfg["spark.executor.memory"] == "6g"
    assert cfg["spark.executor.cores"] == 4
