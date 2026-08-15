import numpy as np

from sparkevitune.features import FeatureBuilder
from sparkevitune.optimizer import BayesianConfigOptimizer


def _optimizer():
    return BayesianConfigOptimizer(predictor=None, feature_builder=FeatureBuilder())


def test_optimizer_can_enable_aqe_without_skewjoin():
    cfg = _optimizer()._decode(np.array([2.0, 2.0, 100.0, 1.0, 0.0, 0.0, 0.6]))
    assert cfg["spark.sql.adaptive.enabled"] == "true"
    assert cfg["spark.sql.adaptive.skewJoin.enabled"] == "false"


def test_optimizer_canonicalizes_skewjoin_when_aqe_is_disabled():
    cfg = _optimizer()._decode(np.array([2.0, 2.0, 100.0, 0.0, 1.0, 1.0, 0.6]))
    assert cfg["spark.sql.adaptive.enabled"] == "false"
    assert cfg["spark.sql.adaptive.skewJoin.enabled"] == "false"
    assert "KryoSerializer" in cfg["spark.serializer"]


def test_optimizer_can_enable_skewjoin_when_aqe_is_enabled():
    cfg = _optimizer()._decode(np.array([2.0, 2.0, 100.0, 1.0, 1.0, 0.0, 0.6]))
    assert cfg["spark.sql.adaptive.enabled"] == "true"
    assert cfg["spark.sql.adaptive.skewJoin.enabled"] == "true"
