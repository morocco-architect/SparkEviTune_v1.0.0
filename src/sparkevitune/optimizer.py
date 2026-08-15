from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable

import numpy as np
from scipy.stats import norm
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import Matern, WhiteKernel

from .features import FeatureBuilder
from .models import CandidateConfiguration, ClusterProfile, Prediction
from .ml import PerformancePredictor


@dataclass
class ObjectiveWeights:
    duration: float = 0.55
    spill: float = 0.20
    cost: float = 0.15
    oom_risk: float = 0.10


class BayesianConfigOptimizer:
    """Lightweight Gaussian-process Bayesian optimization over a bounded Spark search space."""

    def __init__(
        self,
        predictor: PerformancePredictor,
        feature_builder: FeatureBuilder,
        calls: int = 24,
        random_state: int = 42,
        weights: ObjectiveWeights | None = None,
    ):
        self.predictor = predictor
        self.feature_builder = feature_builder
        self.calls = max(10, calls)
        self.rng = np.random.default_rng(random_state)
        self.weights = weights or ObjectiveWeights()

    def _bounds(self, cluster: ClusterProfile) -> list[tuple[float, float]]:
        # Leave at least 15% of worker memory outside executor heap.
        max_heap = max(1.0, cluster.memory_per_worker_gb * 0.85)
        return [
            (1.0, max_heap),
            (1.0, float(max(1, min(cluster.cores_per_worker, 8)))),
            (10.0, 2000.0),
            (0.0, 1.0),  # AQE
            (0.0, 1.0),  # AQE skewJoin
            (0.0, 1.0),  # serializer
            (0.5, 0.8),  # memory fraction
        ]

    def _decode(self, vector: np.ndarray) -> dict[str, object]:
        aqe_enabled = vector[3] >= 0.5
        # Spark's skew-join optimization is conditional on AQE. Keep the two
        # knobs explicit in the search representation, but canonicalize the
        # functionally redundant AQE=false/skewJoin=true combination.
        skew_join_enabled = aqe_enabled and vector[4] >= 0.5
        return {
            "spark.executor.memory": f"{max(1, int(round(vector[0])))}g",
            "spark.executor.cores": max(1, int(round(vector[1]))),
            "spark.sql.shuffle.partitions": max(10, int(round(vector[2] / 10.0) * 10)),
            "spark.sql.adaptive.enabled": "true" if aqe_enabled else "false",
            "spark.sql.adaptive.skewJoin.enabled": "true" if skew_join_enabled else "false",
            "spark.serializer": (
                "org.apache.spark.serializer.KryoSerializer"
                if vector[5] >= 0.5
                else "org.apache.spark.serializer.JavaSerializer"
            ),
            "spark.memory.fraction": round(float(vector[6]), 2),
        }

    def _candidate_config(
        self,
        vector: np.ndarray,
        current_config: dict[str, str],
    ) -> dict[str, object]:
        config = self._decode(vector)
        spark_master = str(current_config.get("spark.master", "")).strip().lower()
        if spark_master.startswith("local"):
            config.pop("spark.executor.memory", None)
            config.pop("spark.executor.cores", None)
        return config

    def _sample(self, bounds: list[tuple[float, float]], size: int) -> np.ndarray:
        return np.column_stack([self.rng.uniform(low, high, size=size) for low, high in bounds])

    def optimize(
        self,
        base_features: dict[str, float],
        current_config: dict[str, str],
        cluster: ClusterProfile,
    ) -> CandidateConfiguration | None:
        baseline = self.predictor.predict(base_features)
        if not baseline.available or baseline.duration_s is None:
            return None

        duration_scale = max(baseline.duration_s, 1.0)
        spill_scale = max(baseline.memory_spill_gb or base_features.get("memory_spill_gb", 0.0), 0.1)
        cost_scale = max(baseline.cost or 1.0, 1.0)

        def evaluate(vector: np.ndarray) -> tuple[float, Prediction]:
            config = self._candidate_config(vector, current_config)
            candidate_features = self.feature_builder.apply_candidate(base_features, config)
            prediction = self.predictor.predict(candidate_features)
            if not prediction.available or prediction.duration_s is None:
                return float("inf"), prediction
            objective = (
                self.weights.duration * prediction.duration_s / duration_scale
                + self.weights.spill * (prediction.memory_spill_gb or 0.0) / spill_scale
                + self.weights.cost * (prediction.cost or 0.0) / cost_scale
                + self.weights.oom_risk * (prediction.oom_risk or 0.0)
            )
            return float(objective), prediction

        bounds = self._bounds(cluster)
        initial_count = min(10, self.calls // 2)
        X = self._sample(bounds, initial_count)
        y: list[float] = []
        predictions: list[Prediction] = []
        for row in X:
            objective, prediction = evaluate(row)
            y.append(objective)
            predictions.append(prediction)

        kernel = Matern(nu=2.5) + WhiteKernel(noise_level=1e-5)
        for _ in range(initial_count, self.calls):
            gp = GaussianProcessRegressor(
                kernel=kernel,
                normalize_y=True,
                random_state=42,
                n_restarts_optimizer=1,
            )
            gp.fit(X, np.asarray(y))
            pool = self._sample(bounds, 500)
            mean, std = gp.predict(pool, return_std=True)
            best = float(np.min(y))
            improvement = best - mean - 0.01
            z = improvement / np.maximum(std, 1e-9)
            expected_improvement = improvement * norm.cdf(z) + std * norm.pdf(z)
            next_row = pool[int(np.argmax(expected_improvement))]
            objective, prediction = evaluate(next_row)
            X = np.vstack([X, next_row])
            y.append(objective)
            predictions.append(prediction)

        best_index = int(np.argmin(y))
        best_vector = X[best_index]
        return CandidateConfiguration(
            values=self._candidate_config(best_vector, current_config),
            objective=float(y[best_index]),
            prediction=predictions[best_index],
            method="Gaussian-process Bayesian optimization",
        )
