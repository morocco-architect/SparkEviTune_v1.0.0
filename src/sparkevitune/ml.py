from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesRegressor, IsolationForest, RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    brier_score_loss,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    precision_score,
    r2_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GroupShuffleSplit, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import RobustScaler

from .features import ANOMALY_FEATURE_COLUMNS, PREDICTION_FEATURE_COLUMNS
from .models import AnomalyResult, Prediction
from .registry import ModelRegistry


@dataclass
class TrainingSummary:
    rows: int
    trained: list[str]
    skipped: dict[str, str]
    metrics: dict[str, dict[str, float]]
    leakage_guard: str = "prediction models exclude post-run targets and metrics"


class ModelTrainer:
    TARGETS = {  # noqa: RUF012
        "duration": "target_duration_s",
        "spill": "target_memory_spill_gb",
        "cost": "target_cost",
        "oom": "target_oom",
    }

    def __init__(
        self,
        registry: ModelRegistry,
        min_rows: int = 20,
        min_scenarios: int = 20,
        random_state: int = 42,
    ):
        self.registry = registry
        self.min_rows = min_rows
        self.min_scenarios = min_scenarios
        self.random_state = random_state

    def _split(self, X: pd.DataFrame, y: pd.Series, frame: pd.DataFrame):
        group_col = next((c for c in ("meta_scenario_id", "meta_workload", "meta_group") if c in frame), None)
        if group_col and frame[group_col].nunique() >= 3:
            groups = frame.loc[y.index, group_col]
            splitter = GroupShuffleSplit(n_splits=1, test_size=0.25, random_state=self.random_state)
            train_idx, test_idx = next(splitter.split(X, y, groups=groups))
            return X.iloc[train_idx], X.iloc[test_idx], y.iloc[train_idx], y.iloc[test_idx], group_col
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=self.random_state
        )
        return X_train, X_test, y_train, y_test, "random"

    def train(self, frame: pd.DataFrame) -> TrainingSummary:
        trained: list[str] = []
        skipped: dict[str, str] = {}
        metrics: dict[str, dict[str, float]] = {}

        training_frame = frame
        if "meta_split" in frame.columns:
            allowed_splits = {"development", "train", "training"}
            normalized_split = (
                frame["meta_split"].fillna("").astype(str).str.strip().str.lower()
            )
            training_frame = frame.loc[normalized_split.isin(allowed_splits)].copy()
        frame = training_frame

        if len(frame) < self.min_rows:
            return TrainingSummary(
                rows=len(frame),
                trained=[],
                skipped={"all": f"Need at least {self.min_rows} training runs."},
                metrics={},
            )

        if "meta_scenario_id" in frame.columns:
            scenario_count = int(frame["meta_scenario_id"].dropna().nunique())
        else:
            scenario_count = len(
                frame.reindex(columns=PREDICTION_FEATURE_COLUMNS, fill_value=0.0).drop_duplicates()
            )
        if scenario_count < self.min_scenarios:
            return TrainingSummary(
                rows=len(frame),
                trained=[],
                skipped={
                    "all": (
                        f"Need at least {self.min_scenarios} distinct training scenarios; "
                        f"found {scenario_count}."
                    )
                },
                metrics={},
            )

        X_all = frame.reindex(columns=PREDICTION_FEATURE_COLUMNS, fill_value=0.0)
        for name, target_column in self.TARGETS.items():
            if target_column not in frame.columns or frame[target_column].dropna().nunique() < 2:
                skipped[name] = f"Missing or constant target: {target_column}"
                continue
            valid = frame[target_column].notna()
            X_valid = X_all.loc[valid]
            y = frame.loc[valid, target_column]
            if len(y) < self.min_rows:
                skipped[name] = f"Only {len(y)} labeled rows."
                continue
            if name == "oom" and y.nunique() < 2:
                skipped[name] = "OOM target contains only one class."
                continue

            X_train, X_test, y_train, y_test, split_kind = self._split(X_valid, y, frame.loc[valid])
            if name == "oom":
                estimator: Any = RandomForestClassifier(
                    n_estimators=300,
                    min_samples_leaf=2,
                    class_weight="balanced",
                    random_state=self.random_state,
                    n_jobs=-1,
                )
            else:
                estimator = ExtraTreesRegressor(
                    n_estimators=400,
                    min_samples_leaf=2,
                    random_state=self.random_state,
                    n_jobs=-1,
                )
            pipeline = Pipeline([("imputer", SimpleImputer(strategy="median")), ("model", estimator)])
            pipeline.fit(X_train, y_train)
            predictions = pipeline.predict(X_test)
            model_metrics: dict[str, float] = {}
            if name != "oom":
                residuals = np.asarray(y_test) - np.asarray(predictions)
                model_metrics = {
                    "mae": float(mean_absolute_error(y_test, predictions)),
                    "rmse": float(math.sqrt(mean_squared_error(y_test, predictions))),
                    "r2": float(r2_score(y_test, predictions)) if len(y_test) > 1 else float("nan"),
                    "residual_std": float(np.std(residuals)),
                }
            else:
                probabilities = pipeline.predict_proba(X_test)[:, 1]
                model_metrics = {
                    "accuracy": float(accuracy_score(y_test, predictions)),
                    "precision": float(precision_score(y_test, predictions, zero_division=0)),
                    "recall": float(recall_score(y_test, predictions, zero_division=0)),
                    "f1": float(f1_score(y_test, predictions, zero_division=0)),
                    "brier": float(brier_score_loss(y_test, probabilities)),
                }
                if len(np.unique(y_test)) == 2:
                    model_metrics["roc_auc"] = float(roc_auc_score(y_test, probabilities))
                    model_metrics["average_precision"] = float(average_precision_score(y_test, probabilities))
            version = f"{name}-rows{len(y)}-rs{self.random_state}"
            self.registry.save(
                name,
                pipeline,
                {
                    "version": version,
                    "feature_columns": PREDICTION_FEATURE_COLUMNS,
                    "target": target_column,
                    "rows": len(y),
                    "metrics": model_metrics,
                    "split_kind": split_kind,
                    "data_kind": "user-supplied historical runs",
                    "target_leakage_guard": True,
                },
            )
            trained.append(name)
            metrics[name] = model_metrics

        anomaly_X = frame.reindex(columns=ANOMALY_FEATURE_COLUMNS, fill_value=0.0)
        normal_mask = frame.get("meta_label_anomaly", pd.Series(index=frame.index, dtype=float)).fillna(0) == 0
        fit_X = anomaly_X.loc[normal_mask] if normal_mask.sum() >= max(3, self.min_rows // 2) else anomaly_X
        anomaly_pipeline = Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", RobustScaler()),
                (
                    "model",
                    IsolationForest(
                        n_estimators=400,
                        contamination="auto",
                        random_state=self.random_state,
                        n_jobs=-1,
                    ),
                ),
            ]
        )
        anomaly_pipeline.fit(fit_X)
        self.registry.save(
            "anomaly",
            anomaly_pipeline,
            {
                "version": f"anomaly-rows{len(fit_X)}-rs{self.random_state}",
                "feature_columns": ANOMALY_FEATURE_COLUMNS,
                "rows": len(fit_X),
                "trained_on_normal_only": bool(normal_mask.sum() >= max(3, self.min_rows // 2)),
            },
        )
        trained.append("anomaly")
        return TrainingSummary(rows=len(frame), trained=trained, skipped=skipped, metrics=metrics)


class PerformancePredictor:
    def __init__(self, registry: ModelRegistry):
        self.registry = registry

    def predict(self, features: dict[str, float]) -> Prediction:
        row = pd.DataFrame(
            [[features.get(name, 0.0) for name in PREDICTION_FEATURE_COLUMNS]],
            columns=PREDICTION_FEATURE_COLUMNS,
        )
        output = Prediction(available=False)
        loaded = 0
        for model_name, attr in [("duration", "duration_s"), ("spill", "memory_spill_gb"), ("cost", "cost")]:
            model = self.registry.load(model_name)
            if model is None:
                continue
            metadata = self.registry.metadata(model_name)
            expected = metadata.get("feature_columns", PREDICTION_FEATURE_COLUMNS)
            model_row = row.reindex(columns=expected, fill_value=0.0)
            value = max(0.0, float(model.predict(model_row)[0]))
            setattr(output, attr, value)
            output.model_versions[model_name] = str(metadata.get("version", "unknown"))
            residual = float(metadata.get("metrics", {}).get("residual_std", 0.0))
            output.uncertainty[attr] = residual
            loaded += 1
        oom_model = self.registry.load("oom")
        if oom_model is not None:
            metadata = self.registry.metadata("oom")
            expected = metadata.get("feature_columns", PREDICTION_FEATURE_COLUMNS)
            model_row = row.reindex(columns=expected, fill_value=0.0)
            if hasattr(oom_model, "predict_proba"):
                output.oom_risk = float(oom_model.predict_proba(model_row)[0][-1])
            else:
                output.oom_risk = float(oom_model.predict(model_row)[0])
            output.model_versions["oom"] = str(metadata.get("version", "unknown"))
            loaded += 1
        output.available = loaded > 0
        if not output.available:
            output.warning = "No trained prediction models are available. The rule-based layer remains active."
        return output


class MLAnomalyDetector:
    def __init__(self, registry: ModelRegistry):
        self.registry = registry

    def detect(self, features: dict[str, float]) -> AnomalyResult:
        model = self.registry.load("anomaly")
        if model is None:
            return AnomalyResult(available=False, explanation="No anomaly model has been trained.")
        metadata = self.registry.metadata("anomaly")
        expected = metadata.get("feature_columns", ANOMALY_FEATURE_COLUMNS)
        row = pd.DataFrame([[features.get(name, 0.0) for name in expected]], columns=expected)
        decision = float(model.decision_function(row)[0])
        prediction = int(model.predict(row)[0])
        normalized_score = float(1.0 / (1.0 + math.exp(5.0 * decision)))
        return AnomalyResult(
            available=True,
            is_anomaly=prediction == -1,
            score=normalized_score,
            explanation=(
                "The run is outside the distribution learned from historical profiles."
                if prediction == -1
                else "The run is within the distribution learned from historical profiles."
            ),
            model_version=str(metadata.get("version", "unknown")),
        )
