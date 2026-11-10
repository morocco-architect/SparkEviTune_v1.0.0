from __future__ import annotations

import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .detector import SymptomDetector
from .engine import RuleEngine
from .feature_store import FeatureStore
from .features import FeatureBuilder
from .fusion import RecommendationFusion
from .llm import ExplanationService
from .ml import MLAnomalyDetector, ModelTrainer, PerformancePredictor, TrainingSummary
from .models import ClusterProfile, HybridReport, WorkloadProfile
from .optimizer import BayesianConfigOptimizer
from .parser import SparkLogParser
from .registry import ModelRegistry
from .utils import stable_hash
from .validator import ConstraintValidator


class SparkEviTunePipeline:
    def __init__(
        self,
        history_db: str | Path | None = None,
        model_dir: str | Path | None = None,
        knowledge_base: str | Path = "knowledge_base",
    ):
        history_db = history_db or os.getenv("SPARKEVITUNE_HISTORY_DB", "data/sparkevitune_history.db")
        model_dir = model_dir or os.getenv("SPARKEVITUNE_MODEL_DIR", "artifacts/models")
        self.store = FeatureStore(history_db)
        self.registry = ModelRegistry(model_dir)
        self.parser = SparkLogParser()
        self.detector = SymptomDetector()
        self.rule_engine = RuleEngine()
        self.feature_builder = FeatureBuilder()
        self.predictor = PerformancePredictor(self.registry)
        self.anomaly_detector = MLAnomalyDetector(self.registry)
        self.optimizer = BayesianConfigOptimizer(
            self.predictor,
            self.feature_builder,
            calls=int(os.getenv("SPARKEVITUNE_OPTIMIZER_CALLS", "24")),
        )
        self.fusion = RecommendationFusion()
        self.validator = ConstraintValidator()
        self.explainer = ExplanationService(knowledge_base)

    def analyze(
        self,
        log_path: str | Path,
        cluster: ClusterProfile | None = None,
        workload: WorkloadProfile | None = None,
        include_explanation: bool = True,
    ) -> HybridReport:
        cluster = cluster or ClusterProfile()
        workload = workload or WorkloadProfile()
        app = self.parser.parse(log_path)
        app = self.detector.detect(app, cluster, workload)
        from .profile_guard import check_profile_compatibility
        _, profile_guard_warning = check_profile_compatibility(app.spark_config.get("spark.master", ""), cluster)
        rule_report = self.rule_engine.build_report(app, cluster)
        features = self.feature_builder.build(app, cluster, workload)
        anomaly = self.anomaly_detector.detect(features)
        baseline_prediction = self.predictor.predict(features)
        candidate = self.optimizer.optimize(features, app.spark_config, cluster)
        fused = self.fusion.fuse(rule_report.recommendations, candidate, app.spark_config)

        proposed: dict[str, Any] = {}
        for recommendation in fused:
            proposed[recommendation.parameter] = recommendation.recommended_value
        validation = self.validator.validate(app.spark_config, proposed, cluster)
        run_id = stable_hash(
            {
                "app_id": app.app_id,
                "metrics": rule_report.metrics,
                "config": app.spark_config,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        report = HybridReport(
            run_id=run_id,
            rule_report=rule_report,
            cluster_profile=cluster,
            workload_profile=workload,
            features=features,
            anomaly=anomaly,
            baseline_prediction=baseline_prediction,
            optimized_candidate=candidate,
            fused_recommendations=fused,
            validation=validation,
            audit={
                "created_at": datetime.now(timezone.utc).isoformat(),
                "pipeline_version": "1.0.0",
                "model_registry": self.registry.status(),
                "profile_guard_warning": profile_guard_warning,
                "human_validation_required": True,
                "auto_apply": False,
            },
        )
        if include_explanation:
            report.explanation = self.explainer.explain(report)
        return report

    def train_models(self) -> TrainingSummary:
        min_rows = int(os.getenv("SPARKEVITUNE_MIN_TRAINING_RUNS", "20"))
        min_scenarios = int(os.getenv("SPARKEVITUNE_MIN_TRAINING_SCENARIOS", "20"))
        trainer = ModelTrainer(
            self.registry,
            min_rows=min_rows,
            min_scenarios=min_scenarios,
        )
        return trainer.train(self.store.dataframe())

    def record_feedback(
        self,
        report: HybridReport,
        observed: dict[str, float],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Record a *verified rerun*, never an unexecuted recommendation.

        Required metadata:
          execution_id: unique ID for this concrete Spark rerun
          executed_config: effective Spark config for that rerun
          execution_verified: True, supplied by the caller after its own checks
          scenario_id: grouping key for leakage-free evaluation
          split: development/train/training/holdout/test

        This guard checks the caller's evidence contract, not the Spark logs
        themselves. The ingestion layer must independently verify provenance.
        """
        metadata = dict(metadata or {})
        required = ("execution_id", "executed_config", "execution_verified", "scenario_id", "split")
        missing = [k for k in required if k not in metadata]
        if missing:
            raise ValueError(f"Verified feedback metadata required: {missing}")
        if metadata["execution_verified"] is not True:
            raise ValueError("Feedback rejected: execution has not been verified")
        if not isinstance(metadata["execution_id"], str) or not metadata["execution_id"].strip():
            raise ValueError("execution_id must be a nonempty unique string")
        if not isinstance(metadata["scenario_id"], str) or not metadata["scenario_id"].strip():
            raise ValueError("scenario_id must be a nonempty grouping label")
        split = str(metadata["split"]).strip().lower()
        if split not in {"development", "train", "training", "holdout", "test"}:
            raise ValueError("Invalid split label")
        config = metadata["executed_config"]
        if not isinstance(config, dict) or not config:
            raise ValueError("executed_config must be a nonempty mapping")
        if "duration_s" not in observed:
            raise ValueError("A measured duration_s is required")
        targets = {}
        for key in ("duration_s", "memory_spill_gb", "cost", "oom"):
            value = observed.get(key)
            if value is None:
                continue  # Unknown is NOT zero.
            value = float(value)
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"Invalid observed {key}: {value}")
            if key == "oom" and value not in (0.0, 1.0):
                raise ValueError("oom must be binary (0 or 1)")
            targets[key] = value
        candidate_features = self.feature_builder.apply_candidate(report.features, config)
        # The same report may be rerun many times; each actual execution gets its own ID.
        observation_id = stable_hash({"report_run_id": report.run_id, "execution_id": metadata["execution_id"]})
        self.store.upsert_run(
            run_id=observation_id,
            app_id=report.rule_report.app_id,
            features=candidate_features,
            config=config,
            targets=targets,
            metadata={"source": "verified_rerun", "parent_report_id": report.run_id,
                      **metadata, "split": split},
        )
