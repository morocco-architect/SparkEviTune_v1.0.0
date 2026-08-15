from __future__ import annotations

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
        targets = {
            "duration_s": float(observed["duration_s"]),
            "memory_spill_gb": float(observed.get("memory_spill_gb", 0.0)),
            "cost": float(observed.get("cost", 0.0)),
            "oom": float(observed.get("oom", 0.0)),
        }
        candidate_features = self.feature_builder.apply_candidate(
            report.features,
            report.validation.configuration,
        )
        self.store.upsert_run(
            run_id=report.run_id,
            app_id=report.rule_report.app_id,
            features=candidate_features,
            config=report.validation.configuration,
            targets=targets,
            metadata={"source": "validated_re-run", **(metadata or {})},
        )
