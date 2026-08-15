from pathlib import Path

from sparkevitune.models import ClusterProfile, WorkloadProfile
from sparkevitune.pipeline import SparkEviTunePipeline


def test_pipeline_runs_without_ml_models(tmp_path):
    fixture = Path(__file__).parent / "fixtures" / "minimal_event_log.jsonl"
    pipeline = SparkEviTunePipeline(
        history_db=tmp_path / "history.db",
        model_dir=tmp_path / "models",
        knowledge_base=Path(__file__).parents[1] / "knowledge_base",
    )
    report = pipeline.analyze(
        fixture,
        ClusterProfile(workers=1, cores_per_worker=4, memory_per_worker_gb=4),
        WorkloadProfile(workload_type="etl", input_size_gb=0.5, num_aggregations=1),
    )
    assert report.rule_report.rule_compliance_score < 100
    assert report.optimized_candidate is None
    assert report.validation.configuration
    assert "human review" in report.explanation.lower()
