import csv
from pathlib import Path


def test_softwarex_evidence_summaries_are_present_and_non_synthetic():
    path = Path("artifacts/softwarex_evidence/ml_readiness_summary.csv")
    assert path.exists()
    rows = {row["metric"]: row["value"] for row in csv.DictReader(path.open(encoding="utf-8"))}
    assert rows["publication_runs"] == "81"
    assert rows["distinct_scenarios"] == "16"
    assert rows["training_min_scenarios"] == "20"
