import pandas as pd
from sparkevitune.ml import ModelTrainer


def test_many_repetitions_do_not_replace_scenario_diversity():
    frame = pd.DataFrame({
        "meta_scenario_id": [f"scenario-{i % 4}" for i in range(40)],
        "target_duration_s": [float(i + 1) for i in range(40)],
    })
    summary = ModelTrainer(registry=None, min_rows=20, min_scenarios=5).train(frame)
    assert summary.trained == []
    assert "distinct training scenarios" in summary.skipped["all"]
    assert "found 4" in summary.skipped["all"]


def test_heldout_rows_do_not_count_toward_training_minimum():
    frame = pd.DataFrame({
        "meta_scenario_id": [f"scenario-{i}" for i in range(30)],
        "meta_split": ["development"] * 10 + ["heldout"] * 20,
        "target_duration_s": [float(i + 1) for i in range(30)],
    })
    summary = ModelTrainer(registry=None, min_rows=20, min_scenarios=1).train(frame)
    assert summary.rows == 10
    assert summary.trained == []
    assert "20 training runs" in summary.skipped["all"]
