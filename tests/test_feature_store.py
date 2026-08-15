from sparkevitune.feature_store import FeatureStore
from sparkevitune.features import FEATURE_COLUMNS


def test_feature_store_round_trip(tmp_path):
    store = FeatureStore(tmp_path / "history.db")
    features = {name: float(index) for index, name in enumerate(FEATURE_COLUMNS)}
    store.upsert_run(
        "run-1",
        "app-1",
        features,
        {},
        {"duration_s": 10.0, "memory_spill_gb": 0.0, "cost": 1.0, "oom": 0.0},
    )
    assert store.count() == 1
    frame = store.dataframe()
    assert frame.loc[0, "target_duration_s"] == 10.0
