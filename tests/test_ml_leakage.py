from sparkevitune.features import ANOMALY_FEATURE_COLUMNS, PREDICTION_FEATURE_COLUMNS


def test_prediction_features_exclude_post_run_targets():
    forbidden = {
        "duration_s",
        "memory_spill_gb",
        "disk_spill_gb",
        "shuffle_write_gb",
        "shuffle_read_gb",
        "gc_ratio",
        "max_skew_ratio",
        "num_stages",
        "num_tasks",
    }
    assert forbidden.isdisjoint(PREDICTION_FEATURE_COLUMNS)
    assert forbidden.intersection(ANOMALY_FEATURE_COLUMNS)
