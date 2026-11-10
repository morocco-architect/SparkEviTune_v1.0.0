"""Unit tests for local Event Log / declared profile provenance guard."""

from sparkevitune.models import ClusterProfile
from sparkevitune.profile_guard import check_profile_compatibility


def test_matching_local_profile():
    mismatch, warning = check_profile_compatibility("local[4]", ClusterProfile(workers=1, cores_per_worker=4))
    assert mismatch is False
    assert warning == ""


def test_large_target_profile_is_withheld():
    mismatch, warning = check_profile_compatibility("local[4]", ClusterProfile(workers=100, cores_per_worker=15))
    assert mismatch is True
    assert "withheld" in warning
    assert "100" in warning


def test_same_total_cores_is_not_same_execution_shape():
    mismatch, _ = check_profile_compatibility("local[4]", ClusterProfile(workers=2, cores_per_worker=2))
    assert mismatch is True


def test_local_star_is_fail_closed():
    mismatch, warning = check_profile_compatibility("local[*]", ClusterProfile(workers=1, cores_per_worker=4))
    assert mismatch is True
    assert "withheld" in warning


def test_local_retries_syntax_is_fail_closed():
    mismatch, _ = check_profile_compatibility("local[4,2]", ClusterProfile(workers=1, cores_per_worker=4))
    assert mismatch is True


def test_remote_cluster_is_not_falsely_certified():
    mismatch, warning = check_profile_compatibility("spark://example:7077", ClusterProfile(workers=100, cores_per_worker=15))
    assert mismatch is False
    assert warning == ""
