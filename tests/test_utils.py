from sparkevitune.utils import (
    BYTES_PER_GIB,
    BYTES_PER_MIB,
    ceil_practical_memory,
    optimal_partitions,
    parse_memory_gb,
    parse_size_bytes,
    recommended_shuffle_partitions,
)


def test_memory_parsing_and_ceiling():
    assert parse_memory_gb("512m") == 0.5
    assert ceil_practical_memory(3.27) == 4


def test_backward_compatible_partition_wrapper():
    assert optimal_partitions(1.3) == 20


def test_parse_spark_size_values():
    assert parse_size_bytes("64MB") == 64 * BYTES_PER_MIB
    assert parse_size_bytes("1g") == BYTES_PER_GIB
    assert parse_size_bytes(4096) == 4096


def test_one_gib_shuffle_with_64_mib_target():
    assert recommended_shuffle_partitions(
        1 * BYTES_PER_GIB,
        64 * BYTES_PER_MIB,
        10,
    ) == 20


def test_partition_count_never_rounds_below_requirement():
    # 641 MiB / 64 MiB requires 11 partitions and must round upward to 20.
    assert recommended_shuffle_partitions(
        641 * BYTES_PER_MIB,
        64 * BYTES_PER_MIB,
        10,
    ) == 20
