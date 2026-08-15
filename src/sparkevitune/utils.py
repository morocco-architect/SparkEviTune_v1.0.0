from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

_MEMORY_RE = re.compile(r"^([0-9]*\.?[0-9]+)\s*(g|gb|m|mb|k|kb|b)?$", re.IGNORECASE)
_SIZE_RE = re.compile(
    r"^([0-9]*\.?[0-9]+)\s*(b|kb|kib|k|mb|mib|m|gb|gib|g|tb|tib|t)?$",
    re.IGNORECASE,
)
MEMORY_STEPS_GB = [1, 2, 4, 6, 8, 12, 16, 24, 32, 48, 64, 96, 128]
BYTES_PER_MIB = 1024**2
BYTES_PER_GIB = 1024**3
DEFAULT_ADVISORY_PARTITION_BYTES = 64 * BYTES_PER_MIB


class MemoryParseError(ValueError):
    pass


class SizeParseError(ValueError):
    pass


def parse_memory_gb(value: str | float) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    match = _MEMORY_RE.match(str(value).strip())
    if not match:
        raise MemoryParseError(f"Unrecognized memory value: {value!r}")
    amount = float(match.group(1))
    unit = (match.group(2) or "b").lower()
    if unit in {"g", "gb"}:
        return amount
    if unit in {"m", "mb"}:
        return amount / 1024.0
    if unit in {"k", "kb"}:
        return amount / (1024.0**2)
    return amount / (1024.0**3)


def parse_size_bytes(value: str | float) -> int:
    """Parse a Spark-style byte size while keeping the internal unit unambiguous.

    Spark commonly accepts values such as ``64MB`` and ``256m``. For this
    project's sizing calculations, suffixes are interpreted as binary units so
    that MiB/GiB displays and byte arithmetic remain consistent.
    """
    if isinstance(value, bool):
        raise SizeParseError(f"Boolean is not a valid byte size: {value!r}")
    if isinstance(value, (int, float)):
        if float(value) < 0:
            raise SizeParseError(f"Negative byte size: {value!r}")
        return int(float(value))
    match = _SIZE_RE.match(str(value).strip())
    if not match:
        raise SizeParseError(f"Unrecognized byte size: {value!r}")
    amount = float(match.group(1))
    unit = (match.group(2) or "b").lower()
    multipliers = {
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "kib": 1024,
        "m": BYTES_PER_MIB,
        "mb": BYTES_PER_MIB,
        "mib": BYTES_PER_MIB,
        "g": BYTES_PER_GIB,
        "gb": BYTES_PER_GIB,
        "gib": BYTES_PER_GIB,
        "t": 1024**4,
        "tb": 1024**4,
        "tib": 1024**4,
    }
    return int(amount * multipliers[unit])


def safe_memory_gb(value: Any, default: float, warnings: list[str], field: str) -> float:
    try:
        return parse_memory_gb(value)
    except MemoryParseError as exc:
        warnings.append(f"{field}: {exc}; using {default} GB for this calculation.")
        return default


def safe_size_bytes(value: Any, default: int, warnings: list[str], field: str) -> int:
    try:
        return parse_size_bytes(value)
    except SizeParseError as exc:
        warnings.append(f"{field}: {exc}; using {default} bytes for this calculation.")
        return default


def ceil_practical_memory(required_gb: float) -> int:
    for step in MEMORY_STEPS_GB:
        if step >= required_gb:
            return step
    return int(math.ceil(required_gb / 16.0) * 16)


def recommended_shuffle_partitions(
    reference_shuffle_bytes: int,
    target_partition_bytes: int = DEFAULT_ADVISORY_PARTITION_BYTES,
    minimum_partitions: int = 10,
) -> int:
    """Return a conservative starting partition count.

    ``reference_shuffle_bytes`` must be the maximum shuffle-write volume of one
    successful stage attempt. Reads are not added to writes, and volumes from
    different stages are not accumulated. The result is rounded upward to a
    multiple of ten so the rule never rounds below the byte-derived requirement.
    """
    minimum = max(1, int(minimum_partitions))
    if reference_shuffle_bytes <= 0:
        return int(math.ceil(minimum / 10.0) * 10)
    if target_partition_bytes <= 0:
        raise ValueError("target_partition_bytes must be positive")
    raw = math.ceil(reference_shuffle_bytes / target_partition_bytes)
    bounded = max(minimum, raw)
    return int(math.ceil(bounded / 10.0) * 10)


def optimal_partitions(total_shuffle_gb: float, target_partition_mb: int = 128) -> int:
    """Backward-compatible wrapper for older callers.

    New code should call :func:`recommended_shuffle_partitions` with an exact
    per-stage byte volume. This wrapper treats the input as GiB and does not
    encode the stage aggregation policy.
    """
    return recommended_shuffle_partitions(
        int(max(0.0, total_shuffle_gb) * BYTES_PER_GIB),
        int(target_partition_mb * BYTES_PER_MIB),
        10,
    )


def to_jsonable(value: Any) -> Any:
    if is_dataclass(value):
        return {k: to_jsonable(v) for k, v in asdict(value).items()}
    if isinstance(value, dict):
        return {str(k): to_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(v) for v in value]
    if hasattr(value, "item"):
        return value.item()
    return value


def stable_hash(payload: Any) -> str:
    blob = json.dumps(to_jsonable(payload), sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]


def ensure_parent(path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    return p
