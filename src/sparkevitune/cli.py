from __future__ import annotations

import argparse
import json

from .models import ClusterProfile, WorkloadProfile
from .pipeline import SparkEviTunePipeline
from .utils import to_jsonable


def analyze_main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a Spark event log.")
    parser.add_argument("log")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cores-per-worker", type=int, default=4)
    parser.add_argument("--memory-per-worker-gb", type=float, default=4.0)
    parser.add_argument("--input-size-gb", type=float, default=0.0)
    parser.add_argument("--input-rows", type=int, default=0)
    parser.add_argument("--no-explanation", action="store_true")
    args = parser.parse_args()
    pipeline = SparkEviTunePipeline()
    report = pipeline.analyze(
        args.log,
        ClusterProfile(args.workers, args.cores_per_worker, args.memory_per_worker_gb),
        WorkloadProfile(input_size_gb=args.input_size_gb, input_rows=args.input_rows),
        include_explanation=not args.no_explanation,
    )
    print(json.dumps(to_jsonable(report), indent=2))


def train_main() -> None:
    summary = SparkEviTunePipeline().train_models()
    print(json.dumps(to_jsonable(summary), indent=2))
