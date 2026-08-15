from __future__ import annotations

import argparse
import json

from sparkevitune.models import ClusterProfile, WorkloadProfile
from sparkevitune.pipeline import SparkEviTunePipeline
from sparkevitune.utils import to_jsonable


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("log")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--cores", type=int, default=4)
    parser.add_argument("--memory", type=float, default=4.0)
    parser.add_argument("--input-size", type=float, default=0.0)
    args = parser.parse_args()
    report = SparkEviTunePipeline().analyze(
        args.log,
        ClusterProfile(args.workers, args.cores, args.memory),
        WorkloadProfile(input_size_gb=args.input_size),
    )
    print(json.dumps(to_jsonable(report), indent=2))
