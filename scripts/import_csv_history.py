from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from sparkevitune.feature_store import FeatureStore
from sparkevitune.features import FEATURE_COLUMNS


METADATA_COLUMNS = [
    "workload",
    "variant",
    "cluster",
    "repetition",
    "label_anomaly",
    "source_kind",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Import historical runs from a CSV file.")
    parser.add_argument("csv")
    parser.add_argument("--db", default="data/sparkevitune_history.db")
    args = parser.parse_args()
    frame = pd.read_csv(args.csv)
    missing = [column for column in FEATURE_COLUMNS if column not in frame.columns]
    if missing:
        raise SystemExit(f"Missing feature columns: {missing}")
    store = FeatureStore(args.db)
    for index, row in frame.iterrows():
        run_id = str(row.get("run_id", f"imported-{index}"))
        targets = {
            "duration_s": float(row["target_duration_s"]),
            "memory_spill_gb": float(row.get("target_memory_spill_gb", 0.0)),
            "cost": float(row.get("target_cost", 0.0)),
            "oom": float(row.get("target_oom", 0.0)),
        }
        metadata = {"source_csv": str(Path(args.csv))}
        for column in METADATA_COLUMNS:
            if column in frame.columns and not pd.isna(row[column]):
                metadata[column] = row[column].item() if hasattr(row[column], "item") else row[column]
        if "workload" in metadata and "cluster" in metadata:
            metadata["scenario_id"] = f"{metadata['cluster']}::{metadata['workload']}"
        store.upsert_run(
            run_id,
            app_id=str(row.get("app_id", run_id)),
            features={column: float(row[column]) for column in FEATURE_COLUMNS},
            config={},
            targets=targets,
            metadata=metadata,
        )
    print(f"Imported {len(frame)} rows into {args.db}.")


if __name__ == "__main__":
    main()
