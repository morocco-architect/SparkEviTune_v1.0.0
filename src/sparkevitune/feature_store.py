from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .features import FEATURE_COLUMNS
from .utils import ensure_parent


class FeatureStore:
    """Small SQLite feature store suitable for a reproducible research prototype."""

    def __init__(self, path: str | Path):
        self.path = ensure_parent(path)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        with closing(self._connect()) as connection:
            with connection:
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS runs (
                        run_id TEXT PRIMARY KEY,
                        created_at TEXT NOT NULL,
                        app_id TEXT,
                        features_json TEXT NOT NULL,
                        config_json TEXT NOT NULL,
                        targets_json TEXT NOT NULL,
                        metadata_json TEXT NOT NULL
                    )
                    """
                )

    def upsert_run(
        self,
        run_id: str,
        app_id: str,
        features: dict[str, float],
        config: dict[str, Any],
        targets: dict[str, float],
        metadata: dict[str, Any] | None = None,
    ) -> None:
        clean_features = {key: float(features.get(key, 0.0)) for key in FEATURE_COLUMNS}
        with closing(self._connect()) as connection:
            with connection:
                connection.execute(
                    """
                    INSERT INTO runs(run_id, created_at, app_id, features_json, config_json, targets_json, metadata_json)
                    VALUES(?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(run_id) DO UPDATE SET
                        created_at=excluded.created_at,
                        app_id=excluded.app_id,
                        features_json=excluded.features_json,
                        config_json=excluded.config_json,
                        targets_json=excluded.targets_json,
                        metadata_json=excluded.metadata_json
                    """,
                    (
                        run_id,
                        datetime.now(timezone.utc).isoformat(),
                        app_id,
                        json.dumps(clean_features, sort_keys=True),
                        json.dumps(config, sort_keys=True),
                        json.dumps(targets, sort_keys=True),
                        json.dumps(metadata or {}, sort_keys=True),
                    ),
                )

    def count(self) -> int:
        with closing(self._connect()) as connection:
            return int(connection.execute("SELECT COUNT(*) FROM runs").fetchone()[0])

    def dataframe(self) -> pd.DataFrame:
        with closing(self._connect()) as connection:
            rows = connection.execute("SELECT * FROM runs ORDER BY created_at").fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            features = json.loads(row["features_json"])
            targets = json.loads(row["targets_json"])
            metadata = json.loads(row["metadata_json"])
            record = {**features, **{f"target_{k}": v for k, v in targets.items()}}
            record["run_id"] = row["run_id"]
            record["app_id"] = row["app_id"]
            for key, value in metadata.items():
                if isinstance(value, (str, int, float, bool)) or value is None:
                    record[f"meta_{key}"] = value
            records.append(record)
        return pd.DataFrame(records)

    def export_csv(self, path: str | Path) -> Path:
        output = ensure_parent(path)
        self.dataframe().to_csv(output, index=False)
        return output
