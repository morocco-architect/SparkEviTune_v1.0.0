from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib

from .utils import ensure_parent


class ModelRegistry:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def model_path(self, name: str) -> Path:
        return self.directory / f"{name}.joblib"

    def metadata_path(self, name: str) -> Path:
        return self.directory / f"{name}.json"

    def save(self, name: str, model: Any, metadata: dict[str, Any]) -> None:
        joblib.dump(model, self.model_path(name))
        payload = {
            **metadata,
            "name": name,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }
        ensure_parent(self.metadata_path(name)).write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )

    def load(self, name: str) -> Any | None:
        path = self.model_path(name)
        return joblib.load(path) if path.exists() else None

    def metadata(self, name: str) -> dict[str, Any]:
        path = self.metadata_path(name)
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    def status(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for path in self.directory.glob("*.json"):
            result[path.stem] = json.loads(path.read_text(encoding="utf-8"))
        return result
