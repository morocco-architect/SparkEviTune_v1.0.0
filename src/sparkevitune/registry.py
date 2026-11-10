from __future__ import annotations

import json
import os
import tempfile
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import joblib


class ModelRegistry:
    """Fail-closed model storage with per-model cross-process locking.

    The lock guards registry-mediated access only. External direct reads/writes
    to .joblib/.json/.eligibility files bypass this protection.
    """

    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def model_path(self, name: str) -> Path:
        return self.directory / f"{name}.joblib"

    def metadata_path(self, name: str) -> Path:
        return self.directory / f"{name}.json"

    def eligibility_path(self, name: str) -> Path:
        return self.directory / f"{name}.eligibility"

    @contextmanager
    def _model_lock(self, name: str, timeout: float = 30.0) -> Iterator[None]:
        """Exclusive OS-level lock for a named model, with bounded waiting.

        The persistent .lock file MUST NOT be removed while any process might
        still use this registry: removing it can create distinct lock inodes.
        """
        lock_path = self.directory / f".{name}.lock"
        with open(lock_path, "a+b") as lock_file:
            # Windows msvcrt.locking requires an addressable byte to lock.
            if lock_file.seek(0, os.SEEK_END) == 0:
                lock_file.write(b"\0")
                lock_file.flush()
            lock_file.seek(0)
            deadline = time.monotonic() + timeout
            if os.name == "nt":
                import msvcrt

                def acquire() -> None:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_NBLCK, 1)

                def release() -> None:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                def acquire() -> None:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

                def release() -> None:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)

            while True:
                try:
                    acquire()
                    break
                except (OSError, BlockingIOError):
                    if time.monotonic() >= deadline:
                        raise TimeoutError(f"Registry lock timeout for model {name!r}") from None
                    time.sleep(0.025)
            try:
                yield
            finally:
                release()

    def _set_eligibility(self, name: str, eligible: bool) -> None:
        """Atomic eligibility gate update. Caller must hold _model_lock."""
        target = self.eligibility_path(name)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{name}.", suffix=".gate.tmp", dir=self.directory
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write("ELIGIBLE\n" if eligible else "INELIGIBLE\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def invalidate(self, names) -> None:
        for name in names:
            with self._model_lock(name):
                self._set_eligibility(name, False)

    def _eligible(self, name: str) -> bool:
        gate = self.eligibility_path(name)
        if not gate.is_file():
            return False  # Legacy snapshots are not auto-trusted.
        try:
            return gate.read_text(encoding="utf-8") == "ELIGIBLE\n"
        except OSError:
            return False

    def save(self, name: str, model: Any, metadata: dict[str, Any]) -> None:
        with self._model_lock(name):
            # Deny load before attempting to replace either published file.
            self._set_eligibility(name, False)
            payload = {
                **metadata,
                "name": name,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            }
            model_fd, model_tmp = tempfile.mkstemp(
                prefix=f".{name}.", suffix=".model.tmp", dir=self.directory
            )
            os.close(model_fd)
            meta_fd, meta_tmp = tempfile.mkstemp(
                prefix=f".{name}.", suffix=".meta.tmp", dir=self.directory
            )
            os.close(meta_fd)
            try:
                joblib.dump(model, model_tmp)
                with open(model_tmp, "rb+") as stream:
                    os.fsync(stream.fileno())
                with open(meta_tmp, "w", encoding="utf-8") as stream:
                    json.dump(payload, stream, indent=2, sort_keys=True)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(model_tmp, self.model_path(name))
                os.replace(meta_tmp, self.metadata_path(name))
                self._set_eligibility(name, True)
            finally:
                for temporary in (model_tmp, meta_tmp):
                    if os.path.exists(temporary):
                        os.unlink(temporary)

    def load(self, name: str) -> Any | None:
        try:
            with self._model_lock(name):
                if not self._eligible(name):
                    return None
                path = self.model_path(name)
                return joblib.load(path) if path.exists() else None
        except TimeoutError:
            return None

    def metadata(self, name: str) -> dict[str, Any]:
        try:
            with self._model_lock(name):
                if not self._eligible(name):
                    return {}
                path = self.metadata_path(name)
                return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        except TimeoutError:
            return {}

    def load_with_metadata(self, name: str) -> tuple[Any | None, dict[str, Any]]:
        """Read a consistent model/metadata pair within one lock acquisition."""
        try:
            with self._model_lock(name):
                if not self._eligible(name):
                    return None, {}
                model_path = self.model_path(name)
                meta_path = self.metadata_path(name)
                if not model_path.is_file() or not meta_path.is_file():
                    return None, {}
                return joblib.load(model_path), json.loads(meta_path.read_text(encoding="utf-8"))
        except TimeoutError:
            return None, {}

    def status(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for path in self.directory.glob("*.json"):
            name = path.stem
            try:
                with self._model_lock(name):
                    if not path.is_file():
                        continue
                    result[name] = {
                        **json.loads(path.read_text(encoding="utf-8")),
                        "eligible": self._eligible(name),
                    }
            except TimeoutError:
                result[name] = {"eligible": False, "status": "lock_timeout"}
        return result
