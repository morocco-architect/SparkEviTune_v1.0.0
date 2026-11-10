"""Isolated registry test; run under spawn on Windows and Linux.

This test uses toy Python payloads. No Spark or real ML training occurs.
"""
from __future__ import annotations

import multiprocessing as mp

import pytest

from sparkevitune.registry import ModelRegistry


def _writer(directory: str, label: str, loops: int, queue) -> None:
    try:
        registry = ModelRegistry(directory)
        for i in range(loops):
            registry.save("shared", {"writer": label, "index": i}, {"writer": label, "index": i})
        queue.put(None)
    except Exception as exc:  # noqa: BLE001
        queue.put(repr(exc))


def _reader(directory: str, loops: int, queue) -> None:
    try:
        registry = ModelRegistry(directory)
        for _ in range(loops):
            model, metadata = registry.load_with_metadata("shared")
            if model is not None and (model["writer"], model["index"]) != (
                metadata["writer"], metadata["index"]
            ):
                raise AssertionError("Cross-version model/metadata pair")
        queue.put(None)
    except Exception as exc:  # noqa: BLE001
        queue.put(repr(exc))


def test_legacy_model_without_gate_is_denied(tmp_path):
    import joblib

    joblib.dump({"v": 1}, tmp_path / "legacy.joblib")
    registry = ModelRegistry(tmp_path)
    assert registry.load("legacy") is None
    assert registry.metadata("legacy") == {}


def test_failed_save_keeps_gate_closed(tmp_path, monkeypatch):
    registry = ModelRegistry(tmp_path)
    registry.save("shared", {"v": 1}, {"v": 1})

    def fail_dump(*args, **kwargs):
        raise RuntimeError("simulated failure")

    monkeypatch.setattr("sparkevitune.registry.joblib.dump", fail_dump)
    with pytest.raises(RuntimeError, match="simulated failure"):
        registry.save("shared", {"v": 2}, {"v": 2})
    assert registry.load("shared") is None
    assert registry.metadata("shared") == {}


def test_spawn_two_writers_one_reader(tmp_path):
    registry = ModelRegistry(tmp_path)
    registry.save("shared", {"writer": "initial", "index": 0}, {"writer": "initial", "index": 0})
    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    jobs = [
        ctx.Process(target=_writer, args=(str(tmp_path), "A", 15, queue)),
        ctx.Process(target=_writer, args=(str(tmp_path), "B", 15, queue)),
        ctx.Process(target=_reader, args=(str(tmp_path), 90, queue)),
    ]
    for job in jobs:
        job.start()
    try:
        for job in jobs:
            job.join(timeout=35)
        assert all(not job.is_alive() for job in jobs), "process timeout/deadlock"
        assert all(job.exitcode == 0 for job in jobs)
        errors = [queue.get(timeout=3) for _ in jobs]
        assert errors == [None, None, None]
        model, metadata = registry.load_with_metadata("shared")
        assert model["writer"] == metadata["writer"]
        assert model["index"] == metadata["index"]
    finally:
        for job in jobs:
            if job.is_alive():
                job.terminate()
                job.join(timeout=3)
