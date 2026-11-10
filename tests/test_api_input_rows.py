import importlib
import io
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))
api = importlib.import_module("api.main")
from sparkevitune.models import WorkloadProfile


@pytest.fixture
def observed(monkeypatch):
    capture = []
    def fake_analyze(path, cluster, workload, include_explanation):
        capture.append(workload)
        return SimpleNamespace(run_id="isolated-api-test")
    monkeypatch.setattr(api.pipeline, "analyze", fake_analyze)
    monkeypatch.setattr(api, "to_jsonable", lambda report: {"run_id": report.run_id})
    return capture

def request(client, params=None):
    return client.post(
        "/analyze/upload",
        params=params or {},
        files={"file": ("small.jsonl", io.BytesIO(b'{"Event":"test"}\n'), "application/json")},
    )

def test_explicit_positive_input_rows(observed):
    response = request(TestClient(api.app), {"input_rows": 50000})
    assert response.status_code == 200, response.text
    assert len(observed) == 1 and observed[0].input_rows == 50000

def test_omitted_input_rows_defaults_to_zero(observed):
    response = request(TestClient(api.app))
    assert response.status_code == 200, response.text
    assert len(observed) == 1 and observed[0].input_rows == 0

def test_negative_input_rows_rejected_before_analysis(observed):
    response = request(TestClient(api.app), {"input_rows": -1})
    assert response.status_code == 422, response.text
    assert observed == []

def test_model_contract_parity_with_cli_construction(observed):
    # CLI builds WorkloadProfile(input_size_gb=args.input_size_gb, input_rows=args.input_rows).
    expected = WorkloadProfile(input_size_gb=1.25, input_rows=123456)
    response = request(TestClient(api.app), {"input_rows": 123456, "input_size_gb": 1.25})
    assert response.status_code == 200, response.text
    actual = observed[0]
    assert (actual.input_rows, actual.input_size_gb) == (expected.input_rows, expected.input_size_gb)