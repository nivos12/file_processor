"""
Tests: status and progress API endpoints.

Covers:
- GET /jobs/{id} returns correct status + step info.
- GET /jobs/{id}/progress returns correct percent.
- GET /jobs/{id}/steps returns per-step detail.
- POST /jobs/{id}/cancel transitions PENDING→CANCELLED.
- 404 for unknown job_id.
"""
import io
import json

import pytest

from app.models import Job
from app.workers.pipeline import execute_pipeline


def _upload(client, pipeline=None):
    if pipeline is None:
        pipeline = [{"step": "validate", "params": {}}]
    resp = client.post(
        "/jobs",
        files={"file": ("data.csv", io.BytesIO(b"name,age\nAlice,30\n"), "text/csv")},
        data={"pipeline": json.dumps(pipeline)},
    )
    assert resp.status_code == 202
    return resp.json()["job_id"]


def test_get_job_status_pending(client):
    job_id = _upload(client)
    resp = client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == job_id
    assert body["status"] == "PENDING"


def test_get_job_status_completed(client, db):
    job_id = _upload(client)
    execute_pipeline(job_id)
    db.expire_all()

    resp = client.get(f"/jobs/{job_id}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "COMPLETED"


def test_get_job_steps(client, db):
    pipeline = [
        {"step": "validate", "params": {}},
        {"step": "compress", "params": {"action": "gzip"}},
    ]
    job_id = _upload(client, pipeline)
    execute_pipeline(job_id)
    db.expire_all()

    resp = client.get(f"/jobs/{job_id}/steps")
    assert resp.status_code == 200
    steps = resp.json()
    assert len(steps) == 2
    assert steps[0]["step_type"] == "validate"
    assert steps[0]["status"] == "COMPLETED"
    assert steps[1]["step_type"] == "compress"
    assert steps[1]["status"] == "COMPLETED"
    # Duration is recorded
    assert steps[0]["duration_seconds"] is not None


def test_progress_reflects_step_states(client, db):
    pipeline = [
        {"step": "validate", "params": {}},
        {"step": "compress", "params": {"action": "gzip"}},
    ]
    job_id = _upload(client, pipeline)
    execute_pipeline(job_id)
    db.expire_all()

    resp = client.get(f"/jobs/{job_id}/progress")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_steps"] == 2
    assert body["progress_percent"] == 100.0
    assert body["status"] == "COMPLETED"


def test_cancel_pending_job(client, db):
    job_id = _upload(client)
    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    body = resp.json()
    assert body["cancelled"] is True

    db.expire_all()
    job = db.get(Job, job_id)
    assert job.status == "CANCELLED"
    assert all(s.status == "SKIPPED" for s in job.steps)


def test_cancel_completed_job_is_noop(client, db):
    job_id = _upload(client)
    execute_pipeline(job_id)
    db.expire_all()

    resp = client.post(f"/jobs/{job_id}/cancel")
    assert resp.status_code == 200
    body = resp.json()
    assert body["cancelled"] is False


def test_get_unknown_job_returns_404(client):
    resp = client.get("/jobs/does-not-exist-xyz")
    assert resp.status_code == 404
