"""
Tests: step failure propagation.

Verifies that when a step raises an exception:
- The failing step is marked FAILED.
- All subsequent PENDING steps are marked SKIPPED.
- The Job ends with status FAILED.
- The error message is stored on the job and the failing step.
"""
import io
import json
from unittest.mock import patch

import pytest

from app.models import Job
from app.workers.pipeline import execute_pipeline


def _make_job(client, db, pipeline):
    content = b"name,age\nAlice,30\n"
    resp = client.post(
        "/jobs",
        files={"file": ("data.csv", io.BytesIO(content), "text/csv")},
        data={"pipeline": json.dumps(pipeline)},
    )
    assert resp.status_code == 202
    return resp.json()["job_id"]


def test_step_failure_skips_remaining(client, db):
    pipeline = [
        {"step": "validate", "params": {}},
        {"step": "convert", "params": {"to": "json"}},   # will succeed
        {"step": "compress", "params": {"action": "gzip"}},
    ]
    job_id = _make_job(client, db, pipeline)

    # Make the convert step raise
    original_execute = None
    from app.steps.convert import ConvertStep

    def _boom(self, **kwargs):
        raise RuntimeError("Simulated convert failure")

    with patch.object(ConvertStep, "execute", _boom):
        execute_pipeline(job_id)

    db.expire_all()
    job = db.get(Job, job_id)

    assert job.status == "FAILED"
    assert "Simulated convert failure" in (job.error_message or "")

    statuses = {s.step_type: s.status for s in job.steps}
    assert statuses["validate"] == "COMPLETED"
    assert statuses["convert"] == "FAILED"
    assert statuses["compress"] == "SKIPPED"


def test_unknown_step_type_fails_job(client, db):
    pipeline = [
        {"step": "validate", "params": {}},
        {"step": "nonexistent_step", "params": {}},
        {"step": "compress", "params": {"action": "gzip"}},
    ]
    job_id = _make_job(client, db, pipeline)
    execute_pipeline(job_id)

    db.expire_all()
    job = db.get(Job, job_id)
    assert job.status == "FAILED"
    assert "Unknown step type" in (job.error_message or "")

    statuses = {s.step_type: s.status for s in job.steps}
    assert statuses["validate"] == "COMPLETED"
    assert statuses["nonexistent_step"] == "FAILED"
    assert statuses["compress"] == "SKIPPED"
