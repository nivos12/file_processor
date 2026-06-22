"""
Tests: result download endpoint.

Covers:
- 202 when job is still PENDING.
- 200 with file content when job is COMPLETED.
- 410 when output file has expired.
- 422 when job FAILED.
"""
import io
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.models import FileReference, Job
from app.workers.pipeline import execute_pipeline


def _upload_and_run(client, db, pipeline=None):
    if pipeline is None:
        pipeline = [{"step": "validate", "params": {}}]
    resp = client.post(
        "/jobs",
        files={"file": ("data.csv", io.BytesIO(b"name,age\nAlice,30\n"), "text/csv")},
        data={"pipeline": json.dumps(pipeline)},
    )
    job_id = resp.json()["job_id"]
    execute_pipeline(job_id)
    db.expire_all()
    return job_id


def test_result_not_ready_returns_202(client):
    resp = client.post(
        "/jobs",
        files={"file": ("data.csv", io.BytesIO(b"name,age\nAlice,30\n"), "text/csv")},
        data={"pipeline": json.dumps([{"step": "validate", "params": {}}])},
    )
    job_id = resp.json()["job_id"]
    # Don't run the pipeline — job stays PENDING
    result_resp = client.get(f"/jobs/{job_id}/result")
    assert result_resp.status_code == 202


def test_result_download_success(client, db):
    job_id = _upload_and_run(client, db)
    resp = client.get(f"/jobs/{job_id}/result")
    assert resp.status_code == 200
    # Validate step passes through input; output is the CSV
    assert len(resp.content) > 0


def test_result_expired_returns_410(client, db):
    job_id = _upload_and_run(client, db)
    job = db.get(Job, job_id)

    # Expire the output file
    out_ref = db.get(FileReference, job.output_file_id)
    out_ref.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db.commit()

    resp = client.get(f"/jobs/{job_id}/result")
    assert resp.status_code == 410


def test_result_failed_job_returns_422(client, db):
    from unittest.mock import patch
    from app.steps.validate import ValidateStep

    def _boom(self, **kwargs):
        raise RuntimeError("validation boom")

    resp = client.post(
        "/jobs",
        files={"file": ("data.csv", io.BytesIO(b"name,age\nAlice,30\n"), "text/csv")},
        data={"pipeline": json.dumps([{"step": "validate", "params": {}}])},
    )
    job_id = resp.json()["job_id"]

    with patch.object(ValidateStep, "execute", _boom):
        execute_pipeline(job_id)
    db.expire_all()

    resp = client.get(f"/jobs/{job_id}/result")
    assert resp.status_code == 422
