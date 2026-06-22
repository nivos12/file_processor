"""
Tests: upload endpoint + job creation.

Covers:
- Successful upload → 202, job_id returned.
- Oversized file → 413.
- Unsupported content type → 415.
- Unsupported extension → 415.
- Invalid pipeline JSON → 422.
- Empty pipeline → 422.
- Verifies DB has Job + JobStep rows after upload.
"""
import io
import json

import pytest

from app.models import Job, JobStep


def _upload(client, content: bytes, filename: str = "data.csv",
            content_type: str = "text/csv",
            pipeline: list | None = None):
    if pipeline is None:
        pipeline = [{"step": "validate", "params": {}}]
    return client.post(
        "/jobs",
        files={"file": (filename, io.BytesIO(content), content_type)},
        data={"pipeline": json.dumps(pipeline)},
    )


# ── Happy path ─────────────────────────────────────────────────────────────

def test_upload_creates_job(client, db, sample_csv):
    content = sample_csv.read_bytes()
    resp = _upload(client, content)
    assert resp.status_code == 202
    body = resp.json()
    assert "job_id" in body
    assert body["status"] == "PENDING"

    job = db.get(Job, body["job_id"])
    assert job is not None
    assert job.status == "PENDING"
    assert len(job.steps) == 1
    assert job.steps[0].step_type == "validate"


def test_upload_multi_step_pipeline(client, db, sample_csv):
    pipeline = [
        {"step": "validate", "params": {}},
        {"step": "compress", "params": {"action": "gzip"}},
    ]
    resp = _upload(client, sample_csv.read_bytes(), pipeline=pipeline)
    assert resp.status_code == 202
    job_id = resp.json()["job_id"]
    job = db.get(Job, job_id)
    assert len(job.steps) == 2


# ── Rejection cases ────────────────────────────────────────────────────────

def test_upload_oversized_file(client, monkeypatch):
    from app import config
    monkeypatch.setattr(config.settings, "max_upload_bytes", 10)
    # also patch the imported value in the module
    import app.api.jobs as jobs_module
    monkeypatch.setattr(jobs_module, "_MAX_BYTES", 10)

    content = b"name,age\nAlice,30\n" * 100  # > 10 bytes
    resp = _upload(client, content)
    assert resp.status_code == 413


def test_upload_unsupported_content_type(client):
    resp = _upload(client, b"<html/>", filename="page.html", content_type="text/html")
    assert resp.status_code == 415


def test_upload_unsupported_extension(client):
    resp = _upload(client, b"data", filename="data.exe", content_type="application/octet-stream")
    assert resp.status_code == 415


def test_upload_invalid_pipeline_json(client):
    resp = client.post(
        "/jobs",
        files={"file": ("data.csv", io.BytesIO(b"a,b\n1,2"), "text/csv")},
        data={"pipeline": "not-json"},
    )
    assert resp.status_code == 422


def test_upload_empty_pipeline(client):
    resp = client.post(
        "/jobs",
        files={"file": ("data.csv", io.BytesIO(b"a,b\n1,2"), "text/csv")},
        data={"pipeline": "[]"},
    )
    assert resp.status_code == 422
