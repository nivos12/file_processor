"""
Tests: end-to-end pipeline execution.

We bypass the RQ queue and call execute_pipeline() directly with a real SQLite
DB so we test the actual step logic without requiring Redis.

Covers:
- validate → transform → convert → compress pipeline on a CSV file.
- filter step: CSV allowlist filtering and JSON allowlist filtering.
- enrich step: metadata envelope around CSV and JSON records.
- Each step transitions PENDING → RUNNING → COMPLETED.
- Job ends COMPLETED with output_file_id set.
"""
import io
import json
from pathlib import Path

import pytest

from app.models import Job, JobStep
from app.workers.pipeline import execute_pipeline


def _make_job(client, db, content: bytes, pipeline: list, filename="data.csv", ct="text/csv"):
    resp = client.post(
        "/jobs",
        files={"file": (filename, io.BytesIO(content), ct)},
        data={"pipeline": json.dumps(pipeline)},
    )
    assert resp.status_code == 202, resp.text
    return resp.json()["job_id"]


def test_validate_only_pipeline(client, db, sample_csv):
    job_id = _make_job(client, db, sample_csv.read_bytes(), [
        {"step": "validate", "params": {}}
    ])
    # Job is PENDING because enqueue was no-op'd — run pipeline directly
    execute_pipeline(job_id)

    db.expire_all()
    job = db.get(Job, job_id)
    assert job.status == "COMPLETED"
    assert job.output_file_id is not None
    assert job.steps[0].status == "COMPLETED"
    assert job.steps[0].duration_seconds is not None


def test_validate_transform_convert_compress(client, db, sample_csv):
    pipeline = [
        {"step": "validate", "params": {"expected_type": "csv"}},
        {"step": "transform", "params": {
            "filter_field": "city",
            "filter_value": "NYC",
            "filter_op": "eq",
            "select_fields": ["name", "city"],
        }},
        {"step": "convert", "params": {"to": "json"}},
        {"step": "compress", "params": {"action": "gzip"}},
    ]
    job_id = _make_job(client, db, sample_csv.read_bytes(), pipeline)
    execute_pipeline(job_id)

    db.expire_all()
    job = db.get(Job, job_id)
    assert job.status == "COMPLETED", job.error_message
    assert len(job.steps) == 4
    for step in job.steps:
        assert step.status == "COMPLETED", f"Step {step.step_type} is {step.status}: {step.error_message}"

    # Output should be a gzip file
    from app.models import FileReference
    out_ref = db.get(FileReference, job.output_file_id)
    assert out_ref is not None
    assert Path(out_ref.storage_path).exists()


def test_csv_transform_filters_rows(tmp_path, db):
    """Unit-level: TransformStep filters CSV rows correctly."""
    from app.steps.transform import TransformStep
    import csv

    src = tmp_path / "input.csv"
    src.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\nCarol,35,NYC\n")
    dst = tmp_path / "output.csv"

    step = TransformStep()
    step.execute(
        input_path=src,
        output_path=dst,
        params={"filter_field": "city", "filter_value": "NYC"},
        job_id="test-job",
        step_index=0,
        db=db,
    )

    with dst.open() as f:
        rows = list(csv.DictReader(f))
    assert len(rows) == 2
    assert all(r["city"] == "NYC" for r in rows)


def test_json_to_csv_convert(tmp_path, db):
    """Unit-level: ConvertStep json→csv round-trip."""
    from app.steps.convert import ConvertStep

    src = tmp_path / "data.json"
    src.write_text(json.dumps([{"x": 1, "y": 2}, {"x": 3, "y": 4}]))
    dst = tmp_path / "out.csv"

    step = ConvertStep()
    step.execute(
        input_path=src,
        output_path=dst,
        params={"to": "csv"},
        job_id="test-job",
        step_index=0,
        db=db,
    )

    import csv
    out = dst.with_suffix(".csv")
    rows = list(csv.DictReader(out.open()))
    assert len(rows) == 2
    assert rows[0]["x"] == "1"


# ── FilterStep ─────────────────────────────────────────────────────────────

def test_filter_csv_allowlist(tmp_path, db):
    """FilterStep keeps only rows whose field value is in valid_values (CSV)."""
    from app.steps.filter import FilterStep
    import csv

    src = tmp_path / "data.csv"
    src.write_text("name,city\nAlice,NYC\nBob,LA\nCarol,NYC\nDave,Chicago\n")
    dst = tmp_path / "out.csv"

    FilterStep().execute(
        input_path=src,
        output_path=dst,
        params={"field": "city", "valid_values": ["NYC", "Chicago"]},
        job_id="test-job",
        step_index=0,
        db=db,
    )

    rows = list(csv.DictReader(dst.open()))
    assert len(rows) == 3
    assert {r["city"] for r in rows} == {"NYC", "Chicago"}


def test_filter_json_allowlist(tmp_path, db):
    """FilterStep keeps only records whose field value is in valid_values (JSON)."""
    from app.steps.filter import FilterStep

    src = tmp_path / "data.json"
    src.write_text(json.dumps([
        {"name": "Alice", "status": "active"},
        {"name": "Bob",   "status": "inactive"},
        {"name": "Carol", "status": "active"},
    ]))
    dst = tmp_path / "out.json"

    FilterStep().execute(
        input_path=src,
        output_path=dst,
        params={"field": "status", "valid_values": ["active"]},
        job_id="test-job",
        step_index=0,
        db=db,
    )

    result = json.loads(dst.read_text())
    assert len(result) == 2
    assert all(r["status"] == "active" for r in result)


def test_filter_no_valid_values_raises(tmp_path, db):
    """FilterStep raises if valid_values is empty."""
    from app.steps.filter import FilterStep
    import pytest

    src = tmp_path / "data.csv"
    src.write_text("name,city\nAlice,NYC\n")
    dst = tmp_path / "out.csv"

    with pytest.raises(ValueError, match="valid_values"):
        FilterStep().execute(
            input_path=src,
            output_path=dst,
            params={"field": "city", "valid_values": []},
            job_id="test-job",
            step_index=0,
            db=db,
        )


# ── EnrichStep ─────────────────────────────────────────────────────────────

def test_enrich_csv_produces_json_envelope(tmp_path, db):
    """EnrichStep wraps CSV records in {_metadata, data} JSON output."""
    from app.steps.enrich import EnrichStep

    src = tmp_path / "data.csv"
    src.write_text("name,age\nAlice,30\nBob,25\n")
    dst = tmp_path / "out.csv"  # step adjusts extension to .json

    result = EnrichStep().execute(
        input_path=src,
        output_path=dst,
        params={},
        job_id="test-job",
        step_index=0,
        db=db,
    )

    actual_path = Path(result["output_path"])
    assert actual_path.suffix == ".json"
    output = json.loads(actual_path.read_text())
    assert "data" in output
    assert "_metadata" in output
    assert len(output["data"]) == 2
    assert output["_metadata"]["row_count"] == 2
    assert output["_metadata"]["job_id"] == "test-job"


def test_enrich_json_includes_all_fields(tmp_path, db):
    """EnrichStep includes all metadata fields by default for JSON input."""
    from app.steps.enrich import EnrichStep

    src = tmp_path / "data.json"
    src.write_text(json.dumps([{"x": 1}, {"x": 2}]))
    dst = tmp_path / "out.json"

    EnrichStep().execute(
        input_path=src,
        output_path=dst,
        params={},
        job_id="test-job",
        step_index=0,
        db=db,
    )

    output = json.loads(dst.read_text())
    meta = output["_metadata"]
    assert "job_id" in meta
    assert "processing_time" in meta
    assert "row_count" in meta
    assert meta["row_count"] == 2
    assert "file_size_bytes" in meta


def test_enrich_include_subset(tmp_path, db):
    """EnrichStep only adds the fields listed in the include param."""
    from app.steps.enrich import EnrichStep

    src = tmp_path / "data.json"
    src.write_text(json.dumps([{"v": 1}]))
    dst = tmp_path / "out.json"

    EnrichStep().execute(
        input_path=src,
        output_path=dst,
        params={"include": ["row_count", "job_id"]},
        job_id="test-job",
        step_index=0,
        db=db,
    )

    output = json.loads(dst.read_text())
    meta = output["_metadata"]
    assert set(meta.keys()) == {"row_count", "job_id"}


def test_filter_then_enrich_pipeline(client, db, sample_csv):
    """End-to-end: filter CSV rows then enrich with metadata."""
    pipeline = [
        {"step": "filter", "params": {"field": "city", "valid_values": ["NYC"]}},
        {"step": "enrich", "params": {"include": ["job_id", "row_count"]}},
    ]
    job_id = _make_job(client, db, sample_csv.read_bytes(), pipeline)
    execute_pipeline(job_id)

    db.expire_all()
    job = db.get(Job, job_id)
    assert job.status == "COMPLETED", job.error_message

    from app.models import FileReference
    out_ref = db.get(FileReference, job.output_file_id)
    output = json.loads(Path(out_ref.storage_path).read_text())
    assert output["_metadata"]["job_id"] == job_id
    # sample_csv has Alice+Carol in NYC, Bob in LA → 2 rows after filter
    assert output["_metadata"]["row_count"] == 2
    assert len(output["data"]) == 2
