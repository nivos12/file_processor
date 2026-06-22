"""
End-to-end tests: full pipeline execution with structured-log verification.

These tests simulate real user usage — upload a file via POST /jobs, run
execute_pipeline() directly (no Redis), then inspect both the output file
and the JSON log lines emitted during processing.

Log capture
-----------
structlog uses stdlib logging with a ProcessorFormatter that renders each
event dict to a JSON string.  We attach an extra StreamHandler backed by a
StringIO buffer, using the *same* ProcessorFormatter that's already wired to
stdout.  This avoids the fd-redirect unreliability of capfd and the sys.stdout-
replacement issue of capsys, and gives us clean, parseable JSON records.

The `log_capture` fixture yields a callable `logs(job_id)` that returns the
list of parsed dicts emitted for that job so far.
"""
import csv
import io
import json
import logging
from pathlib import Path

import pytest

from app.models import FileReference, Job
from app.workers.pipeline import execute_pipeline


# ── Log-capture fixture ────────────────────────────────────────────────────────

@pytest.fixture
def log_capture():
    """
    Yield a `logs(job_id)` callable that returns parsed structlog JSON records.

    Attaches a StringIO StreamHandler with the existing ProcessorFormatter to
    the root logger for the duration of the test, then removes it.
    """
    buf = io.StringIO()
    root = logging.getLogger()

    # Borrow the ProcessorFormatter from whichever handler already has one.
    formatter = next(
        (h.formatter for h in root.handlers if h.formatter is not None),
        None,
    )
    handler = logging.StreamHandler(buf)
    if formatter:
        handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    root.addHandler(handler)

    def logs(job_id: str | None = None) -> list[dict]:
        buf.seek(0)
        records: list[dict] = []
        for line in buf.read().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                pass
        if job_id is not None:
            records = [r for r in records if r.get("job_id") == job_id]
        return records

    yield logs

    root.removeHandler(handler)
    handler.close()


# ── Fixture files ──────────────────────────────────────────────────────────────

@pytest.fixture
def sales_csv(tmp_path) -> Path:
    """8-row sales CSV covering multiple regions, products, and statuses."""
    rows = [
        "id,region,product,quantity,revenue,status",
        "1,East,Widget,100,5000.00,shipped",
        "2,West,Gadget,50,3500.00,pending",
        "3,East,Widget,200,10000.00,shipped",
        "4,North,Doohickey,75,2250.00,cancelled",
        "5,West,Widget,30,1500.00,shipped",
        "6,East,Gadget,120,8400.00,pending",
        "7,North,Widget,60,3000.00,shipped",
        "8,South,Doohickey,90,2700.00,shipped",
    ]
    p = tmp_path / "sales.csv"
    p.write_text("\n".join(rows) + "\n")
    return p


@pytest.fixture
def product_json(tmp_path) -> Path:
    """5-record product JSON with category and active/inactive statuses."""
    products = [
        {"sku": "W-001", "name": "Widget Pro",     "category": "hardware",    "status": "active",   "price": 49.99},
        {"sku": "G-002", "name": "Gadget Plus",    "category": "electronics", "status": "inactive", "price": 129.99},
        {"sku": "D-003", "name": "Doohickey Lite", "category": "hardware",    "status": "active",   "price": 19.99},
        {"sku": "W-004", "name": "Widget Nano",    "category": "hardware",    "status": "active",   "price": 29.99},
        {"sku": "G-005", "name": "Gadget Mini",    "category": "electronics", "status": "active",   "price": 89.99},
    ]
    p = tmp_path / "products.json"
    p.write_text(json.dumps(products, indent=2))
    return p


# ── Upload + run helper ────────────────────────────────────────────────────────

def _run(
    client, db,
    content: bytes,
    pipeline: list,
    filename: str = "data.csv",
    ct: str = "text/csv",
) -> tuple[str, Job]:
    resp = client.post(
        "/jobs",
        files={"file": (filename, io.BytesIO(content), ct)},
        data={"pipeline": json.dumps(pipeline)},
    )
    assert resp.status_code == 202, resp.text
    job_id = resp.json()["job_id"]
    execute_pipeline(job_id)
    db.expire_all()
    return job_id, db.get(Job, job_id)


def _events(records: list[dict]) -> list[str]:
    return [r.get("event", "") for r in records]


# ── Log lifecycle ──────────────────────────────────────────────────────────────

class TestLogLifecycle:
    """Verify the sequence of log events emitted for each job."""

    def test_single_step_emits_full_lifecycle(self, client, db, sales_csv, log_capture):
        job_id, job = _run(client, db, sales_csv.read_bytes(), [
            {"step": "validate", "params": {"expected_type": "csv"}},
        ])
        assert job.status == "COMPLETED"

        events = _events(log_capture(job_id))
        assert "pipeline_started"   in events
        assert "step_started"       in events
        assert "step_completed"     in events
        assert "pipeline_completed" in events

    def test_pipeline_started_carries_total_steps(self, client, db, sales_csv, log_capture):
        pipeline = [
            {"step": "validate", "params": {}},
            {"step": "filter",   "params": {"field": "status", "valid_values": ["shipped"]}},
        ]
        job_id, _ = _run(client, db, sales_csv.read_bytes(), pipeline)

        records = log_capture(job_id)
        started = next(r for r in records if r.get("event") == "pipeline_started")
        assert started["total_steps"] == 2

    def test_multi_step_counts_match_pipeline_length(self, client, db, sales_csv, log_capture):
        pipeline = [
            {"step": "validate", "params": {}},
            {"step": "filter",   "params": {"field": "region", "valid_values": ["East"]}},
            {"step": "convert",  "params": {"to": "json"}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)
        assert job.status == "COMPLETED"

        events = _events(log_capture(job_id))
        assert events.count("step_started")   == 3
        assert events.count("step_completed") == 3

    def test_all_job_log_records_carry_job_id(self, client, db, sales_csv, log_capture):
        pipeline = [
            {"step": "validate", "params": {}},
            {"step": "filter",   "params": {"field": "status", "valid_values": ["shipped"]}},
        ]
        job_id, _ = _run(client, db, sales_csv.read_bytes(), pipeline)

        records = log_capture(job_id)
        # pipeline_started + 2×step_started + 2×step_completed + pipeline_completed = 6 minimum
        assert len(records) >= 6
        for rec in records:
            assert rec.get("job_id") == job_id

    def test_step_records_carry_index_and_type(self, client, db, sales_csv, log_capture):
        pipeline = [
            {"step": "validate", "params": {}},
            {"step": "filter",   "params": {"field": "status", "valid_values": ["shipped"]}},
        ]
        job_id, _ = _run(client, db, sales_csv.read_bytes(), pipeline)

        records = log_capture(job_id)
        step_recs = [r for r in records if r.get("event") in ("step_started", "step_completed")]
        assert len(step_recs) == 4  # 2 started + 2 completed
        for rec in step_recs:
            assert "step_index" in rec, f"missing step_index in {rec}"
            assert "step_type"  in rec, f"missing step_type in {rec}"

    def test_step_completed_logs_duration_and_output_size(self, client, db, sales_csv, log_capture):
        job_id, _ = _run(client, db, sales_csv.read_bytes(), [
            {"step": "filter", "params": {"field": "status", "valid_values": ["shipped"]}},
        ])
        records = log_capture(job_id)
        completed = next(r for r in records if r.get("event") == "step_completed")
        assert completed["duration"] >= 0
        assert completed["output_size"] > 0

    def test_log_records_are_valid_json_with_required_fields(self, client, db, sales_csv, log_capture):
        """Every pipeline log record has 'event' and 'timestamp' fields."""
        job_id, _ = _run(client, db, sales_csv.read_bytes(), [
            {"step": "validate", "params": {}},
        ])
        records = log_capture(job_id)
        assert records, "no log records found for job"
        for rec in records:
            assert "event"     in rec, f"missing 'event' in {rec}"
            assert "timestamp" in rec, f"missing 'timestamp' in {rec}"


# ── Filter step ────────────────────────────────────────────────────────────────

class TestFilterE2E:
    def test_csv_filter_shipped_keeps_correct_rows(self, client, db, sales_csv, log_capture):
        """5 of 8 rows have status=shipped; verify output content and log lifecycle."""
        job_id, job = _run(client, db, sales_csv.read_bytes(), [
            {"step": "filter", "params": {"field": "status", "valid_values": ["shipped"]}},
        ])
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        rows = list(csv.DictReader(Path(out_ref.storage_path).open()))
        assert len(rows) == 5
        assert all(r["status"] == "shipped" for r in rows)

        assert "pipeline_completed" in _events(log_capture(job_id))

    def test_filter_step_logs_rows_in_and_out(self, client, db, sales_csv, log_capture):
        """filter_completed log records the input and output row counts."""
        job_id, _ = _run(client, db, sales_csv.read_bytes(), [
            {"step": "filter", "params": {"field": "region", "valid_values": ["East"]}},
        ])
        records = log_capture(job_id)
        filter_rec = next(r for r in records if r.get("event") == "filter_completed")
        assert filter_rec["rows_in"]  == 8
        assert filter_rec["rows_out"] == 3  # East rows: ids 1, 3, 6

    def test_csv_filter_multi_value_allowlist(self, client, db, sales_csv, log_capture):
        """Filtering East+West keeps 5 rows (ids 1, 2, 3, 5, 6)."""
        job_id, job = _run(client, db, sales_csv.read_bytes(), [
            {"step": "filter", "params": {"field": "region", "valid_values": ["East", "West"]}},
        ])
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        rows = list(csv.DictReader(Path(out_ref.storage_path).open()))
        assert len(rows) == 5
        assert all(r["region"] in {"East", "West"} for r in rows)

    def test_json_filter_active_products(self, client, db, product_json, log_capture):
        """Filter JSON: active products (4 of 5)."""
        job_id, job = _run(
            client, db, product_json.read_bytes(),
            [{"step": "filter", "params": {"field": "status", "valid_values": ["active"]}}],
            filename="products.json", ct="application/json",
        )
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        result = json.loads(Path(out_ref.storage_path).read_text())
        assert len(result) == 4
        assert all(r["status"] == "active" for r in result)

        assert "pipeline_completed" in _events(log_capture(job_id))

    def test_json_filter_by_category(self, client, db, product_json, log_capture):
        """Filter JSON: hardware category produces 3 records (W-001, D-003, W-004)."""
        job_id, job = _run(
            client, db, product_json.read_bytes(),
            [{"step": "filter", "params": {"field": "category", "valid_values": ["hardware"]}}],
            filename="products.json", ct="application/json",
        )
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        result = json.loads(Path(out_ref.storage_path).read_text())
        assert len(result) == 3
        assert {r["sku"] for r in result} == {"W-001", "D-003", "W-004"}


# ── Enrich step ────────────────────────────────────────────────────────────────

class TestEnrichE2E:
    def test_enrich_csv_produces_json_envelope_with_correct_row_count(
        self, client, db, sales_csv, log_capture
    ):
        job_id, job = _run(client, db, sales_csv.read_bytes(), [
            {"step": "enrich", "params": {"include": ["job_id", "row_count", "file_size_bytes"]}},
        ])
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        assert out_ref.storage_path.endswith(".json")
        output = json.loads(Path(out_ref.storage_path).read_text())

        meta = output["_metadata"]
        assert meta["job_id"]       == job_id
        assert meta["row_count"]    == 8
        assert "file_size_bytes"    in meta
        assert "processing_time"    not in meta   # not requested
        assert len(output["data"])  == 8

        assert "step_completed" in _events(log_capture(job_id))

    def test_enrich_row_count_reflects_upstream_filter(self, client, db, sales_csv, log_capture):
        """row_count must match filtered rows (3), not the original file (8)."""
        job_id, job = _run(client, db, sales_csv.read_bytes(), [
            {"step": "filter", "params": {"field": "region", "valid_values": ["East"]}},
            {"step": "enrich", "params": {"include": ["row_count", "job_id"]}},
        ])
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        output = json.loads(Path(out_ref.storage_path).read_text())
        # East rows: ids 1, 3, 6
        assert output["_metadata"]["row_count"] == 3
        assert len(output["data"]) == 3

        events = _events(log_capture(job_id))
        assert events.count("step_completed") == 2  # filter + enrich

    def test_enrich_includes_all_metadata_fields_by_default(
        self, client, db, sales_csv, log_capture
    ):
        job_id, job = _run(client, db, sales_csv.read_bytes(), [
            {"step": "enrich", "params": {}},
        ])
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        meta = json.loads(Path(out_ref.storage_path).read_text())["_metadata"]
        assert "job_id"          in meta
        assert "processing_time" in meta
        assert "row_count"       in meta
        assert "file_size_bytes" in meta

    def test_enrich_logs_enrich_completed_event(self, client, db, sales_csv, log_capture):
        """EnrichStep emits its own enrich_completed event with row_count and fields."""
        job_id, _ = _run(client, db, sales_csv.read_bytes(), [
            {"step": "enrich", "params": {"include": ["row_count", "job_id"]}},
        ])
        records = log_capture(job_id)
        enrich_rec = next(r for r in records if r.get("event") == "enrich_completed")
        assert enrich_rec["row_count"] == 8
        assert set(enrich_rec["fields"]) == {"row_count", "job_id"}

    def test_enrich_json_input_preserves_all_records(self, client, db, product_json, log_capture):
        job_id, job = _run(
            client, db, product_json.read_bytes(),
            [{"step": "enrich", "params": {"include": ["row_count"]}}],
            filename="products.json", ct="application/json",
        )
        assert job.status == "COMPLETED"
        out_ref = db.get(FileReference, job.output_file_id)
        output = json.loads(Path(out_ref.storage_path).read_text())
        assert output["_metadata"]["row_count"] == 5
        assert len(output["data"]) == 5


# ── Multi-step pipelines ───────────────────────────────────────────────────────

class TestMultiStepPipelines:
    def test_validate_filter_convert_compress_full_lifecycle(
        self, client, db, sales_csv, log_capture
    ):
        """4-step pipeline: all steps complete; output is .gz; logs show 4×started/completed."""
        pipeline = [
            {"step": "validate", "params": {"expected_type": "csv"}},
            {"step": "filter",   "params": {"field": "status", "valid_values": ["shipped", "pending"]}},
            {"step": "convert",  "params": {"to": "json"}},
            {"step": "compress", "params": {"action": "gzip"}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)

        assert job.status == "COMPLETED", job.error_message
        assert len(job.steps) == 4
        for step in job.steps:
            assert step.status == "COMPLETED", f"{step.step_type}: {step.error_message}"
            assert step.duration_seconds is not None

        out_ref = db.get(FileReference, job.output_file_id)
        assert out_ref.storage_path.endswith(".gz")
        assert Path(out_ref.storage_path).exists()

        events = _events(log_capture(job_id))
        assert events.count("step_started")   == 4
        assert events.count("step_completed") == 4
        assert "pipeline_completed" in events

    def test_filter_then_enrich_logs_two_step_completions(
        self, client, db, sales_csv, log_capture
    ):
        pipeline = [
            {"step": "filter", "params": {"field": "status",  "valid_values": ["shipped"]}},
            {"step": "enrich", "params": {"include": ["row_count", "job_id"]}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)
        assert job.status == "COMPLETED"

        events = _events(log_capture(job_id))
        assert events.count("step_completed") == 2

        out_ref = db.get(FileReference, job.output_file_id)
        output = json.loads(Path(out_ref.storage_path).read_text())
        assert output["_metadata"]["row_count"] == 5  # shipped rows

    def test_step_durations_stored_in_db_and_logged(self, client, db, sales_csv, log_capture):
        """duration_seconds in the DB and 'duration' in the log are both ≥ 0."""
        pipeline = [
            {"step": "validate", "params": {}},
            {"step": "filter",   "params": {"field": "region", "valid_values": ["East", "West"]}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)
        assert job.status == "COMPLETED"

        for step in job.steps:
            assert step.duration_seconds is not None
            assert step.duration_seconds >= 0

        records = log_capture(job_id)
        completed_recs = [r for r in records if r.get("event") == "step_completed"]
        assert len(completed_recs) == 2
        for rec in completed_recs:
            assert "duration" in rec
            assert rec["duration"] >= 0

    def test_csv_to_json_and_back_preserves_row_count(self, client, db, sales_csv, log_capture):
        """csv → json → csv round-trip; row count survives both conversions."""
        pipeline = [
            {"step": "convert", "params": {"to": "json"}},
            {"step": "convert", "params": {"to": "csv"}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)
        assert job.status == "COMPLETED"

        out_ref = db.get(FileReference, job.output_file_id)
        rows = list(csv.DictReader(Path(out_ref.storage_path).open()))
        assert len(rows) == 8

        events = _events(log_capture(job_id))
        assert events.count("step_completed") == 2

    def test_pipeline_completed_log_has_output_file_id(self, client, db, sales_csv, log_capture):
        job_id, job = _run(client, db, sales_csv.read_bytes(), [
            {"step": "validate", "params": {}},
        ])
        records = log_capture(job_id)
        completed_rec = next(r for r in records if r.get("event") == "pipeline_completed")
        assert completed_rec.get("output_file_id") == job.output_file_id


# ── Failure scenarios ──────────────────────────────────────────────────────────

class TestFailureScenarios:
    def test_unknown_step_logs_error_and_skips_remaining(
        self, client, db, sales_csv, log_capture
    ):
        """unknown step → FAILED; remaining SKIPPED; log shows 'unknown_step_type'."""
        pipeline = [
            {"step": "validate",     "params": {}},
            {"step": "no_such_step", "params": {}},
            {"step": "filter",       "params": {"field": "status", "valid_values": ["shipped"]}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)

        assert job.status == "FAILED"
        assert job.steps[0].status == "COMPLETED"
        assert job.steps[1].status == "FAILED"
        assert job.steps[2].status == "SKIPPED"

        assert "unknown_step_type" in _events(log_capture(job_id))

    def test_step_error_log_contains_error_message(self, client, db, sales_csv, log_capture):
        """step_failed log record includes a non-empty 'error' field."""
        pipeline = [
            {"step": "filter", "params": {"valid_values": ["shipped"]}},  # missing 'field'
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)

        assert job.status == "FAILED"

        records = log_capture(job_id)
        assert "step_failed" in _events(records)
        failed_rec = next(r for r in records if r.get("event") == "step_failed")
        assert failed_rec.get("error")  # non-empty string

    def test_log_error_matches_db_error_message(self, client, db, sales_csv, log_capture):
        """The error string in 'step_failed' log equals job.error_message in the DB."""
        pipeline = [
            {"step": "filter", "params": {"field": "status", "valid_values": []}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)

        assert job.status == "FAILED"
        assert job.error_message

        records = log_capture(job_id)
        failed_rec = next(r for r in records if r.get("event") == "step_failed")
        assert failed_rec["error"] == job.error_message

    def test_failed_job_has_no_output_file(self, client, db, sales_csv, log_capture):
        """A failed job must not set output_file_id."""
        pipeline = [{"step": "no_such_step", "params": {}}]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)

        assert job.status == "FAILED"
        assert job.output_file_id is None

    def test_first_step_failure_leaves_remaining_skipped_in_log(
        self, client, db, sales_csv, log_capture
    ):
        """When step 0 fails, steps 1+ should be SKIPPED with no step_started events."""
        pipeline = [
            {"step": "filter", "params": {"field": "status", "valid_values": []}},  # fails
            {"step": "enrich", "params": {}},
        ]
        job_id, job = _run(client, db, sales_csv.read_bytes(), pipeline)

        assert job.status == "FAILED"
        assert job.steps[1].status == "SKIPPED"

        # Only one step_started (for the failing step); enrich never started
        events = _events(log_capture(job_id))
        assert events.count("step_started") == 1
        assert "pipeline_completed" not in events
