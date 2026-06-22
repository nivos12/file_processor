# CLAUDE.md — File Processing Pipeline

This is a production-quality backend engineering assessment project. It implements a configurable, multi-step file processing pipeline as a REST API. The priority is **clarity and correctness over cleverness** — the code must be readable and defensible in a technical interview.

---

## What this project does

Clients upload a file and a JSON pipeline definition. The API streams the file to disk, persists the job, and enqueues it to a background worker. The worker runs each pipeline step in sequence, passing the output of one step as the input to the next. Each step updates the job status in the database. Clients poll for status or download the final output.

---

## Architecture

```
POST /jobs  →  FastAPI (api/jobs.py)
                 │
                 ├── streams file to disk (64 KB chunks)
                 ├── persists Job + JobStep rows + FileReference
                 └── enqueue_pipeline(job_id) → Redis queue
                                                      │
                                              RQ Worker (workers/pipeline.py)
                                                      │
                                              execute_pipeline(job_id)
                                                      │
                                         step 0 → step 1 → step 2 → ...
                                         (each resolved from STEP_REGISTRY)
```

**Three Docker services** (`docker-compose.yml`):
- `redis` — message broker and RQ result backend
- `api` — FastAPI (uvicorn, port 8000)
- `worker` — RQ worker consuming the `pipeline` queue

Note: there is no separate scheduler service. Cleanup is handled by an asyncio background task inside the API process (see startup cleanup below).

**Storage**: local filesystem. Files stored at `{STORAGE_ROOT}/{job_id}/{uuid4}{ext}`. Original filenames are **never used as filesystem paths** — only stored as metadata in `FileReference`.

**Database**: SQLite via SQLAlchemy 2.0 ORM. Three tables: `jobs`, `job_steps`, `file_references`.

---

## Key design decisions

### Why RQ over Celery
RQ is Redis-only (matches our only broker), has zero broker config overhead, and its worker is a single CLI command. Celery's multi-broker support is irrelevant here and its config is significantly more complex. The RQ `job_timeout` and `result_ttl` options cover all our lifecycle needs.

### Strategy pattern for steps
`app/steps/base.py` defines an abstract `Step` base class with a single `execute()` method. `app/steps/registry.py` exports `STEP_REGISTRY: dict[str, type[Step]]`. The pipeline executor looks up step types by name at runtime. **Adding a new step = one new file + one new line in `registry.py`.**

### Streaming upload
`POST /jobs` reads the multipart body in 64 KB chunks directly to disk. The size limit (100 MB) is enforced mid-stream by accumulating byte count; if exceeded, the partial file is unlinked before returning HTTP 413. The full file is never held in RAM.

### Shared format helpers (`steps/formats.py`)

`detect_format`, `content_type_for`, `read_json`, `write_json`, and `write_csv` live in `app/steps/formats.py` and are imported by every step that reads or writes CSV/JSON. This avoids re-implementing the same `csv.DictReader` / `json.load` / `csv.DictWriter` patterns in each step. CSV *streaming* (row-by-row DictReader inside a `with` block) stays inline in the steps that need it because the open file handle must outlive the loop.

### output_path contract between steps and executor
Steps write to the `output_path` they receive. However, some steps change the file extension — `ConvertStep` (csv→json), `EnrichStep` (always .json), and `CompressStep` (adds .gz or .zip). These steps write to a different path than allocated and signal it by returning `"output_path": str(actual_path)` in their result dict. The pipeline executor reads this:
```python
actual_output_path = Path(result["output_path"]) if result and result.get("output_path") else output_path
```
Every step's `FileReference` is created from `actual_output_path`. The originally-allocated path is never written to (it's just a reserved filename that goes unused). **Do not move or rename the file back to the allocated path** — this was a prior bug in CompressStep that gave gzipped files a .csv extension.

### SQLite + timezone quirk (`_ensure_utc`)
SQLite does not store timezone info. Even with `DateTime(timezone=True)`, after a `db.commit()` SQLAlchemy expires the object and reloads from SQLite — the returned datetime is **naive** (no `tzinfo`). Subtracting a naive datetime from an aware one raises `TypeError`. The fix is `_ensure_utc()` in `workers/pipeline.py`:
```python
def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt
```
Always call this before doing arithmetic on timestamps loaded from DB.

### Notify step webhook payload
`NotifyStep` sends `"status": "COMPLETED"` in the webhook payload even though the DB row is still `PROCESSING` at that point. By the time `notify` executes, all prior steps have succeeded — the effective outcome is complete. Hardcoding `"PROCESSING"` was a prior bug.

### SSRF prevention
`app/services/security.py::validate_webhook_url()` DNS-resolves the webhook hostname and checks every resolved IP against RFC1918 ranges, loopback, link-local, and IPv6 ULA. Called at the start of `NotifyStep.execute()` before any HTTP request. `follow_redirects=False` prevents bypass via open redirect.

### Startup cleanup + queue recovery

Two things happen in `lifespan` beyond `init_db()`:

1. **`_recover_pending_jobs()`** — queries the DB for PENDING jobs and re-enqueues them to Redis. Handles the case where Redis was flushed while the DB still shows jobs as PENDING. Safe to call on every restart: `execute_pipeline()` checks `job.status != "PENDING"` at the top and exits immediately if the job was already picked up by another worker (handles double-enqueue without double-processing).

2. **`_cleanup_loop()`** — an `asyncio.create_task` background coroutine that sleeps for `cleanup_interval_seconds` (default 3600), then calls `run_cleanup()` in a thread via `asyncio.to_thread()`. Cancelled cleanly when the lifespan exits. No separate scheduler process is needed, and the cleanup runs on its own clock regardless of worker queue load.

**On-demand cleanup**: `POST /admin/cleanup` calls `run_cleanup()` immediately. Useful for testing or operational recovery without waiting for the next scheduled interval.

**Directory cleanup**: `_expire_files()` removes the job's storage directory (`{STORAGE_ROOT}/{job_id}/`) via `parent.rmdir()` after the last file in it is unlinked. The directory is only removed when empty — jobs with multiple step files clean up progressively.

### Crash recovery
If the worker process is killed mid-job, the `Job.status` row stays in `PROCESSING`. The cleanup task (`services/cleanup.py`) marks jobs in PROCESSING whose `started_at` is older than a configurable threshold as FAILED. Intentional design decision: no resume. See `DECISIONS.md §3`.

---

## File map

```
app/
  config.py           — all settings via pydantic-settings (reads .env)
  database.py         — SQLAlchemy engine, SessionLocal, get_db(), init_db()
  models.py           — Job, JobStep, FileReference ORM models
  schemas.py          — Pydantic v2 request/response schemas
  logging_config.py   — structlog JSON logging setup
  main.py             — FastAPI app, lifespan handler (init_db + cleanup loop + startup recovery); POST /admin/cleanup
  api/
    jobs.py           — all REST endpoints (upload, status, progress, steps, result, cancel, admin cleanup)
  workers/
    queue.py          — RQ setup, get_redis(), enqueue_pipeline()
    pipeline.py       — RQ task: execute_pipeline(job_id)
  steps/
    base.py           — Step ABC with execute() signature
    registry.py       — STEP_REGISTRY dict
    formats.py        — shared CSV/JSON read/write helpers (detect_format, read_json, write_json, write_csv)
    validate.py       — validates file (CSV/JSON integrity + metadata)
    transform.py      — filter/select/string-transform (CSV streaming)
    filter.py         — allowlist filter: keep rows where field ∈ valid_values
    convert.py        — format conversion (csv↔json) via converter registry
    enrich.py         — wraps records in {_metadata, data} JSON envelope
    compress.py       — gzip/gunzip/zip/unzip via action registry
    notify.py         — webhook POST with retry + idempotency key + SSRF guard
  services/
    storage.py        — allocate_path(), create_file_reference(), is_expired()
    security.py       — sanitize_filename(), validate_webhook_url()
    cleanup.py        — expire files + reap orphaned PROCESSING jobs
tests/
  conftest.py         — fixtures: per-test SQLite DB, TestClient, monkeypatches
  test_upload.py      — upload success, oversized, bad content-type, bad extension
  test_pipeline.py    — step unit tests: transform, convert, filter, enrich
  test_failure.py     — step failure → remaining steps SKIPPED, unknown step type
  test_status.py      — status/progress/steps endpoints, cancel logic
  test_result.py      — result download, 202 not-ready, 410 expired, 422 failed
  test_notify.py      — SSRF blocks, retry logic, idempotency key
  test_e2e.py         — end-to-end pipeline runs with realistic fixture files + structured-log assertions
```

---

## Data models

**`FileReference`** — a file on disk  
`id, storage_path, original_filename, size, content_type, created_at, expires_at`

**`Job`** — one pipeline execution  
`id, input_file_id → FileReference, output_file_id → FileReference, pipeline_definition (JSON text), status, current_step_index, error_message, api_key, created_at, started_at, completed_at`  
Status values: `PENDING → PROCESSING → COMPLETED | FAILED | CANCELLED`

**`JobStep`** — one step within a job  
`id, job_id, step_index, step_type, parameters (JSON text), status, input_file_id, output_file_id, error_message, started_at, completed_at, duration_seconds`  
Status values: `PENDING → RUNNING → COMPLETED | FAILED | SKIPPED`

---

## Job lifecycle

1. `POST /jobs` creates Job (PENDING) + N JobStep rows (all PENDING)
2. RQ worker picks up `execute_pipeline(job_id)`
3. Job → PROCESSING; for each step: PENDING → RUNNING → COMPLETED
4. On step failure: step → FAILED, remaining → SKIPPED, job → FAILED
5. On all steps COMPLETED: job → COMPLETED, `output_file_id` set to last step's output
6. Cancel via `POST /jobs/{id}/cancel`: job → CANCELLED, PENDING steps → SKIPPED

---

## Test isolation strategy

The critical challenge: `execute_pipeline()` opens its own `SessionLocal()` inside the RQ task. This is a different SQLAlchemy session than the test's session. Tests must ensure both sessions see the same data.

**Solution**: each test gets a fresh SQLite file (via `tmp_path`). Two monkeypatches in `conftest.py`:
```python
monkeypatch.setattr(pipeline_module, "SessionLocal", TestSession)
monkeypatch.setattr(db_module, "SessionLocal", TestSession)
```
`enqueue_pipeline` is patched to a no-op; tests call `execute_pipeline(job_id)` directly.

Data is **committed**, not rolled back — the DB file is discarded after the test anyway.

**Do not use rollback-based isolation here.** SQLite does not support seeing uncommitted data across connections.

---

## Running tests

```bash
# No Redis or Docker needed
source .venv/bin/activate
pytest -v
```

66 tests, ~0.7 seconds.

### Test suites at a glance

| File | What it covers |
|---|---|
| `test_upload.py` | Upload success, oversized files, bad content-type, bad extension |
| `test_pipeline.py` | Unit-level step tests: transform, convert, filter, enrich |
| `test_failure.py` | Step failure propagation, unknown step type |
| `test_status.py` | Status/progress/steps endpoints, cancel logic |
| `test_result.py` | Result download, 202 not-ready, 410 expired, 422 failed |
| `test_notify.py` | SSRF blocks, retry logic, idempotency key |
| `test_e2e.py` | End-to-end pipelines with realistic fixture files, verifies both output content and structured-log events |

### Log capture in `test_e2e.py`

`test_e2e.py` verifies that the correct structlog JSON events are emitted for each job. The `log_capture` fixture (defined in the test file) attaches a `StringIO`-backed `StreamHandler` — sharing the existing `ProcessorFormatter` — to the root logger for the duration of each test. This is more reliable than `capfd` (fd-level redirect) or `capsys` (replaces `sys.stdout`), both of which can miss writes that go through a `StreamHandler` that captured the original `sys.stdout` object at import time.

Usage in a test:
```python
def test_example(client, db, sales_csv, log_capture):
    job_id, job = _run(client, db, sales_csv.read_bytes(), [...])
    records = log_capture(job_id)           # list[dict], filtered by job_id
    events = _events(records)               # list[str] of "event" field values
    assert "pipeline_completed" in events
```

---

## Running locally (with Docker)

```bash
docker-compose up --build
# API at http://localhost:8000
# Docs at http://localhost:8000/docs
```

Or without Docker:
```bash
source .venv/bin/activate
redis-server &
uvicorn app.main:app --reload &
rq worker pipeline
```

---

## Available pipeline steps

| Step | Key params | Notes |
|---|---|---|
| `validate` | `expected_type` | Checks non-empty, optional type match, extracts CSV headers / JSON keys |
| `transform` | `filter_field`, `filter_value`, `filter_op`, `select_fields`, `string_transforms`, `output_format` | Streaming CSV; single-value filter with eq/contains/gt/lt |
| `filter` | `field`, `valid_values` (list) | Exact-match allowlist; keeps rows where field ∈ valid_values |
| `convert` | `to` | Format conversion: csv↔json via converter registry |
| `enrich` | `include` (list, default all) | Wraps records in `{_metadata, data}` JSON; always outputs .json |
| `compress` | `action` (gzip/gunzip/zip/unzip) | Action registry; signals actual output path via `result["output_path"]` |
| `notify` | `url`, `max_retries`, `timeout_seconds` | Webhook with SSRF guard, idempotency key, exponential backoff |

## Adding a new pipeline step

1. Create `app/steps/my_step.py` with a class inheriting from `Step`
2. Implement `execute(*, input_path, output_path, params, job_id, step_index, db) -> dict | None`
3. The method must write to `output_path` (or a path it returns as `result["output_path"]`)
4. Use helpers from `app/steps/formats.py` for CSV/JSON I/O
5. Return at minimum `{"content_type": str, "output_filename": str}`
6. Add one line to `app/steps/registry.py`:
   ```python
   from app.steps.my_step import MyStep
   return { ..., "my_step": MyStep }
   ```

---

## Environment variables

All config in `app/config.py` via pydantic-settings. Copy `.env.example` to `.env`.

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `sqlite:///./data/jobs.db` | SQLAlchemy URL |
| `STORAGE_ROOT` | `./data/files` | File storage directory |
| `REDIS_URL` | `redis://localhost:6379/0` | RQ broker |
| `MAX_UPLOAD_BYTES` | `104857600` | 100 MB upload limit |
| `FILE_RETENTION_SECONDS` | `86400` | 24 h file expiry |
| `WEBHOOK_MAX_RETRIES` | `3` | NotifyStep retry count |
| `LOG_LEVEL` | `INFO` | structlog level |

---

## Known constraints and non-issues

- **SQLAlchemy 2.0.51 required** — 2.0.36 is incompatible with Python 3.14's union type syntax (`str | None` in `Mapped`). Pin to `>=2.0.41`.
- **IDE "Cannot find module" errors** — false positives if the IDE uses system Python instead of `.venv`. Tests and runtime both work correctly.
- **`TransformStep` JSON limitation** — uses `json.load()` (full in-memory load) rather than streaming. For CSV, it streams row-by-row. A streaming JSON library (e.g. `ijson`) could improve this for very large files.
- **SSRF guard is best-effort** — DNS resolution at validation time is vulnerable to DNS rebinding. Full protection requires an egress proxy at the network layer.
- **SQLite concurrency** — `check_same_thread=False` is set but SQLite itself serializes writes. Acceptable for a single-server deployment; swap for PostgreSQL if horizontal scaling is needed.
