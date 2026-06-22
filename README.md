# File Processing Pipeline

A FastAPI-based backend that accepts file uploads and runs configurable multi-step
processing pipelines asynchronously via Redis + RQ workers.

## Stack

| Component | Technology |
|-----------|-----------|
| Web framework | FastAPI |
| Database | SQLite via SQLAlchemy |
| Job queue | Redis + RQ |
| File storage | Local filesystem (organized by job UUID) |
| Logging | structlog (JSON) |
| Container | Docker + docker-compose |
| Tests | pytest |

## Quickstart

### Prerequisites
- Docker + docker-compose, **or** Python 3.11+ and a local Redis instance.

### With Docker (recommended)

```bash
cp .env.example .env
docker-compose up --build
```

The API is available at `http://localhost:8000`.

### Local development (without Docker)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env
# Edit .env — ensure REDIS_URL points at a running Redis

# Start the API (includes background cleanup loop)
uvicorn app.main:app --reload

# In a separate terminal: start the worker
rq worker pipeline
```

## Running Tests

Tests run without Redis or a real worker — the queue is mocked.

```bash
pip install -r requirements.txt
pytest -v
```

Expected output: all tests pass in under 5 seconds.

## API Reference

### POST /jobs — Upload a file and start a pipeline

```bash
curl -X POST http://localhost:8000/jobs \
  -F "file=@mydata.csv;type=text/csv" \
  -F 'pipeline=[{"step":"validate","params":{}},{"step":"transform","params":{"filter_field":"city","filter_value":"NYC"}},{"step":"convert","params":{"to":"json"}},{"step":"compress","params":{"action":"gzip"}}]'
```

Response (202 Accepted):
```json
{
  "job_id": "a1b2c3d4-...",
  "status": "PENDING",
  "message": "Job accepted and queued for processing"
}
```

### GET /jobs/{job_id} — Full job status

```bash
curl http://localhost:8000/jobs/a1b2c3d4-...
```

### GET /jobs/{job_id}/progress — Lightweight progress check

```bash
curl http://localhost:8000/jobs/a1b2c3d4-.../progress
```

```json
{
  "id": "a1b2c3d4-...",
  "status": "PROCESSING",
  "current_step_index": 2,
  "total_steps": 4,
  "progress_percent": 50.0,
  "error_message": null
}
```

### GET /jobs/{job_id}/steps — Per-step detail

```bash
curl http://localhost:8000/jobs/a1b2c3d4-.../steps
```

### GET /jobs/{job_id}/result — Download output file

```bash
curl -OJ http://localhost:8000/jobs/a1b2c3d4-.../result
```

- **202** — job not yet complete
- **200** — returns the output file as a download
- **410** — file has expired and been deleted
- **422** — job failed

### POST /jobs/{job_id}/cancel — Cancel a pending/processing job

```bash
curl -X POST http://localhost:8000/jobs/a1b2c3d4-.../cancel
```

### GET /health — Health check

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

### POST /admin/cleanup — Trigger cleanup immediately

Runs the file expiry + orphan-job sweep without waiting for the next scheduled interval.

```bash
curl -X POST http://localhost:8000/admin/cleanup
# {"expired_files": 3, "orphaned_jobs": 0}
```

## Pipeline Steps

All steps are specified as `{"step": "<name>", "params": {...}}` in the pipeline array.

### validate
Checks file is non-empty, extension matches expected type, and the content is
parseable. Extracts metadata (size, row count, MIME type). Passes the file through unchanged.

```json
{"step": "validate", "params": {"expected_type": "csv"}}
```

### transform
Filters rows, selects columns, and applies string transforms. Streams CSV row-by-row.

```json
{
  "step": "transform",
  "params": {
    "filter_field": "city",
    "filter_value": "NYC",
    "filter_op": "eq",
    "select_fields": ["name", "city"],
    "string_transforms": {"name": "upper"}
  }
}
```

`filter_op` options: `eq` (default), `contains`, `gt`, `lt`.

### convert
Converts between formats using a pluggable converter registry.
Currently supports: `csv→json`, `json→csv`.

```json
{"step": "convert", "params": {"to": "json"}}
```

### compress
Compress or decompress a file.

```json
{"step": "compress", "params": {"action": "gzip"}}
```

`action` options: `gzip` (default), `gunzip`, `zip`, `unzip`.

### notify
POST a webhook with job status. Retries with exponential backoff.
Sends a stable `X-Idempotency-Key` header on every attempt.

```json
{
  "step": "notify",
  "params": {
    "url": "https://your-server.com/webhook",
    "max_retries": 3,
    "timeout_seconds": 10
  }
}
```

The webhook payload includes `"status": "COMPLETED"` — by the time `notify` executes,
all prior steps have succeeded. Webhook URLs that resolve to private/internal IP ranges
are rejected (SSRF guard).

## Security Notes

- **Path traversal prevention**: uploaded files are stored at UUID-derived paths.
  Original filenames are kept only as metadata in the database.
- **SSRF guard**: webhook URLs in the `notify` step are DNS-resolved and checked
  against RFC1918 + loopback ranges before any HTTP call.
- **Secrets**: all configuration is via environment variables. `.env` is gitignored.
  See `.env.example`.

## File Retention

Files are automatically deleted after `FILE_RETENTION_SECONDS` (default 24 hours).
When all files for a job are deleted, the job's storage directory is also removed.
The cleanup sweep also reaps PROCESSING jobs whose worker died more than 1 hour ago.

The cleanup runs on a background loop inside the API process every
`CLEANUP_INTERVAL_SECONDS` (default 1 hour). Use `POST /admin/cleanup` to trigger
it on demand.

See `DECISIONS.md §3` for crash recovery and directory cleanup details.
