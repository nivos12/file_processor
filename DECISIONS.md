# Architecture Decisions

## 1. Large File Handling

**Decision**: Stream uploads to disk in 64 KB chunks; never buffer the full file in RAM.

**Implementation**: FastAPI's `UploadFile` wraps a `SpooledTemporaryFile`. The upload
endpoint reads `await file.read(CHUNK_SIZE)` in a loop and writes each chunk directly
to the destination file. The size limit (100 MB) is enforced by accumulating
`total_bytes` in the loop and returning HTTP 413 if exceeded — at that point the
partial file is unlinked.

**Why this matters**: A naive `contents = await file.read()` would load 100 MB per
concurrent upload into the API process's heap. With 10 concurrent uploads that's 1 GB
of RAM just for buffering, before any processing begins. Chunked streaming keeps memory
footprint flat regardless of file size or concurrency.

**Transform step**: CSV transformation uses `csv.DictReader` row-by-row — the file is
never materialized into a list. For JSON, we do `json.load()` the whole file, which is
a known limitation (JSON has no streaming parse format in the stdlib). For files in the
100 MB range this is acceptable; for larger files we'd swap in `ijson` for streaming
JSON parsing.

---

## 2. Step Failure Strategy

**Default behavior**: When a step raises an exception, the executor immediately marks
that step FAILED, marks all remaining PENDING steps SKIPPED, sets Job.status = FAILED,
and returns. No subsequent steps execute.

**Rationale**: This is the safest default. Steps are data-transforming and often
stateful — running a downstream step on a partially-corrupted file could produce
silently wrong output or cascade the failure in harder-to-debug ways.

**Per-step retry**: Each step can specify `"_retries": N` in its params dict. The
executor retries that step up to N times with exponential backoff (2^attempt seconds)
before giving up. This is designed for transient failures (e.g. a flaky network call
inside a step) and is distinct from the notify step's own built-in retry logic.

**Trade-off**: There is currently no "continue on failure" mode. If you wanted to
allow a pipeline to skip a failed step and continue, you'd add a `"on_failure": "skip"`
param and handle it in the executor. This was not implemented because it creates
surprising behavior (you can't compress a file that wasn't converted yet).

---

## 3. Cleanup Strategy

**File retention**: Every `FileReference` row gets an `expires_at` timestamp set at
creation time (default: 24 hours from upload). The cleanup sweep
(`app/services/cleanup.py:run_cleanup`) queries for rows past `expires_at`, deletes
the file from disk, and sets `storage_path = "DELETED"` on the row (rather than
deleting the row, so the job's audit trail stays intact).

**Crash recovery**: If a worker process is killed mid-job (SIGKILL, OOM, host restart),
the Job row stays in `PROCESSING` status indefinitely. The cleanup sweep detects these
"orphan" jobs: any Job with `status = PROCESSING` and `started_at` older than
`stale_threshold_seconds` (default 1 hour) is marked FAILED with a message explaining
what happened. The client must re-submit to retry.

**Why not resume?**: Mid-step resumption would require checkpointing within each step
(e.g. recording how many CSV rows were written). That's significant added complexity
with marginal benefit given the 100 MB file size cap. Marking FAILED is honest —
the output cannot be trusted — and re-submission is cheap because the input file
still exists until its own `expires_at`.

**Scheduling**: The cleanup task is an `asyncio.create_task` background coroutine
started in the FastAPI `lifespan` handler. It sleeps for `CLEANUP_INTERVAL_SECONDS`
(default 1 hour), then calls `run_cleanup()` in a thread via `asyncio.to_thread()`,
and repeats until the application shuts down. This keeps cleanup decoupled from the
RQ worker queue — cleanup runs on its own clock regardless of worker load.

**On-demand trigger**: `POST /admin/cleanup` runs the sweep immediately without
waiting for the next scheduled interval. Useful for testing or operational recovery.

**Directory removal**: When all `FileReference` rows for a job are expired, the job
directory (`{STORAGE_ROOT}/{job_id}/`) is removed via `parent.rmdir()` after the last
file is unlinked. The directory is only deleted when empty, so jobs with multiple
step outputs clean up progressively.

---

## 4. Progress Tracking

**How**: `Job.current_step_index` is updated before each step starts (so it reflects
"currently running" step, not "last completed"). `JobStep.status` transitions:
`PENDING → RUNNING → COMPLETED/FAILED/SKIPPED`. Duration is stored on each step row.

**API**: `GET /jobs/{id}/progress` returns a lightweight payload with
`progress_percent` computed as `completed_or_terminal_steps / total_steps * 100`.
`GET /jobs/{id}/steps` returns the full per-step breakdown including duration and
error messages.

**Why not websockets/SSE?**: The assignment specifies REST polling. For a production
system you'd add an SSE endpoint (`GET /jobs/{id}/events`) that streams step
transitions. The DB model already supports this — each `JobStep` status change is a
commit that an SSE handler could watch via DB polling or a Redis pub/sub channel.

**Trade-off**: Polling on `GET /jobs/{id}/progress` at e.g. 1s intervals is fine for
a single client. Under high concurrency, consider adding a `Last-Modified` / `ETag`
header so clients can use conditional GET to avoid reading the DB on every poll.

---

## 5. One Thing I'd Do Differently

**Swap SQLite for PostgreSQL**.

SQLite works well for a single-server deployment, but its serialized write model becomes
a bottleneck if the worker and API process write concurrently (e.g. the worker updating
`JobStep.status` while the API handles a status poll that triggers an implicit flush).
`check_same_thread=False` suppresses the safety check but doesn't eliminate the lock
contention.

PostgreSQL would give us row-level locking, proper `RETURNING` support (avoiding the
`db.refresh()` round-trips we do after commits), and a clear path to horizontal scaling
if we needed multiple API or worker replicas. The SQLAlchemy ORM layer means the swap
is mostly a config change (`DATABASE_URL`) plus a Alembic migration setup — no
application logic needs to change.
