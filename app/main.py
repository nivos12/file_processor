"""
FastAPI application entry point.

Uses the lifespan context manager (FastAPI ≥0.93) instead of the deprecated
@app.on_event("startup") decorator.

On startup:
  1. init_db()        — create tables if they don't exist.
  2. _recover_pending_jobs() — re-enqueue any PENDING jobs whose RQ entries may
     have been lost if Redis was restarted while the DB still shows them PENDING.
  3. _cleanup_loop()  — background asyncio task that calls run_cleanup() on the
     configured interval.  Cancelled cleanly on shutdown.
"""
import asyncio
import contextlib
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.database import init_db
from app.logging_config import configure_logging, get_logger

configure_logging(os.getenv("LOG_LEVEL", "INFO"))
logger = get_logger(__name__)


async def _cleanup_loop() -> None:
    """Run cleanup on a fixed interval in a background thread."""
    from app.services.cleanup import run_cleanup
    while True:
        await asyncio.sleep(settings.cleanup_interval_seconds)
        try:
            await asyncio.to_thread(run_cleanup)
        except Exception as exc:
            logger.error("cleanup_loop_error", error=str(exc))


def _recover_pending_jobs() -> None:
    """
    Re-enqueue PENDING jobs found in the DB at startup.

    Handles the case where Redis was flushed/restarted while jobs were queued:
    the DB still shows them PENDING but the RQ queue is empty.  Re-enqueuing is
    safe because execute_pipeline() checks the job status and skips anything
    that isn't PENDING (so a double-enqueue just results in one no-op dequeue).
    """
    from app.database import SessionLocal
    from app.models import Job
    from app.workers.queue import enqueue_pipeline

    db = SessionLocal()
    try:
        pending = db.query(Job).filter(Job.status == "PENDING").all()
        for job in pending:
            try:
                enqueue_pipeline(job.id)
                logger.info("job_requeued_on_startup", job_id=job.id)
            except Exception as exc:
                logger.warning("job_requeue_failed", job_id=job.id, error=str(exc))
    except Exception as exc:
        logger.warning("startup_recovery_failed", error=str(exc))
    finally:
        db.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await asyncio.to_thread(_recover_pending_jobs)
    cleanup_task = asyncio.create_task(_cleanup_loop())
    logger.info("app_started", env=os.getenv("ENV", "development"))
    yield
    cleanup_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await cleanup_task


app = FastAPI(
    title="File Processing Pipeline API",
    description="Upload files and run configurable multi-step processing pipelines.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok"}


@app.post("/admin/cleanup", tags=["ops"])
async def trigger_cleanup() -> dict:
    """Immediately run the file expiry + orphan-job cleanup sweep."""
    from app.services.cleanup import run_cleanup
    result = await asyncio.to_thread(run_cleanup)
    return result


from app.api.jobs import router as jobs_router  # noqa: E402
app.include_router(jobs_router)
