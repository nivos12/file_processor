"""
Cleanup service — deletes expired files and marks orphaned jobs.

Two responsibilities:
1. Expire sweep: find FileReference rows past their expires_at, delete the
   file from disk, mark the row (storage_path set to "DELETED").
2. Orphan sweep: find Job rows stuck in PROCESSING for > stale_threshold_seconds.
   These are jobs whose worker died mid-execution. Mark them FAILED with a
   crash-recovery message.

Run this as a scheduled RQ job (enqueued by the periodic task below) or as a
standalone script: `python -m app.services.cleanup`.

Crash recovery contract (documented in DECISIONS.md §3):
- A PROCESSING job with no heartbeat after stale_threshold is treated as crashed.
- We cannot resume from the middle of a step; the job is marked FAILED.
- The client must re-submit if they want to retry.
- Future improvement: store per-step checkpoints to enable partial resume.
"""
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal
from app.logging_config import get_logger
from app.models import FileReference, Job, JobStep

logger = get_logger(__name__)


def run_cleanup(
    db: Session | None = None,
    stale_threshold_seconds: int = 3600,
) -> dict:
    """
    Run both cleanup sweeps. Returns a summary dict for observability.
    Can be called directly (from a script or test) or via the RQ periodic task.
    """
    close_db = db is None
    if db is None:
        db = SessionLocal()

    try:
        expired = _expire_files(db)
        orphans = _reap_orphan_jobs(db, stale_threshold_seconds)
        db.commit()
        result = {"expired_files": expired, "orphaned_jobs": orphans}
        logger.info("cleanup_completed", **result)
        return result
    except Exception as exc:
        db.rollback()
        logger.error("cleanup_failed", error=str(exc))
        raise
    finally:
        if close_db:
            db.close()


def _expire_files(db: Session) -> int:
    now = datetime.now(timezone.utc)
    refs = db.query(FileReference).filter(
        FileReference.expires_at != None,  # noqa: E711
        FileReference.storage_path != "DELETED",
    ).all()

    deleted = 0
    for ref in refs:
        exp = ref.expires_at
        if exp and exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp and now > exp:
            path = Path(ref.storage_path)
            if path.exists():
                try:
                    path.unlink()
                    # Remove the job directory once it's empty
                    parent = path.parent
                    if parent.exists() and not any(parent.iterdir()):
                        parent.rmdir()
                except OSError as exc:
                    logger.warning("cleanup_delete_failed", path=str(path), error=str(exc))
                    continue
            ref.storage_path = "DELETED"
            deleted += 1
            logger.info("file_expired", file_id=ref.id, original=ref.original_filename)

    return deleted


def _reap_orphan_jobs(db: Session, threshold_seconds: int) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=threshold_seconds)
    orphans = db.query(Job).filter(
        Job.status == "PROCESSING",
        Job.started_at != None,  # noqa: E711
        Job.started_at < cutoff,
    ).all()

    for job in orphans:
        job.status = "FAILED"
        job.error_message = (
            "Job marked FAILED by cleanup: worker likely crashed mid-execution. "
            "Re-submit to retry."
        )
        job.completed_at = datetime.now(timezone.utc)
        # Mark any RUNNING/PENDING steps as FAILED/SKIPPED
        for step in job.steps:
            if step.status == "RUNNING":
                step.status = "FAILED"
                step.error_message = "Worker crashed"
                step.completed_at = datetime.now(timezone.utc)
            elif step.status == "PENDING":
                step.status = "SKIPPED"
        logger.warning("orphan_job_reaped", job_id=job.id, started_at=str(job.started_at))

    return len(orphans)


# ── Periodic RQ task ───────────────────────────────────────────────────────

def schedule_cleanup() -> None:
    """Enqueue a cleanup run via RQ. Called at startup to register a recurring job."""
    from rq_scheduler import Scheduler
    from app.workers.queue import get_redis

    scheduler = Scheduler(connection=get_redis())
    scheduler.schedule(
        scheduled_time=datetime.now(timezone.utc),
        func=run_cleanup,
        interval=settings.cleanup_interval_seconds,
        repeat=None,  # run indefinitely
        id="periodic_cleanup",
    )
    logger.info("cleanup_scheduled", interval_seconds=settings.cleanup_interval_seconds)


if __name__ == "__main__":
    from app.logging_config import configure_logging
    configure_logging()
    result = run_cleanup()
    print(result)
