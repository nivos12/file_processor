"""
Pipeline executor — the RQ task entry point.

Execution model:
1. Load Job + steps from DB.
2. For each step in order: PENDING → RUNNING → COMPLETED/FAILED.
3. On failure: mark remaining PENDING steps SKIPPED, set Job.status = FAILED.
4. On success of all steps: set Job.status = COMPLETED, point Job.output_file_id
   at the last step's output.

Crash recovery:
If the worker process is killed mid-job, the Job row stays in PROCESSING status.
The cleanup task (services/cleanup.py) will eventually mark orphaned PROCESSING
jobs as FAILED — see DECISIONS.md §3 for the full story.
"""
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import structlog

from app.config import settings
from app.database import SessionLocal
from app.logging_config import get_logger
from app.models import FileReference, Job, JobStep
from app.services.storage import allocate_path, create_file_reference
from app.steps.registry import STEP_REGISTRY

logger = get_logger(__name__)

_MAX_STEP_RETRIES = 0  # default: no retries per step (configurable via step params)


def execute_pipeline(job_id: str) -> None:
    """RQ task: run all pipeline steps for job_id sequentially."""
    db = SessionLocal()
    bound_log = logger.bind(job_id=job_id)
    try:
        job: Job | None = db.get(Job, job_id)
        if job is None:
            bound_log.error("pipeline_job_not_found")
            return

        if job.status == "CANCELLED":
            bound_log.info("pipeline_skipped_cancelled")
            return

        if job.status != "PENDING":
            # Already PROCESSING, COMPLETED, or FAILED.  Can happen when a job
            # is re-enqueued at startup (Redis flush recovery) but another worker
            # or the original worker already picked it up.
            bound_log.warning("pipeline_skipped_not_pending", status=job.status)
            return

        job.status = "PROCESSING"
        job.started_at = datetime.now(timezone.utc)
        db.commit()
        bound_log.info("pipeline_started", total_steps=len(job.steps))

        # The current working file starts as the job's input file
        current_file_id: str | None = job.input_file_id

        for step_row in job.steps:
            if job.status == "CANCELLED":
                _skip_remaining(db, job)
                break

            step_log = bound_log.bind(step_index=step_row.step_index, step_type=step_row.step_type)

            # Resolve step implementation
            StepClass = STEP_REGISTRY.get(step_row.step_type)
            if StepClass is None:
                _fail_step(db, job, step_row, f"Unknown step type: {step_row.step_type!r}")
                _skip_remaining(db, job, from_index=step_row.step_index + 1)
                job.status = "FAILED"
                job.error_message = step_row.error_message
                job.completed_at = datetime.now(timezone.utc)
                db.commit()
                step_log.error("unknown_step_type")
                return

            params = json.loads(step_row.parameters or "{}")
            retries = int(params.pop("_retries", _MAX_STEP_RETRIES))

            # Wire up the input file
            step_row.input_file_id = current_file_id
            step_row.status = "RUNNING"
            step_row.started_at = datetime.now(timezone.utc)
            job.current_step_index = step_row.step_index
            db.commit()
            step_log.info("step_started")

            # Determine output path for this step
            input_ref: FileReference | None = (
                db.get(FileReference, current_file_id) if current_file_id else None
            )
            output_path = allocate_path(
                job_id,
                f"step{step_row.step_index}_{input_ref.original_filename if input_ref else 'output'}",
            )

            attempt = 0
            last_error: Exception | None = None
            while attempt <= retries:
                try:
                    step_instance = StepClass()
                    t0 = time.perf_counter()
                    result = step_instance.execute(
                        input_path=Path(input_ref.storage_path) if input_ref else None,
                        output_path=output_path,
                        params=params,
                        job_id=job_id,
                        step_index=step_row.step_index,
                        db=db,
                    )
                    duration = time.perf_counter() - t0
                    last_error = None
                    break
                except Exception as exc:
                    last_error = exc
                    attempt += 1
                    if attempt <= retries:
                        backoff = 2 ** attempt
                        step_log.warning(
                            "step_retry",
                            attempt=attempt,
                            backoff=backoff,
                            error=str(exc),
                        )
                        time.sleep(backoff)

            if last_error is not None:
                _fail_step(db, job, step_row, str(last_error))
                _skip_remaining(db, job, from_index=step_row.step_index + 1)
                job.status = "FAILED"
                job.error_message = str(last_error)
                job.completed_at = datetime.now(timezone.utc)
                db.commit()
                step_log.error(
                    "step_failed",
                    error=str(last_error),
                    duration=round(time.perf_counter() - (step_row.started_at.timestamp() if step_row.started_at else time.time()), 3),
                )
                return

            # Step succeeded — resolve actual output path (step may write to a
            # different extension than what was allocated, e.g. convert csv→json)
            actual_output_path = Path(result["output_path"]) if result and result.get("output_path") else output_path

            out_size = actual_output_path.stat().st_size if actual_output_path.exists() else 0
            out_content_type = result.get("content_type", "application/octet-stream") if result else "application/octet-stream"
            out_filename = result.get("output_filename", actual_output_path.name) if result else actual_output_path.name

            out_ref = create_file_reference(
                db,
                storage_path=actual_output_path,
                original_filename=out_filename,
                size=out_size,
                content_type=out_content_type,
            )

            step_row.output_file_id = out_ref.id
            step_row.status = "COMPLETED"
            step_row.completed_at = datetime.now(timezone.utc)
            step_row.duration_seconds = round(duration, 4)
            db.commit()

            current_file_id = out_ref.id
            step_log.info("step_completed", duration=step_row.duration_seconds, output_size=out_size)

        # All steps completed
        job.status = "COMPLETED"
        job.output_file_id = current_file_id
        job.completed_at = datetime.now(timezone.utc)
        db.commit()
        bound_log.info("pipeline_completed", output_file_id=current_file_id)

    except Exception as exc:
        bound_log.error("pipeline_executor_crashed", error=str(exc), exc_info=True)
        try:
            job = db.get(Job, job_id)
            if job and job.status not in ("COMPLETED", "FAILED", "CANCELLED"):
                job.status = "FAILED"
                job.error_message = f"Internal executor error: {exc}"
                job.completed_at = datetime.now(timezone.utc)
                db.commit()
        except Exception:
            pass
    finally:
        db.close()


def _ensure_utc(dt: datetime) -> datetime:
    """SQLite returns naive datetimes; treat them as UTC when doing arithmetic."""
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _fail_step(db, job: Job, step: JobStep, error: str) -> None:
    step.status = "FAILED"
    step.error_message = error
    step.completed_at = datetime.now(timezone.utc)
    if step.started_at:
        started = _ensure_utc(step.started_at)
        step.duration_seconds = round(
            (step.completed_at - started).total_seconds(), 4
        )


def _skip_remaining(db, job: Job, from_index: int = 0) -> None:
    for s in job.steps:
        if s.step_index >= from_index and s.status == "PENDING":
            s.status = "SKIPPED"
