"""
Job API endpoints.

Upload flow:
  1. Validate content-type and filename extension before accepting the body.
  2. Stream body to disk in 64 KB chunks — never buffer whole file in RAM.
  3. Persist Job + FileReference to DB.
  4. Enqueue the pipeline job via RQ.
  5. Return job_id immediately (202 Accepted).

The file is stored at a UUID-derived path; the original filename is kept only
as metadata in FileReference, never used as a filesystem path.
"""
import json
import uuid
from pathlib import Path

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import get_db
from app.logging_config import get_logger
from app.models import FileReference, Job, JobStep
from app.workers.queue import enqueue_pipeline
from app.schemas import (
    CancelResponse,
    JobCreateResponse,
    JobProgressOut,
    JobStatusOut,
    JobStepOut,
    PipelineStep,
)
from app.services.security import sanitize_filename
from app.services.storage import (
    allocate_path,
    create_file_reference,
    is_expired,
)

logger = get_logger(__name__)
router = APIRouter(prefix="/jobs", tags=["jobs"])

_CHUNK = settings.chunk_size
_MAX_BYTES = settings.max_upload_bytes


# ── Upload ─────────────────────────────────────────────────────────────────

@router.post("", response_model=JobCreateResponse, status_code=202)
async def create_job(
    file: UploadFile = File(...),
    pipeline: str = Form(..., description="JSON array of pipeline steps"),
    db: Session = Depends(get_db),
) -> JobCreateResponse:
    # --- validate content-type before touching the body ---
    content_type = (file.content_type or "").split(";")[0].strip()
    if content_type not in settings.allowed_content_types:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported content type: {content_type!r}. "
                   f"Allowed: {settings.allowed_content_types}",
        )

    safe_name = sanitize_filename(file.filename or "upload")
    ext = Path(safe_name).suffix.lower()
    if ext not in settings.allowed_extensions:
        raise HTTPException(
            status_code=415,
            detail=f"Unsupported file extension: {ext!r}. "
                   f"Allowed: {settings.allowed_extensions}",
        )

    # --- parse and validate pipeline definition ---
    try:
        raw_steps: list[dict] = json.loads(pipeline)
        pipeline_steps = [PipelineStep(**s) for s in raw_steps]
    except (json.JSONDecodeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail=f"Invalid pipeline JSON: {exc}") from exc

    if not pipeline_steps:
        raise HTTPException(status_code=422, detail="Pipeline must have at least one step")

    # --- stream body to disk, enforcing size limit ---
    job_id = str(uuid.uuid4())
    storage_path = allocate_path(job_id, safe_name)

    total_bytes = 0
    try:
        with storage_path.open("wb") as fout:
            while True:
                chunk = await file.read(_CHUNK)
                if not chunk:
                    break
                total_bytes += len(chunk)
                if total_bytes > _MAX_BYTES:
                    storage_path.unlink(missing_ok=True)
                    raise HTTPException(
                        status_code=413,
                        detail=f"File exceeds maximum allowed size of {_MAX_BYTES} bytes",
                    )
                fout.write(chunk)
    except HTTPException:
        raise
    except Exception as exc:
        storage_path.unlink(missing_ok=True)
        logger.error("upload_stream_failed", job_id=job_id, error=str(exc))
        raise HTTPException(status_code=500, detail="Failed to store uploaded file") from exc

    # --- persist FileReference, Job, JobSteps ---
    file_ref = create_file_reference(
        db,
        storage_path=storage_path,
        original_filename=safe_name,
        size=total_bytes,
        content_type=content_type,
    )

    job = Job(
        id=job_id,
        input_file_id=file_ref.id,
        pipeline_definition=pipeline,
        status="PENDING",
        current_step_index=0,
    )
    db.add(job)
    db.flush()

    for idx, step in enumerate(pipeline_steps):
        db.add(
            JobStep(
                id=str(uuid.uuid4()),
                job_id=job_id,
                step_index=idx,
                step_type=step.step,
                parameters=json.dumps(step.params),
                status="PENDING",
                input_file_id=file_ref.id if idx == 0 else None,
            )
        )

    db.commit()

    # --- enqueue ---
    enqueue_pipeline(job_id)

    logger.info("job_created", job_id=job_id, steps=len(pipeline_steps), size=total_bytes)
    return JobCreateResponse(
        job_id=job_id,
        status="PENDING",
        message="Job accepted and queued for processing",
    )


# ── Status ─────────────────────────────────────────────────────────────────

@router.get("/{job_id}", response_model=JobStatusOut)
def get_job_status(job_id: str, db: Session = Depends(get_db)) -> JobStatusOut:
    job = _get_job_or_404(job_id, db)
    return JobStatusOut.model_validate(job)


@router.get("/{job_id}/progress", response_model=JobProgressOut)
def get_job_progress(job_id: str, db: Session = Depends(get_db)) -> JobProgressOut:
    job = _get_job_or_404(job_id, db)
    total = len(job.steps)
    done = sum(1 for s in job.steps if s.status in ("COMPLETED", "FAILED", "SKIPPED"))
    pct = (done / total * 100) if total else 0.0
    return JobProgressOut(
        id=job.id,
        status=job.status,
        current_step_index=job.current_step_index,
        total_steps=total,
        progress_percent=round(pct, 1),
        error_message=job.error_message,
    )


@router.get("/{job_id}/steps", response_model=list[JobStepOut])
def get_job_steps(job_id: str, db: Session = Depends(get_db)) -> list[JobStepOut]:
    job = _get_job_or_404(job_id, db)
    return [JobStepOut.model_validate(s) for s in job.steps]


# ── Result ─────────────────────────────────────────────────────────────────

@router.get("/{job_id}/result")
def get_job_result(job_id: str, db: Session = Depends(get_db)):
    job = _get_job_or_404(job_id, db)

    if job.status != "COMPLETED":
        if job.status in ("PENDING", "PROCESSING"):
            raise HTTPException(status_code=202, detail="Job is not yet complete")
        raise HTTPException(status_code=422, detail=f"Job ended with status {job.status!r}")

    if job.output_file_id is None:
        raise HTTPException(status_code=404, detail="No output file found for this job")

    ref: FileReference | None = db.get(FileReference, job.output_file_id)
    if ref is None:
        raise HTTPException(status_code=404, detail="Output file record missing")

    if is_expired(ref):
        raise HTTPException(status_code=410, detail="Output file has expired and been deleted")

    path = Path(ref.storage_path)
    if not path.exists():
        raise HTTPException(status_code=410, detail="Output file no longer exists on disk")

    return FileResponse(
        path=str(path),
        filename=ref.original_filename,
        media_type=ref.content_type or "application/octet-stream",
    )


# ── Cancel ─────────────────────────────────────────────────────────────────

@router.post("/{job_id}/cancel", response_model=CancelResponse)
def cancel_job(job_id: str, db: Session = Depends(get_db)) -> CancelResponse:
    job = _get_job_or_404(job_id, db)

    if job.status not in ("PENDING", "PROCESSING"):
        return CancelResponse(
            job_id=job_id,
            cancelled=False,
            message=f"Job cannot be cancelled in status {job.status!r}",
        )

    job.status = "CANCELLED"
    for step in job.steps:
        if step.status == "PENDING":
            step.status = "SKIPPED"
    db.commit()

    logger.info("job_cancelled", job_id=job_id)
    return CancelResponse(job_id=job_id, cancelled=True, message="Job cancelled")


# ── Helpers ────────────────────────────────────────────────────────────────

def _get_job_or_404(job_id: str, db: Session) -> Job:
    job = db.get(Job, job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id!r} not found")
    return job
