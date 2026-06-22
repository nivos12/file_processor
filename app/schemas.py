"""Pydantic schemas for request/response validation."""
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# ── Pipeline definition ────────────────────────────────────────────────────

class PipelineStep(BaseModel):
    step: str = Field(..., description="Step type name, e.g. 'validate'")
    params: dict[str, Any] = Field(default_factory=dict)


# ── File reference ─────────────────────────────────────────────────────────

class FileReferenceOut(BaseModel):
    id: str
    original_filename: str
    size: int
    content_type: str
    created_at: datetime
    expires_at: datetime | None

    model_config = {"from_attributes": True}


# ── Job step ───────────────────────────────────────────────────────────────

class JobStepOut(BaseModel):
    id: str
    step_index: int
    step_type: str
    parameters: str
    status: str
    error_message: str | None
    started_at: datetime | None
    completed_at: datetime | None
    duration_seconds: float | None
    input_file: FileReferenceOut | None = None
    output_file: FileReferenceOut | None = None

    model_config = {"from_attributes": True}


# ── Job ────────────────────────────────────────────────────────────────────

class JobCreateResponse(BaseModel):
    job_id: str
    status: str
    message: str


class JobStatusOut(BaseModel):
    id: str
    status: str
    current_step_index: int
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
    input_file: FileReferenceOut | None = None
    output_file: FileReferenceOut | None = None
    steps: list[JobStepOut] = []

    model_config = {"from_attributes": True}


class JobProgressOut(BaseModel):
    id: str
    status: str
    current_step_index: int
    total_steps: int
    progress_percent: float
    error_message: str | None

    model_config = {"from_attributes": True}


class CancelResponse(BaseModel):
    job_id: str
    cancelled: bool
    message: str
