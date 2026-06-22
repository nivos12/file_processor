"""
SQLAlchemy ORM models.

Three entities: Job, JobStep, FileReference.
Indices on Job.status and Job.created_at since those are the most common
query predicates (status dashboard, cleanup sweeps).
"""
import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    DateTime, Float, ForeignKey, Index, Integer, String, Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return str(uuid.uuid4())


class FileReference(Base):
    __tablename__ = "file_references"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    storage_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    original_filename: Mapped[str] = mapped_column(String(512), nullable=False)
    size: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    content_type: Mapped[str] = mapped_column(String(128), nullable=False, default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # back-references populated by Job relationship declarations
    input_for_jobs: Mapped[list["Job"]] = relationship(
        "Job", foreign_keys="Job.input_file_id", back_populates="input_file"
    )
    output_for_jobs: Mapped[list["Job"]] = relationship(
        "Job", foreign_keys="Job.output_file_id", back_populates="output_file"
    )
    input_for_steps: Mapped[list["JobStep"]] = relationship(
        "JobStep", foreign_keys="JobStep.input_file_id", back_populates="input_file"
    )
    output_for_steps: Mapped[list["JobStep"]] = relationship(
        "JobStep", foreign_keys="JobStep.output_file_id", back_populates="output_file"
    )


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    input_file_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("file_references.id"), nullable=True
    )
    output_file_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("file_references.id"), nullable=True
    )
    pipeline_definition: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    current_step_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    api_key: Mapped[str | None] = mapped_column(String(256), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=_now
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    input_file: Mapped["FileReference | None"] = relationship(
        "FileReference",
        foreign_keys=[input_file_id],
        back_populates="input_for_jobs",
    )
    output_file: Mapped["FileReference | None"] = relationship(
        "FileReference",
        foreign_keys=[output_file_id],
        back_populates="output_for_jobs",
    )
    steps: Mapped[list["JobStep"]] = relationship(
        "JobStep", back_populates="job", order_by="JobStep.step_index"
    )

    __table_args__ = (
        Index("ix_jobs_status", "status"),
        Index("ix_jobs_created_at", "created_at"),
    )


class JobStep(Base):
    __tablename__ = "job_steps"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    job_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("jobs.id"), nullable=False
    )
    step_index: Mapped[int] = mapped_column(Integer, nullable=False)
    step_type: Mapped[str] = mapped_column(String(64), nullable=False)
    parameters: Mapped[str] = mapped_column(Text, nullable=False, default="{}")
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="PENDING")
    input_file_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("file_references.id"), nullable=True
    )
    output_file_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("file_references.id"), nullable=True
    )
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    duration_seconds: Mapped[float | None] = mapped_column(Float, nullable=True)

    job: Mapped["Job"] = relationship("Job", back_populates="steps")
    input_file: Mapped["FileReference | None"] = relationship(
        "FileReference",
        foreign_keys=[input_file_id],
        back_populates="input_for_steps",
    )
    output_file: Mapped["FileReference | None"] = relationship(
        "FileReference",
        foreign_keys=[output_file_id],
        back_populates="output_for_steps",
    )

    __table_args__ = (
        Index("ix_job_steps_job_id", "job_id"),
        Index("ix_job_steps_status", "status"),
    )
