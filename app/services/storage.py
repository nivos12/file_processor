"""
File storage service.

Design decisions:
- Files are stored at {storage_root}/{job_id}/{uuid4}{ext} — never the
  user-supplied filename — to prevent path traversal attacks.
- The original filename is preserved only in the FileReference DB row.
- ext is extracted from the sanitized filename for readability in the FS,
  but the path is always UUID-based so traversal is impossible.
"""
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy.orm import Session

from app.config import settings
from app.logging_config import get_logger
from app.models import FileReference

logger = get_logger(__name__)

_STORAGE_ROOT = Path(settings.storage_root)


def _safe_extension(filename: str) -> str:
    """Return lowercased extension from filename, or '' if none / disallowed."""
    p = Path(filename)
    ext = p.suffix.lower()
    allowed = set(settings.allowed_extensions)
    return ext if ext in allowed else ""


def allocate_path(job_id: str, original_filename: str) -> Path:
    """
    Return a safe storage path for a new file.

    Path: {storage_root}/{job_id}/{uuid}{ext}
    The caller is responsible for writing the file to this path.
    """
    job_dir = _STORAGE_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    ext = _safe_extension(original_filename)
    filename = f"{uuid.uuid4()}{ext}"
    return job_dir / filename


def create_file_reference(
    db: Session,
    *,
    storage_path: Path | str,
    original_filename: str,
    size: int,
    content_type: str,
    retention_seconds: int | None = None,
) -> FileReference:
    ret = retention_seconds or settings.file_retention_seconds
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ret)
    ref = FileReference(
        id=str(uuid.uuid4()),
        storage_path=str(storage_path),
        original_filename=original_filename,
        size=size,
        content_type=content_type,
        expires_at=expires_at,
    )
    db.add(ref)
    db.flush()  # get the id without committing
    logger.info(
        "file_reference_created",
        file_id=ref.id,
        path=ref.storage_path,
        size=size,
    )
    return ref


def delete_file(storage_path: str) -> bool:
    """Delete a file from disk. Returns True if deleted, False if not found."""
    p = Path(storage_path)
    if p.exists():
        p.unlink()
        logger.info("file_deleted", path=storage_path)
        return True
    logger.warning("file_not_found_on_delete", path=storage_path)
    return False


def is_expired(ref: FileReference) -> bool:
    if ref.expires_at is None:
        return False
    now = datetime.now(timezone.utc)
    exp = ref.expires_at
    if exp.tzinfo is None:
        exp = exp.replace(tzinfo=timezone.utc)
    return now > exp
