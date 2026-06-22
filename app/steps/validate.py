"""
Validate step.

Checks:
- File is not empty.
- Extension/mimetype matches expected_type param (if provided).
- For CSV: attempts to parse the first 1024 bytes to confirm dialect.
- For JSON: attempts json.loads of the full file (acceptable since validation
  is the first step and files are <=100 MB).
- Extracts and returns metadata: size, detected mimetype, line/row count.

Output: a copy of the input file (pass-through); validate doesn't transform data.
We still write an output so the pipeline chain stays uniform.
"""
import csv
import json
import mimetypes
import shutil
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.steps.base import Step

logger = get_logger(__name__)


class ValidateStep(Step):
    def execute(
        self,
        *,
        input_path: Path | None,
        output_path: Path,
        params: dict[str, Any],
        job_id: str,
        step_index: int,
        db: Session,
    ) -> dict[str, Any]:
        log = logger.bind(job_id=job_id, step_index=step_index, step="validate")

        if input_path is None or not input_path.exists():
            raise ValueError("validate: input file does not exist")

        size = input_path.stat().st_size
        if size == 0:
            raise ValueError("validate: file is empty")

        # Detect MIME type from extension
        guessed_type, _ = mimetypes.guess_type(str(input_path))
        ext = input_path.suffix.lower()

        expected_type = params.get("expected_type")
        if expected_type:
            expected_type = expected_type.lower()
            if expected_type not in (ext.lstrip("."), (guessed_type or "")):
                # Allow loose matching: "csv" matches ".csv" or "text/csv"
                normalized_ext = ext.lstrip(".")
                if expected_type != normalized_ext and expected_type not in (guessed_type or ""):
                    raise ValueError(
                        f"validate: expected type {expected_type!r} "
                        f"but got extension {ext!r} (mime: {guessed_type!r})"
                    )

        metadata: dict[str, Any] = {"size": size, "mime_type": guessed_type, "extension": ext}

        # Format-specific integrity checks
        if ext == ".csv":
            metadata.update(_validate_csv(input_path))
        elif ext == ".json":
            metadata.update(_validate_json(input_path))

        log.info("validate_passed", **metadata)

        # Pass-through: copy input to output unchanged
        shutil.copy2(input_path, output_path)

        return {
            "content_type": guessed_type or "application/octet-stream",
            "output_filename": input_path.name,
            "metadata": metadata,
        }


def _validate_csv(path: Path) -> dict[str, Any]:
    row_count = 0
    with path.open(newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        try:
            headers = reader.fieldnames or []
        except csv.Error as exc:
            raise ValueError(f"validate: CSV parse error: {exc}") from exc
        for _ in reader:
            row_count += 1
    return {"row_count": row_count, "headers": list(headers)}


def _validate_json(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"validate: invalid JSON: {exc}") from exc
    if isinstance(data, list):
        return {"record_count": len(data)}
    if isinstance(data, dict):
        return {"keys": list(data.keys())[:20]}  # cap to avoid huge metadata
    return {}
