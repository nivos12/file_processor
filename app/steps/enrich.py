"""
Enrich step — attach job/file metadata to the processed data.

Wraps the input records in a structured envelope so consumers can audit
how and when the data was processed.  Output is always JSON.

Available metadata fields (all included by default; use `include` param to
restrict to a subset):
  job_id            — the pipeline job identifier
  upload_time       — when the file was uploaded (Job.created_at)
  processing_time   — ISO-8601 UTC timestamp when this step ran
  row_count         — number of records in the input file
  file_size_bytes   — size of the input file on disk

Output shape:
  {
    "_metadata": { "job_id": "...", "row_count": 42, ... },
    "data": [ ...original records... ]
  }

The output path is adjusted to .json because the envelope format is always JSON.
The executor will use result["output_path"] to track the actual written file.
"""
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.models import Job
from app.steps import formats
from app.steps.base import Step

logger = get_logger(__name__)

_ALL_FIELDS = {"job_id", "upload_time", "processing_time", "row_count", "file_size_bytes"}


class EnrichStep(Step):
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
        log = logger.bind(job_id=job_id, step_index=step_index, step="enrich")

        if input_path is None or not input_path.exists():
            raise ValueError("enrich: input file does not exist")

        include: set[str] = set(params.get("include", list(_ALL_FIELDS)))
        fmt = formats.detect_format(input_path)

        # Read records from input (streaming for CSV)
        if fmt == "csv":
            with input_path.open(newline="", encoding="utf-8", errors="replace") as f:
                records: list[dict] = [dict(row) for row in csv.DictReader(f)]
        else:
            records = formats.read_json(input_path)

        # Build metadata block
        meta: dict[str, Any] = {}

        if "job_id" in include:
            meta["job_id"] = job_id

        if "processing_time" in include:
            meta["processing_time"] = datetime.now(timezone.utc).isoformat()

        if "upload_time" in include:
            job: Job | None = db.get(Job, job_id)
            if job and job.created_at:
                ts = job.created_at
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                meta["upload_time"] = ts.isoformat()

        if "row_count" in include:
            meta["row_count"] = len(records)

        if "file_size_bytes" in include:
            meta["file_size_bytes"] = input_path.stat().st_size

        # Always output JSON; adjust path extension so next step sees .json
        actual_output = output_path.with_suffix(".json")
        with actual_output.open("w", encoding="utf-8") as f:
            json.dump({"_metadata": meta, "data": records}, f, indent=2)

        log.info("enrich_completed", row_count=len(records), fields=sorted(meta.keys()))

        return {
            "content_type": "application/json",
            "output_filename": input_path.stem + "_enriched.json",
            "output_path": str(actual_output),
        }
