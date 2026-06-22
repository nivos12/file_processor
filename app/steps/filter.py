"""
Filter step — keep only records where a field's value is in an allowed set.

This is an exact-match allowlist filter.  For inequality or substring filters
use the transform step (filter_op param).

Params:
  field (str):         Column/key name to inspect.
  valid_values (list): Rows whose field value is NOT in this list are dropped.

Supports .csv (streamed row-by-row) and .json input.
Output format matches input format.
"""
import csv
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.steps import formats
from app.steps.base import Step

logger = get_logger(__name__)


class FilterStep(Step):
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
        log = logger.bind(job_id=job_id, step_index=step_index, step="filter")

        if input_path is None or not input_path.exists():
            raise ValueError("filter: input file does not exist")

        field: str | None = params.get("field")
        valid_values: list[str] = [str(v) for v in params.get("valid_values", [])]

        if not field:
            raise ValueError("filter: 'field' param is required")
        if not valid_values:
            raise ValueError("filter: 'valid_values' must be a non-empty list")

        allowed = set(valid_values)
        fmt = formats.detect_format(input_path)

        rows_in = 0
        rows_out = 0

        if fmt == "csv":
            kept: list[dict] = []
            headers: list[str] = []
            with input_path.open(newline="", encoding="utf-8", errors="replace") as f:
                reader = csv.DictReader(f)
                headers = list(reader.fieldnames or [])
                for row in reader:
                    rows_in += 1
                    if str(row.get(field, "")) in allowed:
                        kept.append(dict(row))
                        rows_out += 1
            formats.write_csv(kept, output_path, headers)
        else:
            records = formats.read_json(input_path)
            rows_in = len(records)
            kept = [r for r in records if str(r.get(field, "")) in allowed]
            rows_out = len(kept)
            formats.write_json(kept, output_path)

        log.info(
            "filter_completed",
            field=field,
            valid_values=valid_values,
            rows_in=rows_in,
            rows_out=rows_out,
        )

        return {
            "content_type": formats.content_type_for(fmt),
            "output_filename": input_path.name,
        }
