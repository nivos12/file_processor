"""
Transform step — filter rows, select columns, apply string transforms.

CSV: streaming via csv.DictReader row-by-row — never loads full file into list.
JSON: one-shot load (JSON has no line-by-line streaming format by nature).

Supported params:
  filter_field (str): column/key to filter on
  filter_value (str): keep rows where filter_field == filter_value
  filter_op (str): "eq" (default) | "contains" | "gt" | "lt"
  select_fields (list[str]): only include these fields in output
  string_transforms (dict[str, str]): {"field": "upper|lower|trim|strip"}
  output_format: "csv" | "json" — defaults to same as input
"""
import csv
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.steps import formats
from app.steps.base import Step

logger = get_logger(__name__)


class TransformStep(Step):
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
        log = logger.bind(job_id=job_id, step_index=step_index, step="transform")

        if input_path is None or not input_path.exists():
            raise ValueError("transform: input file does not exist")

        in_fmt = formats.detect_format(input_path)
        out_format = params.get("output_format", in_fmt)

        filter_field = params.get("filter_field")
        filter_value = params.get("filter_value")
        filter_op = params.get("filter_op", "eq")
        select_fields: list[str] | None = params.get("select_fields")
        string_transforms: dict[str, str] = params.get("string_transforms", {})

        if in_fmt == "csv":
            rows_in, rows_out = _transform_csv(
                input_path, output_path, out_format,
                filter_field, filter_value, filter_op,
                select_fields, string_transforms,
            )
        else:
            rows_in, rows_out = _transform_json(
                input_path, output_path, out_format,
                filter_field, filter_value, filter_op,
                select_fields, string_transforms,
            )

        log.info("transform_completed", rows_in=rows_in, rows_out=rows_out)

        return {
            "content_type": formats.content_type_for(out_format),
            "output_filename": input_path.stem + f".{out_format}",
        }


# ── CSV transform ──────────────────────────────────────────────────────────

def _transform_csv(
    src: Path, dst: Path, out_format: str,
    filter_field, filter_value, filter_op,
    select_fields, string_transforms,
) -> tuple[int, int]:
    rows_in = 0
    rows_out = 0

    with src.open(newline="", encoding="utf-8", errors="replace") as fin:
        reader = csv.DictReader(fin)
        headers = list(reader.fieldnames or [])
        out_headers = select_fields if select_fields else headers

        if out_format == "json":
            records: list[dict] = []
            for row in reader:
                rows_in += 1
                if not _passes_filter(row, filter_field, filter_value, filter_op):
                    continue
                row = _apply_string_transforms(row, string_transforms)
                records.append({k: row[k] for k in out_headers if k in row})
                rows_out += 1
            formats.write_json(records, dst)
        else:
            with dst.open("w", newline="", encoding="utf-8") as fout:
                writer = csv.DictWriter(fout, fieldnames=out_headers, extrasaction="ignore")
                writer.writeheader()
                for row in reader:
                    rows_in += 1
                    if not _passes_filter(row, filter_field, filter_value, filter_op):
                        continue
                    row = _apply_string_transforms(row, string_transforms)
                    writer.writerow({k: row.get(k, "") for k in out_headers})
                    rows_out += 1

    return rows_in, rows_out


# ── JSON transform ─────────────────────────────────────────────────────────

def _transform_json(
    src: Path, dst: Path, out_format: str,
    filter_field, filter_value, filter_op,
    select_fields, string_transforms,
) -> tuple[int, int]:
    data = formats.read_json(src)
    rows_in = len(data)

    out_records = []
    for row in data:
        if not _passes_filter(row, filter_field, filter_value, filter_op):
            continue
        row = _apply_string_transforms(row, string_transforms)
        if select_fields:
            row = {k: row[k] for k in select_fields if k in row}
        out_records.append(row)

    rows_out = len(out_records)

    if out_format == "csv":
        headers = select_fields or (list(out_records[0].keys()) if out_records else [])
        formats.write_csv(out_records, dst, headers)
    else:
        formats.write_json(out_records, dst)

    return rows_in, rows_out


# ── Helpers ────────────────────────────────────────────────────────────────

def _passes_filter(
    row: dict, field: str | None, value: str | None, op: str
) -> bool:
    if field is None:
        return True
    cell = str(row.get(field, ""))
    val = str(value) if value is not None else ""
    if op == "eq":
        return cell == val
    if op == "contains":
        return val in cell
    if op == "gt":
        try:
            return float(cell) > float(val)
        except ValueError:
            return cell > val
    if op == "lt":
        try:
            return float(cell) < float(val)
        except ValueError:
            return cell < val
    return True


def _apply_string_transforms(row: dict, transforms: dict[str, str]) -> dict:
    if not transforms:
        return row
    result = dict(row)
    for field, op in transforms.items():
        if field not in result:
            continue
        v = str(result[field])
        if op == "upper":
            result[field] = v.upper()
        elif op == "lower":
            result[field] = v.lower()
        elif op in ("trim", "strip"):
            result[field] = v.strip()
    return result
