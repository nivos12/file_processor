"""
Convert step — format conversion via a converter registry.

Adding a new conversion pair:
1. Write a function def convert_X_to_Y(src, dst) -> None.
2. Register it with @_register("x", "y").

The pipeline param `to` specifies the target format (e.g. "json", "csv").
Source format is inferred from the input file extension.
"""
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.steps import formats
from app.steps.base import Step

logger = get_logger(__name__)

# Registry: (from_format, to_format) → converter function
ConverterFn = Callable[[Path, Path], None]
CONVERTERS: dict[tuple[str, str], ConverterFn] = {}


def _register(from_fmt: str, to_fmt: str):
    def decorator(fn: ConverterFn) -> ConverterFn:
        CONVERTERS[(from_fmt, to_fmt)] = fn
        return fn
    return decorator


# ── CSV → JSON ─────────────────────────────────────────────────────────────

@_register("csv", "json")
def csv_to_json(src: Path, dst: Path) -> None:
    import csv
    with src.open(newline="", encoding="utf-8", errors="replace") as f:
        records = [dict(row) for row in csv.DictReader(f)]
    formats.write_json(records, dst)


# ── JSON → CSV ─────────────────────────────────────────────────────────────

@_register("json", "csv")
def json_to_csv(src: Path, dst: Path) -> None:
    data = formats.read_json(src)
    if not data:
        dst.write_text("")
        return

    # Gather all keys in order of first appearance
    headers: list[str] = []
    seen: set[str] = set()
    for row in data:
        for k in row:
            if k not in seen:
                headers.append(k)
                seen.add(k)

    formats.write_csv(data, dst, headers)


# ── Step class ─────────────────────────────────────────────────────────────

class ConvertStep(Step):
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
        log = logger.bind(job_id=job_id, step_index=step_index, step="convert")

        if input_path is None or not input_path.exists():
            raise ValueError("convert: input file does not exist")

        from_fmt = formats.detect_format(input_path)
        to_fmt = str(params.get("to", "")).lower()
        if not to_fmt:
            raise ValueError("convert: 'to' param is required (e.g. 'json' or 'csv')")

        converter = CONVERTERS.get((from_fmt, to_fmt))
        if converter is None:
            supported = [f"{a}→{b}" for a, b in CONVERTERS]
            raise ValueError(
                f"convert: no converter for {from_fmt!r} → {to_fmt!r}. "
                f"Supported: {supported}"
            )

        # Output path uses the target extension so the next step sees the right format
        actual_output = output_path.with_suffix(f".{to_fmt}")
        converter(input_path, actual_output)

        out_name = input_path.stem + f".{to_fmt}"
        log.info("convert_completed", from_fmt=from_fmt, to_fmt=to_fmt)

        return {
            "content_type": formats.content_type_for(to_fmt),
            "output_filename": out_name,
            "output_path": str(actual_output),  # actual path written (extension adjusted)
        }
