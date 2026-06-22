"""
Shared file format helpers for CSV and JSON steps.

Steps that read/write CSV or JSON import these helpers instead of
re-implementing the same patterns.  CSV *streaming* (row-by-row DictReader)
is kept inline in the individual steps because the open file handle must stay
alive for the duration of the loop; only the non-streaming patterns live here.
"""
import csv
import json
from pathlib import Path
from typing import Any

CONTENT_TYPES: dict[str, str] = {
    "csv": "text/csv",
    "json": "application/json",
}


def detect_format(path: Path) -> str:
    """Return 'csv' or 'json' from file extension, or raise ValueError."""
    ext = path.suffix.lower().lstrip(".")
    if ext not in CONTENT_TYPES:
        raise ValueError(
            f"Unsupported format {path.suffix!r}; expected .csv or .json"
        )
    return ext


def content_type_for(fmt: str) -> str:
    return CONTENT_TYPES.get(fmt, "application/octet-stream")


def read_json(path: Path) -> list[dict[str, Any]]:
    """Load a JSON file and normalise to a list of dicts."""
    with path.open(encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        data = [data]
    return [r for r in data if isinstance(r, dict)]


def write_json(records: list[dict[str, Any]], path: Path) -> None:
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)


def write_csv(
    records: list[dict[str, Any]],
    path: Path,
    headers: list[str] | None = None,
) -> None:
    """Write records as CSV. Headers default to keys of the first record."""
    if not records and not headers:
        path.write_text("")
        return
    fieldnames = headers or list(records[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in records:
            writer.writerow(row)
