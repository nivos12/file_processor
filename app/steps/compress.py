"""
Compress step.

Supported operations (controlled by `action` param):
  gzip (default): gzip-compress the input file → output.gz
  gunzip: decompress a .gz file → original extension
  zip: zip-compress the input → output.zip
  unzip: extract first file from a .zip archive

Written with explicit strategy dispatch (not if/elif) for extensibility.
"""
import gzip
import shutil
import zipfile
from pathlib import Path
from typing import Any, Callable

from sqlalchemy.orm import Session

from app.config import settings
from app.logging_config import get_logger
from app.steps.base import Step

logger = get_logger(__name__)
_CHUNK = settings.chunk_size

# Registry: action name → handler function
ActionFn = Callable[[Path, Path], dict[str, Any]]
_ACTIONS: dict[str, ActionFn] = {}


def _action(name: str):
    def decorator(fn: ActionFn) -> ActionFn:
        _ACTIONS[name] = fn
        return fn
    return decorator


@_action("gzip")
def _gzip(src: Path, dst: Path) -> dict[str, Any]:
    out = dst.with_suffix(dst.suffix + ".gz") if not dst.suffix.endswith(".gz") else dst
    with src.open("rb") as fin, gzip.open(out, "wb") as fout:
        shutil.copyfileobj(fin, fout, length=_CHUNK)
    return {
        "content_type": "application/gzip",
        "output_filename": src.name + ".gz",
        "output_path_override": out,
    }


@_action("gunzip")
def _gunzip(src: Path, dst: Path) -> dict[str, Any]:
    if not src.suffix.endswith(".gz"):
        raise ValueError(f"compress/gunzip: expected .gz input, got {src.suffix!r}")
    out_name = src.stem  # strip .gz
    out = dst.parent / out_name
    with gzip.open(src, "rb") as fin, out.open("wb") as fout:
        shutil.copyfileobj(fin, fout, length=_CHUNK)
    import mimetypes
    ct, _ = mimetypes.guess_type(out_name)
    return {
        "content_type": ct or "application/octet-stream",
        "output_filename": out_name,
        "output_path_override": out,
    }


@_action("zip")
def _zip(src: Path, dst: Path) -> dict[str, Any]:
    out = dst.with_suffix(".zip") if not dst.suffix.endswith(".zip") else dst
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(src, arcname=src.name)
    return {
        "content_type": "application/zip",
        "output_filename": src.name + ".zip",
        "output_path_override": out,
    }


@_action("unzip")
def _unzip(src: Path, dst: Path) -> dict[str, Any]:
    if not zipfile.is_zipfile(src):
        raise ValueError("compress/unzip: input is not a valid ZIP file")
    with zipfile.ZipFile(src, "r") as zf:
        names = zf.namelist()
        if not names:
            raise ValueError("compress/unzip: ZIP archive is empty")
        # Extract the first file (or specify via param in the future)
        target_name = names[0]
        out = dst.parent / Path(target_name).name
        with zf.open(target_name) as fin, out.open("wb") as fout:
            shutil.copyfileobj(fin, fout, length=_CHUNK)
    import mimetypes
    ct, _ = mimetypes.guess_type(target_name)
    return {
        "content_type": ct or "application/octet-stream",
        "output_filename": Path(target_name).name,
        "output_path_override": out,
    }


class CompressStep(Step):
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
        log = logger.bind(job_id=job_id, step_index=step_index, step="compress")

        if input_path is None or not input_path.exists():
            raise ValueError("compress: input file does not exist")

        action = params.get("action", "gzip")
        handler = _ACTIONS.get(action)
        if handler is None:
            raise ValueError(
                f"compress: unknown action {action!r}. Supported: {list(_ACTIONS)}"
            )

        result = handler(input_path, output_path)

        # Handlers write to a different path than allocated (e.g. .csv → .csv.gz).
        # Signal the actual path to the executor via output_path so FileReference
        # points to a file with the correct extension.
        actual_path = result.pop("output_path_override", None)
        if actual_path is not None:
            result["output_path"] = str(actual_path)
            log.info("compress_completed", action=action, output_size=Path(actual_path).stat().st_size if Path(actual_path).exists() else 0)
        else:
            log.info("compress_completed", action=action, output_size=output_path.stat().st_size if output_path.exists() else 0)

        return result
