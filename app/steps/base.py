"""
Step base class (strategy interface).

Every step must subclass Step and implement execute().

execute() contract:
  - input_path: Path to the current working file (None only for first step if no input).
  - output_path: Caller-allocated path where the step MUST write its output.
  - params: Dict of step-specific config from the pipeline JSON.
  - job_id / step_index: For logging context.
  - db: Live SQLAlchemy session (for steps that need to read/write DB, e.g. notify).
  - Returns a dict with at least {"content_type": str, "output_filename": str},
    or None to let the executor infer defaults.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from sqlalchemy.orm import Session


class Step(ABC):
    @abstractmethod
    def execute(
        self,
        *,
        input_path: Path | None,
        output_path: Path,
        params: dict[str, Any],
        job_id: str,
        step_index: int,
        db: Session,
    ) -> dict[str, Any] | None:
        """
        Execute the step. Must write output to output_path.
        Returns metadata dict or None.
        """
        ...
