"""
Notify step — POST a webhook with job status + result location.

Retry strategy: exponential backoff, configurable attempts (default 3).
Idempotency: each webhook call carries a stable `X-Idempotency-Key` header
derived from job_id + step_index. If the receiver is idempotent on this key,
retries won't cause duplicate processing.

SSRF guard: webhook URL is validated against private IP ranges before any
HTTP call is made (see services/security.py).

Output: a copy of the input file (notify is a side-effect step, not a
transformation). The pipeline chain stays uniform.
"""
import hashlib
import json
import shutil
import time
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy.orm import Session

from app.logging_config import get_logger
from app.services.security import validate_webhook_url
from app.steps.base import Step

logger = get_logger(__name__)


class NotifyStep(Step):
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
        log = logger.bind(job_id=job_id, step_index=step_index, step="notify")

        url: str | None = params.get("url")
        if not url:
            raise ValueError("notify: 'url' param is required")

        # SSRF guard — raises ValueError if URL targets internal network
        validate_webhook_url(url)

        max_retries: int = int(params.get("max_retries", 3))
        timeout: float = float(params.get("timeout_seconds", 10))

        # Stable idempotency key: SHA-256 of job_id + step_index
        idempotency_key = hashlib.sha256(
            f"{job_id}:{step_index}".encode()
        ).hexdigest()

        # Build payload — by the time notify executes, all prior steps have
        # completed successfully, so the effective pipeline status is COMPLETED.
        # (The DB row is still PROCESSING until the executor finishes, but the
        # receiver cares about the logical outcome, not the DB state machine.)
        payload = {
            "job_id": job_id,
            "status": "COMPLETED",
            "step_index": step_index,
            "result_url": params.get("result_url", f"/jobs/{job_id}/result"),
            "metadata": params.get("metadata", {}),
        }

        headers = {
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key,
            "X-Job-Id": job_id,
        }

        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                response = httpx.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=timeout,
                    follow_redirects=False,  # don't follow redirects to avoid SSRF bypass
                )
                response.raise_for_status()
                log.info(
                    "webhook_sent",
                    attempt=attempt,
                    status_code=response.status_code,
                    idempotency_key=idempotency_key,
                )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt < max_retries:
                    backoff = 2 ** attempt  # 2s, 4s, 8s...
                    log.warning(
                        "webhook_retry",
                        attempt=attempt,
                        next_in=backoff,
                        error=str(exc),
                    )
                    time.sleep(backoff)
                else:
                    log.error(
                        "webhook_failed",
                        attempts=max_retries,
                        error=str(exc),
                    )

        if last_error is not None:
            raise RuntimeError(
                f"notify: webhook failed after {max_retries} attempts: {last_error}"
            )

        # Pass-through: copy input to output
        if input_path and input_path.exists():
            shutil.copy2(input_path, output_path)
            ct = "application/octet-stream"
            fname = input_path.name
        else:
            output_path.write_text(json.dumps({"notified": True, "job_id": job_id}))
            ct = "application/json"
            fname = "notify_result.json"

        return {"content_type": ct, "output_filename": fname}
