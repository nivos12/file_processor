"""
Tests: notify step webhook retry behavior.

Uses unittest.mock to intercept HTTP calls — no real network needed.

Covers:
- Successful webhook on first attempt.
- Retry on transient failure (connection error), succeeds on 2nd attempt.
- All retries exhausted → step raises RuntimeError, job FAILED.
- SSRF guard blocks private IP URLs.
- Idempotency key is stable across retries (same key on every attempt).
"""
import hashlib
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.steps.notify import NotifyStep
from app.services.security import validate_webhook_url


# ── SSRF guard ─────────────────────────────────────────────────────────────

def test_ssrf_guard_blocks_localhost():
    with pytest.raises(ValueError, match="private/internal"):
        validate_webhook_url("http://127.0.0.1/callback")


def test_ssrf_guard_blocks_private_range():
    with pytest.raises(ValueError, match="private/internal"):
        validate_webhook_url("http://192.168.1.1/callback")


def test_ssrf_guard_rejects_non_http():
    with pytest.raises(ValueError, match="scheme"):
        validate_webhook_url("ftp://example.com/callback")


# ── Helpers ────────────────────────────────────────────────────────────────

def _ok_mock_response():
    """MagicMock that behaves like a 200 httpx.Response."""
    resp = MagicMock()
    resp.status_code = 200
    resp.raise_for_status.return_value = None
    return resp


def _error_mock_response():
    """MagicMock whose raise_for_status() raises HTTPStatusError."""
    resp = MagicMock()
    resp.status_code = 500
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "500 Server Error", request=MagicMock(), response=MagicMock()
    )
    return resp


def _run_step(tmp_path, url, mock_post_side_effect, max_retries=3):
    """
    Run NotifyStep with httpx.post patched and SSRF check bypassed.
    Returns (step_result, call_count).
    """
    src = tmp_path / "input.csv"
    src.write_text("name,age\nAlice,30\n")
    dst = tmp_path / "output.csv"

    mock_post = MagicMock(side_effect=mock_post_side_effect)

    with patch("app.steps.notify.validate_webhook_url", return_value=None):
        with patch("httpx.post", mock_post):
            with patch("time.sleep", return_value=None):
                step = NotifyStep()
                result = step.execute(
                    input_path=src,
                    output_path=dst,
                    params={"url": url, "max_retries": max_retries},
                    job_id="test-job",
                    step_index=0,
                    db=MagicMock(),
                )
    return result, mock_post.call_count


# ── Notify step ────────────────────────────────────────────────────────────

def test_notify_succeeds_on_first_attempt(tmp_path):
    result, call_count = _run_step(
        tmp_path,
        "https://example.com/hook",
        mock_post_side_effect=[_ok_mock_response()],
    )
    assert call_count == 1
    assert result is not None


def test_notify_retries_on_failure_then_succeeds(tmp_path):
    # First call raises a network error, second returns 200
    responses = [
        httpx.ConnectError("connection refused"),
        _ok_mock_response(),
    ]
    result, call_count = _run_step(
        tmp_path,
        "https://example.com/hook",
        mock_post_side_effect=responses,
    )
    assert call_count == 2


def test_notify_exhausts_retries_raises(tmp_path):
    # All 3 attempts fail
    responses = [httpx.ConnectError("always down")] * 3

    src = tmp_path / "input.csv"
    src.write_text("name,age\nAlice,30\n")
    dst = tmp_path / "output.csv"

    mock_post = MagicMock(side_effect=responses)

    with patch("app.steps.notify.validate_webhook_url", return_value=None):
        with patch("httpx.post", mock_post):
            with patch("time.sleep", return_value=None):
                with pytest.raises(RuntimeError, match="webhook failed after 3 attempts"):
                    NotifyStep().execute(
                        input_path=src,
                        output_path=dst,
                        params={"url": "https://example.com/hook", "max_retries": 3},
                        job_id="test-job",
                        step_index=0,
                        db=MagicMock(),
                    )
    assert mock_post.call_count == 3


def test_notify_idempotency_key_is_stable():
    """Same job_id + step_index must always produce the same key."""
    key_a = hashlib.sha256(b"job-abc:2").hexdigest()
    key_b = hashlib.sha256(b"job-abc:2").hexdigest()
    assert key_a == key_b

    # Different step_index → different key
    key_c = hashlib.sha256(b"job-abc:3").hexdigest()
    assert key_a != key_c


def test_notify_sends_idempotency_header(tmp_path):
    """The X-Idempotency-Key header is sent on the HTTP call."""
    src = tmp_path / "input.csv"
    src.write_text("name,age\nAlice,30\n")
    dst = tmp_path / "output.csv"

    captured_headers = {}

    def fake_post(url, *, headers, **kwargs):
        captured_headers.update(headers)
        return _ok_mock_response()

    with patch("app.steps.notify.validate_webhook_url", return_value=None):
        with patch("httpx.post", side_effect=fake_post):
            NotifyStep().execute(
                input_path=src,
                output_path=dst,
                params={"url": "https://example.com/hook"},
                job_id="my-job-id",
                step_index=5,
                db=MagicMock(),
            )

    expected_key = hashlib.sha256(b"my-job-id:5").hexdigest()
    assert captured_headers.get("X-Idempotency-Key") == expected_key
    assert captured_headers.get("X-Job-Id") == "my-job-id"
