"""
Pytest fixtures.

Isolation strategy:
- Each test gets a fresh SQLite DB file in a tmp directory (function scope).
- Storage root is also redirected to a tmp directory.
- app.database.SessionLocal and app.workers.pipeline.SessionLocal are both
  patched to use the test DB engine so that execute_pipeline() — which opens
  its own session — sees the same data the test committed.
- The RQ enqueue call is patched to a no-op; tests call execute_pipeline() directly.
- HTTP calls in the notify step are intercepted by unittest.mock.
"""
import json
import os
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# ── Shared temp dir for the whole test session ─────────────────────────────
# (individual DB files live inside via tmp_path fixture)

_SESSION_TMPDIR = tempfile.mkdtemp(prefix="fileproc_test_")

# Set env vars BEFORE any app module is imported
os.environ["DATABASE_URL"] = f"sqlite:///{_SESSION_TMPDIR}/initial.db"
os.environ["STORAGE_ROOT"] = f"{_SESSION_TMPDIR}/files"
os.environ["REDIS_URL"] = "redis://localhost:6379/15"  # unused in tests

from app.config import settings  # noqa: E402

settings.storage_root = f"{_SESSION_TMPDIR}/files"
Path(settings.storage_root).mkdir(parents=True, exist_ok=True)

from app.database import Base, get_db  # noqa: E402
from app.main import app  # noqa: E402
import app.workers.pipeline as pipeline_module  # noqa: E402


# ── Per-test DB isolation ──────────────────────────────────────────────────

@pytest.fixture
def test_engine(tmp_path):
    """Fresh SQLite DB file per test."""
    db_path = tmp_path / "test.db"
    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False},
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


@pytest.fixture
def db(test_engine):
    """
    DB session connected to the per-test SQLite file.
    Data is committed normally (not wrapped in a rollback); the DB file is
    discarded after the test via tmp_path cleanup.
    """
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = TestSession()
    yield session
    session.close()


@pytest.fixture
def client(db, test_engine, monkeypatch):
    """
    TestClient with:
    - DB dependency overridden to the per-test session.
    - SessionLocal in pipeline.py patched to use the same test engine.
    - RQ enqueue replaced with a no-op.
    """
    TestSession = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

    def override_get_db():
        s = TestSession()
        try:
            yield s
        finally:
            s.close()

    app.dependency_overrides[get_db] = override_get_db

    # Patch SessionLocal used inside execute_pipeline so it sees the test DB
    monkeypatch.setattr(pipeline_module, "SessionLocal", TestSession)

    # Also patch the database module's SessionLocal for anything that imports it
    import app.database as db_module
    monkeypatch.setattr(db_module, "SessionLocal", TestSession)

    # Prevent real Redis enqueue
    monkeypatch.setattr("app.api.jobs.enqueue_pipeline", lambda job_id: None)

    with TestClient(app, raise_server_exceptions=True) as c:
        yield c

    app.dependency_overrides.clear()


@pytest.fixture
def sample_csv(tmp_path) -> Path:
    p = tmp_path / "data.csv"
    p.write_text("name,age,city\nAlice,30,NYC\nBob,25,LA\nCarol,35,NYC\n")
    return p


@pytest.fixture
def sample_json(tmp_path) -> Path:
    p = tmp_path / "data.json"
    p.write_text(json.dumps([
        {"name": "Alice", "age": 30, "city": "NYC"},
        {"name": "Bob", "age": 25, "city": "LA"},
    ]))
    return p
