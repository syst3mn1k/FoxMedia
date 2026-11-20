
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

import app.services.jobs as jobs_module


@pytest.mark.asyncio
async def test_resume_job_from_queued_after_worker_crash(monkeypatch):
    job_id = uuid4()

    job = SimpleNamespace(
        id=job_id,
        status="queued",
        attempts=0,
        started_at=None,
        retry_at=None,
    )

    session = SimpleNamespace(
        get=AsyncMock(return_value=job),
        commit=AsyncMock(),
        refresh=AsyncMock(),
    )

    events = []

    monkeypatch.setattr(
        jobs_module,
        "record_event",
        lambda session, job_id, event_type, details:
            events.append((event_type, details)),
    )

    start_attempt_mock = AsyncMock()

    monkeypatch.setattr(
        jobs_module,
        "start_attempt",
        start_attempt_mock,
    )

    result = await jobs_module.resume_job(
        session=session,
        job_id=str(job_id),
    )

    assert result.status == "running"
    assert result.attempts == 1
    assert result.started_at is not None
    assert result.retry_at is None

    session.commit.assert_awaited_once()
    session.refresh.assert_awaited_once_with(job)

    assert events == [
        (
            "job.recovered",
            {
                "attempt": 1,
                "previous_status": "queued",
            },
        )
    ]


@pytest.mark.asyncio
async def test_resume_job_does_not_restart_terminal_job(monkeypatch):
    job = SimpleNamespace(
        id=uuid4(),
        status="succeeded",
        attempts=1,
        started_at=None,
        retry_at=None,
    )

    session = SimpleNamespace(
        get=AsyncMock(return_value=job),
        commit=AsyncMock(),
        refresh=AsyncMock(),
    )

    result = await jobs_module.resume_job(
        session=session,
        job_id=str(job.id),
    )

    assert result.status == "succeeded"
    assert result.attempts == 1
    session.commit.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_job_clears_previous_retry_error(monkeypatch):
    job = SimpleNamespace(
        id=uuid4(),
        status="running",
        attempts=2,
        result=None,
        finished_at=None,
        error_code="HTTP_503",
        error_detail="HTTPStatusError",
        retry_at="previous-retry",
    )

    session = SimpleNamespace(
        commit=AsyncMock(),
    )

    events = []

    monkeypatch.setattr(
        jobs_module,
        "record_event",
        lambda session, job_id, event_type, details:
            events.append((event_type, details)),
    )

    finish_attempt_mock = AsyncMock()

    monkeypatch.setattr(
        jobs_module,
        "finish_attempt",
        finish_attempt_mock,
    )

    await jobs_module.complete_job(
        session=session,
        job=job,
        result={"verified": True},
    )

    assert job.status == "succeeded"
    assert job.result == {"verified": True}
    assert job.error_code is None
    assert job.error_detail is None
    assert job.retry_at is None
    assert job.finished_at is not None

    session.commit.assert_awaited_once()

    assert events[0][0] == "job.succeeded"
