from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import redis.asyncio as redis
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.audit import AuditEvent
from app.models.job import Job
from app.queues.streams import enqueue_job


class JobCancellationConflict(Exception):
    pass


def record_event(
    session: AsyncSession,
    job_id: UUID,
    event_type: str,
    details: dict[str, Any] | None = None,
) -> None:
    session.add(
        AuditEvent(
            job_id=job_id,
            event_type=event_type,
            details=details,
        )
    )


async def _create_job(
    session: AsyncSession,
    command_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None = None,
) -> tuple[Job, bool]:

    if idempotency_key is not None:
        existing = await session.scalar(
            select(Job).where(
                Job.idempotency_key == idempotency_key
            )
        )

        if existing is not None:
            return existing, False

    job = Job(
        command_type=command_type,
        status="queued",
        payload=payload,
        attempts=0,
        idempotency_key=idempotency_key,
    )

    session.add(job)

    try:
        await session.flush()

        record_event(
            session,
            job.id,
            "job.created",
            {"command_type": command_type},
        )

        await session.commit()

    except IntegrityError:
        await session.rollback()

        if idempotency_key is not None:
            existing = await session.scalar(
                select(Job).where(
                    Job.idempotency_key == idempotency_key
                )
            )

            if existing is not None:
                return existing, False

        raise

    await session.refresh(job)

    return job, True


async def create_job(
    session: AsyncSession,
    command_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None = None,
) -> Job:

    job, _ = await _create_job(
        session=session,
        command_type=command_type,
        payload=payload,
        idempotency_key=idempotency_key,
    )

    return job


async def create_and_enqueue_job(
    session: AsyncSession,
    client: redis.Redis,
    command_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None = None,
) -> Job:

    job, created = await _create_job(
        session=session,
        command_type=command_type,
        payload=payload,
        idempotency_key=idempotency_key,
    )

    if created:
        await enqueue_job(
            client,
            settings.redis_stream,
            {
                "job_id": str(job.id),
                "command_type": job.command_type,
                "correlation_id": str(job.correlation_id),
            },
        )

    return job


async def start_job(
    session: AsyncSession,
    job_id: str,
) -> Job:
    job = await session.get(
        Job,
        UUID(job_id),
        with_for_update=True,
    )

    if job is None:
        raise ValueError(f"Job not found: {job_id}")

    if job.status in {"succeeded", "failed", "cancelled"}:
        return job

    if job.status != "queued":
        raise ValueError(
            f"Job {job_id} cannot start from status {job.status}"
        )

    job.status = "running"
    job.attempts += 1
    job.started_at = datetime.now(UTC)

    record_event(
        session,
        job.id,
        "job.started",
        {"attempt": job.attempts},
    )

    await session.commit()
    await session.refresh(job)

    return job


async def get_job(
    session: AsyncSession,
    job_id: str,
) -> Job:
    job = await session.get(
        Job,
        UUID(job_id),
    )

    if job is None:
        raise ValueError(f"Job not found: {job_id}")

    return job


async def cancel_job(
    session: AsyncSession,
    job_id: str,
) -> Job:
    job = await session.get(
        Job,
        UUID(job_id),
        with_for_update=True,
    )

    if job is None:
        raise ValueError(f"Job not found: {job_id}")

    if job.status == "cancelled":
        return job

    if job.status != "queued":
        raise JobCancellationConflict(
            f"Job {job_id} cannot be cancelled from status {job.status}"
        )

    job.status = "cancelled"
    job.finished_at = datetime.now(UTC)

    record_event(
        session,
        job.id,
        "job.cancelled",
        {"previous_status": "queued"},
    )

    await session.commit()
    await session.refresh(job)

    return job


async def list_job_events(
    session: AsyncSession,
    job_id: str,
) -> list[AuditEvent]:
    job = await get_job(session, job_id)

    result = await session.scalars(
        select(AuditEvent)
        .where(AuditEvent.job_id == job.id)
        .order_by(
            AuditEvent.created_at.asc(),
            AuditEvent.id.asc(),
        )
    )

    return list(result.all())


async def schedule_retry(
    session: AsyncSession,
    job: Job,
    delay_seconds: float,
    error_code: str,
) -> None:
    now = datetime.now(UTC)

    job.status = "retry_wait"
    job.retry_at = now + timedelta(seconds=delay_seconds)
    job.error_code = error_code

    record_event(
        session,
        job.id,
        "job.retry_scheduled",
        {
            "attempt": job.attempts,
            "error_code": error_code,
            "delay_seconds": delay_seconds,
            "retry_at": job.retry_at.isoformat(),
        },
    )

    await session.commit()


async def fail_job(
    session: AsyncSession,
    job: Job,
    error_code: str,
    error_detail: str | None = None,
) -> None:
    job.status = "failed"
    job.error_code = error_code
    job.error_detail = error_detail
    job.finished_at = datetime.now(UTC)
    job.retry_at = None

    record_event(
        session,
        job.id,
        "job.failed",
        {
            "attempt": job.attempts,
            "error_code": error_code,
        },
    )

    await session.commit()


async def resume_job(
    session: AsyncSession,
    job_id: str,
) -> Job:
    job = await session.get(
        Job,
        UUID(job_id),
        with_for_update=True,
    )

    if job is None:
        raise ValueError(f"Job not found: {job_id}")

    if job.status in {"succeeded", "failed", "cancelled"}:
        return job

    now = datetime.now(UTC)
    previous_status = job.status

    if previous_status == "retry_wait":
        if job.retry_at is not None and job.retry_at > now:
            return job

    elif previous_status != "running":
        raise ValueError(
            f"Job {job_id} cannot be resumed from status {job.status}"
        )

    job.status = "running"
    job.attempts += 1
    job.started_at = now
    job.retry_at = None

    event_type = (
        "job.retry_started"
        if previous_status == "retry_wait"
        else "job.recovered"
    )

    record_event(
        session,
        job.id,
        event_type,
        {
            "attempt": job.attempts,
            "previous_status": previous_status,
        },
    )

    await session.commit()
    await session.refresh(job)

    return job


async def complete_job(
    session: AsyncSession,
    job: Job,
    result: dict[str, Any],
) -> None:
    job.status = "succeeded"
    job.result = result
    job.finished_at = datetime.now(UTC)

    record_event(
        session,
        job.id,
        "job.succeeded",
        {"attempt": job.attempts},
    )

    await session.commit()
