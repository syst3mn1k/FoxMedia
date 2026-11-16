from datetime import datetime, timezone
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

    if job.status != "queued":
        raise ValueError(
            f"Job {job_id} cannot start from status {job.status}"
        )

    job.status = "running"
    job.attempts += 1
    job.started_at = datetime.now(timezone.utc)

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

    if job.status != "running":
        raise ValueError(
            f"Job {job_id} cannot be resumed from status {job.status}"
        )

    job.attempts += 1
    job.started_at = datetime.now(timezone.utc)

    record_event(
        session,
        job.id,
        "job.recovered",
        {"attempt": job.attempts},
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
    job.finished_at = datetime.now(timezone.utc)

    record_event(
        session,
        job.id,
        "job.succeeded",
        {"attempt": job.attempts},
    )

    await session.commit()
