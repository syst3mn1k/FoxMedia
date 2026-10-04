from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.models.job import Job
from app.queues.streams import enqueue_job


async def create_job(
    session: AsyncSession,
    command_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None = None,
) -> Job:
    job = Job(
        command_type=command_type,
        status="queued",
        payload=payload,
        attempts=0,
        idempotency_key=idempotency_key,
    )

    session.add(job)
    await session.commit()
    await session.refresh(job)

    return job


async def create_and_enqueue_job(
    session: AsyncSession,
    client: redis.Redis,
    command_type: str,
    payload: dict[str, Any],
    idempotency_key: str | None = None,
) -> Job:
    job = await create_job(
        session=session,
        command_type=command_type,
        payload=payload,
        idempotency_key=idempotency_key,
    )

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

    await session.commit()
