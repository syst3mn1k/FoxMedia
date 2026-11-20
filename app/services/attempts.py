from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job_attempt import JobAttempt


async def start_attempt(
    session: AsyncSession,
    job_id: UUID,
    attempt_no: int,
    worker_id: str = "worker-1",
) -> JobAttempt:
    attempt = JobAttempt(
        job_id=job_id,
        attempt_no=attempt_no,
        worker_id=worker_id,
        outcome="running",
    )

    session.add(attempt)
    await session.flush()

    return attempt


async def finish_attempt(
    session: AsyncSession,
    job_id: UUID,
    attempt_no: int,
    outcome: str,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> JobAttempt | None:

    stmt = (
        select(JobAttempt)
        .where(
            JobAttempt.job_id == job_id,
            JobAttempt.attempt_no == attempt_no,
        )
        .with_for_update()
    )

    result = await session.execute(stmt)
    attempt = result.scalar_one_or_none()

    # Legacy job without an attempt record.
    if attempt is None:
        return None

    if attempt.outcome != "running":
        return attempt

    ended_at = datetime.now(UTC)

    attempt.outcome = outcome
    attempt.ended_at = ended_at
    attempt.duration_ms = max(
        0,
        round(
            (ended_at - attempt.started_at).total_seconds() * 1000
        ),
    )
    attempt.error_code = error_code
    attempt.error_detail = error_detail

    await session.flush()

    return attempt
