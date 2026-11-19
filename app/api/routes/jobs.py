from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.schemas.jobs import JobAccepted, JobCreate, JobEventRead, JobRead
from app.services.jobs import (
    JobCancellationConflict,
    cancel_job,
    create_and_enqueue_job,
    get_job,
    list_job_events,
)

router = APIRouter()


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobAccepted,
)
async def create_job_endpoint(
    command: JobCreate,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> JobAccepted:
    job = await create_and_enqueue_job(
        session=session,
        client=request.app.state.redis,
        command_type=command.command_type,
        payload=command.payload,
        idempotency_key=command.idempotency_key,
    )

    return JobAccepted(
        job_id=job.id,
        status=job.status,
    )


@router.get(
    "/{job_id}",
    response_model=JobRead,
)
async def read_job_endpoint(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> JobRead:
    try:
        job = await get_job(
            session=session,
            job_id=str(job_id),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        ) from exc

    return JobRead.model_validate(job)


@router.get(
    "/{job_id}/events",
    response_model=list[JobEventRead],
)
async def read_job_events_endpoint(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> list[JobEventRead]:
    try:
        events = await list_job_events(
            session=session,
            job_id=str(job_id),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        ) from exc

    return [
        JobEventRead.model_validate(event)
        for event in events
    ]

@router.post(
    "/{job_id}/cancel",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=JobAccepted,
)
async def cancel_job_endpoint(
    job_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> JobAccepted:
    try:
        job = await cancel_job(
            session=session,
            job_id=str(job_id),
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found",
        ) from exc
    except JobCancellationConflict as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc

    return JobAccepted(
        job_id=job.id,
        status=job.status,
    )