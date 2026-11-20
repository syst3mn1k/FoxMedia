import asyncio
import json
import logging
import time
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as redis
from redis.exceptions import ResponseError

from app.core.config import settings
from app.core.errors import classify_job_error
from app.core.logging import configure_logging
from app.db.session import SessionFactory
from app.queues.streams import reclaim_pending
from app.services.jobs import (
    complete_job,
    fail_job,
    resume_job,
    schedule_retry,
    start_job,
)
from app.services.outbox_publisher import OutboxPublisher
from app.services.retry import calculate_backoff
from app.workers.dispatcher import dispatch

logger = logging.getLogger(__name__)


async def resume_when_due(
    session,
    job_id: str,
):
    job = await resume_job(
        session=session,
        job_id=job_id,
    )

    while job.status == "retry_wait":
        if job.retry_at is not None:
            remaining = (job.retry_at - datetime.now(UTC)).total_seconds()

            if remaining > 0:
                await asyncio.sleep(remaining)

        job = await resume_job(
            session=session,
            job_id=job_id,
        )

    return job


async def process_message(
    client: redis.Redis,
    message_id: str,
    fields: dict[str, str],
    *,
    recovered: bool,
) -> None:
    payload: dict[str, Any] = json.loads(fields["payload"])

    job_id = payload["job_id"]
    command_type = payload["command_type"]
    correlation_id = payload["correlation_id"]

    logger.info(
        "job_received",
        extra={
            "service": "butler-worker",
            "job_id": job_id,
            "correlation_id": correlation_id,
            "command_type": command_type,
        },
    )

    started = time.perf_counter()

    async with SessionFactory() as session:
        if recovered:
            job = await resume_when_due(session, job_id)
        else:
            job = await start_job(session, job_id)

        skipped = job.status in {
            "succeeded",
            "failed",
            "cancelled",
        }

        if recovered and not skipped:
            logger.info(
                "job_recovered",
                extra={
                    "service": "butler-worker",
                    "job_id": str(job.id),
                    "correlation_id": str(job.correlation_id),
                    "command_type": job.command_type,
                    "attempt": job.attempts,
                },
            )

        while job.status == "running":
            execution_payload = {
                **job.payload,
                "job_id": str(job.id),
                "command_type": job.command_type,
                "correlation_id": str(job.correlation_id),
            }

            logger.info(
                "job_started",
                extra={
                    "service": "butler-worker",
                    "job_id": str(job.id),
                    "correlation_id": str(job.correlation_id),
                    "command_type": job.command_type,
                    "attempt": job.attempts,
                },
            )

            try:
                result = await dispatch(execution_payload)

            except Exception as exc:
                error_class, error_code = classify_job_error(exc)

                if error_class == "permanent" or job.attempts >= settings.job_max_attempts:
                    await fail_job(
                        session=session,
                        job=job,
                        error_code=error_code,
                        error_detail=type(exc).__name__,
                    )

                    logger.error(
                        "job_failed",
                        extra={
                            "service": "butler-worker",
                            "job_id": str(job.id),
                            "correlation_id": str(job.correlation_id),
                            "command_type": job.command_type,
                            "attempt": job.attempts,
                            "error_code": error_code,
                        },
                    )
                    break

                delay = calculate_backoff(
                    attempt=job.attempts - 1,
                    base_delay=settings.job_retry_base_seconds,
                    max_delay=settings.job_retry_max_seconds,
                )

                await schedule_retry(
                    session=session,
                    job=job,
                    delay_seconds=delay,
                    error_code=error_code,
                )

                logger.warning(
                    "job_retry_scheduled",
                    extra={
                        "service": "butler-worker",
                        "job_id": str(job.id),
                        "correlation_id": str(job.correlation_id),
                        "command_type": job.command_type,
                        "attempt": job.attempts,
                        "error_code": error_code,
                        "error_class": error_class,
                        "delay_seconds": round(delay, 2),
                    },
                )

                await asyncio.sleep(delay)

                job = await resume_when_due(
                    session=session,
                    job_id=job_id,
                )

                if job.status in {
                    "succeeded",
                    "failed",
                    "cancelled",
                }:
                    break

                continue

            await complete_job(
                session=session,
                job=job,
                result=result,
            )

            duration_ms = round((time.perf_counter() - started) * 1000)

            logger.info(
                "job_succeeded",
                extra={
                    "service": "butler-worker",
                    "job_id": str(job.id),
                    "correlation_id": str(job.correlation_id),
                    "command_type": job.command_type,
                    "attempt": job.attempts,
                    "duration_ms": duration_ms,
                },
            )
            break

        if skipped:
            logger.info(
                "job_skipped_terminal",
                extra={
                    "service": "butler-worker",
                    "job_id": job_id,
                    "correlation_id": correlation_id,
                    "command_type": command_type,
                    "status": job.status,
                },
            )

    await client.xack(
        settings.redis_stream,
        settings.redis_consumer_group,
        message_id,
    )

    logger.info(
        "job_acknowledged",
        extra={
            "service": "butler-worker",
            "job_id": job_id,
            "correlation_id": correlation_id,
            "command_type": command_type,
        },
    )


async def run_outbox_publisher(
    publisher: OutboxPublisher,
) -> None:
    while True:
        try:
            published_count = await publisher.publish_batch()

            if published_count:
                logger.info(
                    "outbox_batch_published",
                    extra={
                        "service": "butler-worker",
                        "published_count": published_count,
                    },
                )

        except asyncio.CancelledError:
            raise

        except Exception:
            logger.exception(
                "outbox_publish_failed",
                extra={"service": "butler-worker"},
            )

        await asyncio.sleep(1)


async def main() -> None:
    configure_logging(settings.log_level)

    client = redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    consumer_name = "worker-1"

    # Create the stream and consumer group on first startup.
    try:
        await client.xgroup_create(
            name=settings.redis_stream,
            groupname=settings.redis_consumer_group,
            id="0",
            mkstream=True,
        )
        logger.info("redis_consumer_group_created")
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


    publisher = OutboxPublisher(
        session_factory=SessionFactory,
        client=client,
        stream=settings.redis_stream,
        publisher_id=f"{consumer_name}-outbox",
    )

    publisher_task = asyncio.create_task(
        run_outbox_publisher(publisher),
        name="outbox-publisher",
    )

    logger.info(
        "worker_started",
        extra={
            "service": "butler-worker",
        },
    )

    try:
        while True:
            recovered_messages = await reclaim_pending(
                client,
                settings.redis_stream,
                settings.redis_consumer_group,
                consumer_name,
            )

            for message_id, fields in recovered_messages:
                await process_message(
                    client,
                    message_id,
                    fields,
                    recovered=True,
                )

            messages = await client.xreadgroup(
                groupname=settings.redis_consumer_group,
                consumername=consumer_name,
                streams={settings.redis_stream: ">"},
                count=1,
                block=5000,
            )

            if not messages:
                continue

            for _, entries in messages:
                for message_id, fields in entries:
                    await process_message(
                        client,
                        message_id,
                        fields,
                        recovered=False,
                    )
    finally:
        publisher_task.cancel()

        try:
            await publisher_task
        except asyncio.CancelledError:
            pass

        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
