import asyncio
import json
import logging
import time
from typing import Any

import redis.asyncio as redis

from app.core.config import settings
from app.core.logging import configure_logging
from app.db.session import SessionFactory
from app.queues.streams import reclaim_pending
from app.services.jobs import complete_job, resume_job, start_job
from app.workers.dispatcher import dispatch


logger = logging.getLogger(__name__)


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
            job = await resume_job(
                session=session,
                job_id=job_id,
            )

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
        else:
            job = await start_job(
                session=session,
                job_id=job_id,
            )

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

        result = await dispatch(execution_payload)

        await complete_job(
            session=session,
            job=job,
            result=result,
        )

        duration_ms = round(
            (time.perf_counter() - started) * 1000
        )

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


async def main() -> None:
    configure_logging(settings.log_level)

    client = redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    consumer_name = "worker-1"

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
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
