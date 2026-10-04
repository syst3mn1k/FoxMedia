import json
from typing import Any

import redis.asyncio as redis


async def enqueue_job(
    client: redis.Redis,
    stream: str,
    payload: dict[str, Any],
) -> str:
    return await client.xadd(
        stream,
        {
            "payload": json.dumps(
                payload,
                separators=(",", ":"),
            )
        },
        maxlen=10_000,
        approximate=True,
    )


async def reclaim_pending(
    client: redis.Redis,
    stream: str,
    group: str,
    consumer: str,
    min_idle_ms: int = 30_000,
    count: int = 10,
) -> list[tuple[str, dict[str, str]]]:
    result = await client.xautoclaim(
        name=stream,
        groupname=group,
        consumername=consumer,
        min_idle_time=min_idle_ms,
        start_id="0-0",
        count=count,
    )

    messages = result[1]

    return messages
