import asyncio

import redis.asyncio as redis

from app.core.config import settings
from app.workers.handlers import handle_system_echo


async def main() -> None:
    client = redis.from_url(
        settings.redis_url,
        decode_responses=True,
    )

    consumer_name = "worker-1"

    print(
        f"Worker started: "
        f"stream={settings.redis_stream}, "
        f"group={settings.redis_consumer_group}, "
        f"consumer={consumer_name}"
    )

    while True:
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
                print(f"Received job: {message_id}")

                command_type = fields.get("command_type")

                if command_type == "system.echo":
                    payload = {
                        "job_id": fields.get("job_id"),
                        "correlation_id": fields.get("correlation_id"),
                    }

                    result = await handle_system_echo(payload)

                    print(f"Result: {result}")

                    await client.xack(
                        settings.redis_stream,
                        settings.redis_consumer_group,
                        message_id,
                    )

                    print(f"ACK: {message_id}")

                else:
                    print(f"Unknown command_type: {command_type}")


if __name__ == "__main__":
    asyncio.run(main())
