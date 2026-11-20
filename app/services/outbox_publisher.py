import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from redis.asyncio import Redis
from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models.audit import JobEvent
from app.models.outbox import OutboxMessage


@dataclass(frozen=True)
class ClaimedMessage:
    id: int
    aggregate_type: str
    aggregate_id: UUID
    topic: str
    payload: dict
    attempts: int


class OutboxPublisher:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        client: Redis,
        stream: str,
        publisher_id: str,
        batch_size: int = 20,
        lease_seconds: int = 30,
        retry_base_seconds: int = 5,
        retry_max_seconds: int = 300,
    ) -> None:
        self.session_factory = session_factory
        self.client = client
        self.stream = stream
        self.publisher_id = publisher_id
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds
        self.retry_base_seconds = retry_base_seconds
        self.retry_max_seconds = retry_max_seconds

    async def _claim(self) -> list[ClaimedMessage]:
        now = datetime.now(UTC)

        eligibility = or_(
            and_(
                OutboxMessage.status == "pending",
                OutboxMessage.next_attempt_at <= now,
            ),
            and_(
                OutboxMessage.status == "publishing",
                OutboxMessage.locked_until <= now,
            ),
        )

        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(OutboxMessage)
                    .where(eligibility)
                    .order_by(OutboxMessage.created_at)
                    .limit(self.batch_size)
                    .with_for_update(skip_locked=True)
                )

                rows = result.scalars().all()
                claimed = []

                for row in rows:
                    row.status = "publishing"
                    row.locked_by = self.publisher_id
                    row.locked_until = now + timedelta(seconds=self.lease_seconds)
                    row.attempts += 1

                    claimed.append(
                        ClaimedMessage(
                            id=row.id,
                            aggregate_type=row.aggregate_type,
                            aggregate_id=row.aggregate_id,
                            topic=row.topic,
                            payload=dict(row.payload),
                            attempts=row.attempts,
                        )
                    )

            return claimed

    def _encode_message(
        self,
        message: ClaimedMessage,
    ) -> dict[str, str]:
        payload = dict(message.payload)
        payload["outbox_id"] = str(message.id)
        payload.setdefault("schema_version", 1)

        return {
            "payload": json.dumps(
                payload,
                separators=(",", ":"),
            )
        }

    async def _mark_published(
        self,
        message: ClaimedMessage,
    ) -> None:
        now = datetime.now(UTC)

        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(OutboxMessage)
                    .where(
                        OutboxMessage.id == message.id,
                        OutboxMessage.status == "publishing",
                        OutboxMessage.locked_by == self.publisher_id,
                    )
                    .with_for_update()
                )

                row = result.scalar_one_or_none()

                if row is None:
                    return

                row.status = "published"
                row.published_at = now
                row.locked_by = None
                row.locked_until = None
                row.last_error = None

                if message.aggregate_type == "job":
                    session.add(
                        JobEvent(
                            job_id=message.aggregate_id,
                            event_type="outbox.published",
                            actor_type="system",
                            payload={"outbox_id": message.id, "stream": message.topic},
                        )
                    )

    async def _schedule_retry(
        self,
        message: ClaimedMessage,
        error: Exception,
    ) -> None:
        delay = min(
            self.retry_base_seconds * (2 ** (message.attempts - 1)),
            self.retry_max_seconds,
        )

        async with self.session_factory() as session:
            async with session.begin():
                result = await session.execute(
                    select(OutboxMessage)
                    .where(
                        OutboxMessage.id == message.id,
                        OutboxMessage.status == "publishing",
                        OutboxMessage.locked_by == self.publisher_id,
                    )
                    .with_for_update()
                )

                row = result.scalar_one_or_none()

                if row is None:
                    return

                row.status = "pending"
                row.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
                row.locked_by = None
                row.locked_until = None
                row.last_error = type(error).__name__

    async def publish_batch(self) -> int:
        messages = await self._claim()
        published = 0

        for message in messages:
            try:
                await self.client.xadd(
                    message.topic,
                    self._encode_message(message),
                    maxlen=10_000,
                    approximate=True,
                )

            except Exception as exc:
                await self._schedule_retry(message, exc)
                continue

            await self._mark_published(message)
            published += 1

        return published
