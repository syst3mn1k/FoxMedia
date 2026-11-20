
import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Identity,
    Index,
    Integer,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"

    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'publishing', 'published')",
            name="ck_outbox_status",
        ),
        CheckConstraint(
            "attempts >= 0",
            name="ck_outbox_attempts",
        ),
        Index(
            "ix_outbox_pending",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("status = 'pending'"),
        ),
        Index(
            "ix_outbox_expired_lease",
            "locked_until",
            postgresql_where=text("status = 'publishing'"),
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    aggregate_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    aggregate_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        nullable=False,
    )

    topic: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )

    status: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default=text("'pending'"),
    )

    attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        server_default=text("0"),
    )

    next_attempt_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    locked_by: Mapped[str | None] = mapped_column(Text)
    locked_until: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )

    published_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )

    last_error: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
