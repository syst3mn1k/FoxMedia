
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Text,
    desc,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, synonym

from app.db.base import Base


class JobEvent(Base):
    __tablename__ = "job_events"

    __table_args__ = (
        Index(
            "ix_job_events_job_created",
            "job_id",
            desc("created_at"),
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    job_id: Mapped[UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
    )

    event_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    old_status: Mapped[str | None] = mapped_column(Text)
    new_status: Mapped[str | None] = mapped_column(Text)

    actor_type: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="system",
    )

    actor_id: Mapped[str | None] = mapped_column(Text)

    payload: Mapped[dict] = mapped_column(
        JSONB,
        nullable=False,
        server_default=text("'{}'::jsonb"),
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    # Compatibility with existing service code.
    details = synonym("payload")


# Temporary compatibility alias for existing imports.
AuditEvent = JobEvent
