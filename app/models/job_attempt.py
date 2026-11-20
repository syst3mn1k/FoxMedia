import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Identity,
    Index,
    Integer,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class JobAttempt(Base):
    __tablename__ = "job_attempts"

    __table_args__ = (
        UniqueConstraint(
            "job_id",
            "attempt_no",
            name="uq_job_attempts_number",
        ),
        CheckConstraint(
            "attempt_no > 0",
            name="ck_job_attempts_positive",
        ),
        CheckConstraint(
            "outcome IN ('running','succeeded','retryable_failure',"
            "'final_failure','cancelled')",
            name="ck_job_attempts_outcome",
        ),
        CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0",
            name="ck_job_attempts_duration",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        Identity(always=True),
        primary_key=True,
    )

    job_id: Mapped[uuid.UUID] = mapped_column(
        PG_UUID(as_uuid=True),
        ForeignKey("jobs.id", ondelete="RESTRICT"),
        nullable=False,
    )

    attempt_no: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
    )

    worker_id: Mapped[str | None] = mapped_column(Text)

    outcome: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        server_default="running",
    )

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )

    ended_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
    )

    duration_ms: Mapped[int | None] = mapped_column(Integer)

    error_code: Mapped[str | None] = mapped_column(Text)

    error_detail: Mapped[str | None] = mapped_column(Text)


Index(
    "ix_job_attempts_job_started",
    JobAttempt.job_id,
    JobAttempt.started_at.desc(),
)
