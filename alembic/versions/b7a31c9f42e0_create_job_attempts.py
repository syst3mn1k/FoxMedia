"""create job attempts table

Revision ID: b7a31c9f42e0
Revises: 6e21d1e52255
"""

from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "b7a31c9f42e0"
down_revision: str | Sequence[str] | None = "6e21d1e52255"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_attempts",
        sa.Column(
            "id",
            sa.BigInteger(),
            sa.Identity(always=True),
            nullable=False,
        ),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.Text(), nullable=True),
        sa.Column(
            "outcome",
            sa.Text(),
            server_default=sa.text("'running'"),
            nullable=False,
        ),
        sa.Column(
            "started_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("ended_at", sa.DateTime(timezone=True)),
        sa.Column("duration_ms", sa.Integer()),
        sa.Column("error_code", sa.Text()),
        sa.Column("error_detail", sa.Text()),
        sa.CheckConstraint(
            "attempt_no > 0",
            name="ck_job_attempts_positive",
        ),
        sa.CheckConstraint(
            "outcome IN ('running','succeeded','retryable_failure',"
            "'final_failure','cancelled')",
            name="ck_job_attempts_outcome",
        ),
        sa.CheckConstraint(
            "duration_ms IS NULL OR duration_ms >= 0",
            name="ck_job_attempts_duration",
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "job_id",
            "attempt_no",
            name="uq_job_attempts_number",
        ),
    )

    op.create_index(
        "ix_job_attempts_job_started",
        "job_attempts",
        ["job_id"],
        unique=False,
        postgresql_include=["started_at"],
    )

    op.execute(
        "DROP INDEX IF EXISTS ix_job_attempts_job_started"
    )
    op.execute(
        "CREATE INDEX ix_job_attempts_job_started "
        "ON job_attempts (job_id, started_at DESC)"
    )


def downgrade() -> None:
    op.drop_index(
        "ix_job_attempts_job_started",
        table_name="job_attempts",
    )
    op.drop_table("job_attempts")
