
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "c91e6a8f2b40"
down_revision: str | None = "b7a31c9f42e0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "job_events",
        sa.Column(
            "id",
            sa.BigInteger(),
            sa.Identity(always=True),
            primary_key=True,
        ),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.Text(), nullable=False),
        sa.Column("old_status", sa.Text()),
        sa.Column("new_status", sa.Text()),
        sa.Column(
            "actor_type",
            sa.Text(),
            nullable=False,
            server_default="system",
        ),
        sa.Column("actor_id", sa.Text()),
        sa.Column(
            "payload",
            postgresql.JSONB(),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
            ondelete="RESTRICT",
        ),
    )

    op.create_index(
        "ix_job_events_job_created",
        "job_events",
        ["job_id", sa.text("created_at DESC")],
    )

    # Preserve existing audit history.
    op.execute("""
        INSERT INTO job_events (
            job_id,
            event_type,
            actor_type,
            payload,
            created_at
        )
        SELECT
            job_id,
            event_type,
            'system',
            COALESCE(details, '{}'::jsonb),
            created_at
        FROM audit_events
        ORDER BY created_at, id
    """)


def downgrade() -> None:
    op.drop_index(
        "ix_job_events_job_created",
        table_name="job_events",
    )
    op.drop_table("job_events")
