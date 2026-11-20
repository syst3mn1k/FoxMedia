
from typing import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "d42f8a1c6e90"
down_revision: str | Sequence[str] | None = "c91e6a8f2b40"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "outbox_messages",
        sa.Column(
            "id",
            sa.BigInteger(),
            sa.Identity(always=True),
            primary_key=True,
        ),
        sa.Column("aggregate_type", sa.Text(), nullable=False),
        sa.Column(
            "aggregate_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("topic", sa.Text(), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column(
            "status",
            sa.Text(),
            server_default=sa.text("'pending'"),
            nullable=False,
        ),
        sa.Column(
            "attempts",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("locked_by", sa.Text()),
        sa.Column("locked_until", sa.DateTime(timezone=True)),
        sa.Column("published_at", sa.DateTime(timezone=True)),
        sa.Column("last_error", sa.Text()),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "status IN ('pending', 'publishing', 'published')",
            name="ck_outbox_status",
        ),
        sa.CheckConstraint(
            "attempts >= 0",
            name="ck_outbox_attempts",
        ),
    )

    op.create_index(
        "ix_outbox_pending",
        "outbox_messages",
        ["next_attempt_at", "created_at"],
        postgresql_where=sa.text("status = 'pending'"),
    )

    op.create_index(
        "ix_outbox_expired_lease",
        "outbox_messages",
        ["locked_until"],
        postgresql_where=sa.text("status = 'publishing'"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_expired_lease",
        table_name="outbox_messages",
    )

    op.drop_index(
        "ix_outbox_pending",
        table_name="outbox_messages",
    )

    op.drop_table("outbox_messages")
