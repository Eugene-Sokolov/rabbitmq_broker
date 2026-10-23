"""add outbox published index

Revision ID: c13cfee05935
Revises: 3e54064e3c96
Create Date: 2026-10-23 02:47:49.618857

"""

from collections.abc import Sequence

from alembic import op


revision: str = "c13cfee05935"
down_revision: str | Sequence[str] | None = "3e54064e3c96"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index(
        "ix_outbox_published",
        "outbox",
        ["published_at"],
        postgresql_where="published_at IS NOT NULL",
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_outbox_published", table_name="outbox")
