"""run cancel reason, tracing session, and priority

Revision ID: 002_run_meta
Revises: 001_initial
Create Date: 2026-11-16 07:40:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "002_run_meta"
down_revision: str | Sequence[str] | None = "001_initial"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("runs", sa.Column("cancel_reason", sa.Text(), nullable=True))
    op.add_column("runs", sa.Column("langsmith_session_name", sa.String(length=512), nullable=True))
    op.add_column(
        "runs",
        sa.Column("priority", sa.Integer(), server_default=sa.text("0"), nullable=False),
    )


def downgrade() -> None:
    op.drop_column("runs", "priority")
    op.drop_column("runs", "langsmith_session_name")
    op.drop_column("runs", "cancel_reason")
