"""Add funded_amount and holdings to strategies

Revision ID: 003
Revises: 002
Create Date: 2026-02-24 00:00:00.000000
"""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "003"
down_revision = "002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("strategies", sa.Column("funded_amount", sa.Float(), nullable=True))
    op.add_column("strategies", sa.Column("holdings", postgresql.JSON(astext_type=sa.Text()), nullable=True, server_default="{}"))
    op.execute("UPDATE strategies SET is_active = false")


def downgrade() -> None:
    op.drop_column("strategies", "holdings")
    op.drop_column("strategies", "funded_amount")
