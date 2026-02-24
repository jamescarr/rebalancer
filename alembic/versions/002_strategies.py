"""Add strategies table, replace user_portfolios

Revision ID: 002
Revises: 001
Create Date: 2026-02-24 00:00:00.000000
"""

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "strategies",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("strategy_type", sa.String(), nullable=False, server_default="fixed_weight"),
        sa.Column("assets", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("config", postgresql.JSON(astext_type=sa.Text()), nullable=False, server_default="{}"),
        sa.Column("rebalance_interval_seconds", sa.Integer(), nullable=False, server_default="86400"),
        sa.Column("drift_threshold", sa.Float(), nullable=False, server_default="0.05"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("last_evaluated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_evaluation_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("current_allocation", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("last_evaluation_result", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )

    op.add_column(
        "rebalance_jobs",
        sa.Column("strategy_id", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_rebalance_jobs_strategy_id",
        "rebalance_jobs", "strategies",
        ["strategy_id"], ["id"],
    )

    op.drop_table("user_portfolios")


def downgrade() -> None:
    op.create_table(
        "user_portfolios",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4),
        sa.Column("account_id", sa.String(), nullable=False, unique=True),
        sa.Column("target_allocation", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("drift_threshold", sa.Float(), nullable=False, server_default="0.05"),
        sa.Column("auto_rebalance", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_rebalanced_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.drop_constraint("fk_rebalance_jobs_strategy_id", "rebalance_jobs", type_="foreignkey")
    op.drop_column("rebalance_jobs", "strategy_id")
    op.drop_table("strategies")
