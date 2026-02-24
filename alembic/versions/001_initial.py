"""Initial schema: rebalance_jobs, trade_executions, user_portfolios

Revision ID: 001
Revises:
Create Date: 2024-01-01 00:00:00.000000
"""

import uuid

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "rebalance_jobs",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
        ),
        sa.Column("account_id", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("target_allocation", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("drift_threshold", sa.Float(), nullable=False, server_default="0.05"),
        sa.Column("current_task_id", sa.String(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("positions_fetched_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("trades_calculated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("execution_started_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "trade_executions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
        ),
        sa.Column(
            "job_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("rebalance_jobs.id"),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(), nullable=False),
        sa.Column("side", sa.String(), nullable=False),
        sa.Column("intended_qty", sa.Float(), nullable=False),
        sa.Column("alpaca_order_id", sa.String(), nullable=True),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("filled_qty", sa.Float(), nullable=True),
        sa.Column("filled_avg_price", sa.Float(), nullable=True),
        sa.Column("submitted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("filled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
    )

    op.create_table(
        "user_portfolios",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            primary_key=True,
            default=uuid.uuid4,
        ),
        sa.Column("account_id", sa.String(), nullable=False, unique=True),
        sa.Column("target_allocation", postgresql.JSON(astext_type=sa.Text()), nullable=False),
        sa.Column("drift_threshold", sa.Float(), nullable=False, server_default="0.05"),
        sa.Column("auto_rebalance", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("last_rebalanced_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index("ix_rebalance_jobs_account_id", "rebalance_jobs", ["account_id"])
    op.create_index("ix_rebalance_jobs_status", "rebalance_jobs", ["status"])
    op.create_index("ix_rebalance_jobs_created_at", "rebalance_jobs", ["created_at"])
    op.create_index("ix_trade_executions_job_id", "trade_executions", ["job_id"])
    op.create_index(
        "ix_trade_executions_alpaca_order_id",
        "trade_executions",
        ["alpaca_order_id"],
    )


def downgrade() -> None:
    op.drop_table("trade_executions")
    op.drop_table("rebalance_jobs")
    op.drop_table("user_portfolios")
