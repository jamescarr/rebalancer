"""Shared constants and retry policies for all workflows."""

from datetime import timedelta

from temporalio.common import RetryPolicy

BROKER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=2),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=2),
    maximum_attempts=5,
    non_retryable_error_types=["APIError"],
)

DB_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=1),
    maximum_attempts=3,
)

BROKER_TIMEOUT = timedelta(seconds=30)
DB_TIMEOUT = timedelta(seconds=10)

FILL_POLL_INTERVAL_SECONDS = 15
FILL_POLL_DEADLINE = timedelta(minutes=5)

MIN_QTY_THRESHOLD = 1e-6
