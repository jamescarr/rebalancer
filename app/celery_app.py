from celery import Celery

from app.config import get_settings

settings = get_settings()

app = Celery(
    "rebalancer",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=["app.tasks"],
)

app.conf.timezone = "UTC"
app.conf.beat_schedule_filename = "tmp/celerybeat-schedule"

app.conf.beat_schedule = {
    "evaluate-strategies": {
        "task": "app.tasks.evaluate_due_strategies",
        "schedule": 10.0,
    },
    "stuck-job-watchdog": {
        "task": "app.tasks.find_and_alert_stuck_jobs",
        "schedule": 900.0,
    },
}
