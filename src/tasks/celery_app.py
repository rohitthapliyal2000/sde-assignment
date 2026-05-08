from celery import Celery

from src.config import settings

celery_app = Celery(
    "voicebot",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_default_queue=settings.POSTCALL_CELERY_QUEUE,
    beat_schedule={
        "drain-due-workflow-jobs": {
            "task": "drain_due_workflow_jobs_task",
            "schedule": 15.0,
        },
        "recover-stale-workflow-jobs": {
            "task": "recover_stale_workflow_jobs_task",
            "schedule": 60.0,
        },
    },
)
