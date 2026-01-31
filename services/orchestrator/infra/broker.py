"""
Celery broker configuration for Orchestrator service.
"""

from celery import Celery
import os


def create_celery_app() -> Celery:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    
    app = Celery(
        "orchestrator",
        broker=redis_url,
        backend=redis_url
    )
    
    app.conf.update(
        task_serializer="json",
        accept_content=["json"],
        result_serializer="json",
        timezone="UTC",
        enable_utc=True,
        task_track_started=True,
        task_acks_late=True,
        worker_prefetch_multiplier=1,
        task_reject_on_worker_lost=True,
        task_routes={
            "orchestrator.*": {"queue": "orchestrator"},
            "worker.*": {"queue": "worker"}
        }
    )
    
    return app
