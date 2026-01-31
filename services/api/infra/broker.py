"""
Message broker client for API service.
"""

from celery import Celery
import os
from shared.logging_config import get_correlation_id


class BrokerClient:
    """Celery client for API service"""
    
    def __init__(self, broker_url: str = None):
        url = broker_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        
        self.app = Celery(
            "api",
            broker=url,
            backend=url
        )
        
        self.app.conf.update(
            task_serializer="json",
            accept_content=["json"],
            result_serializer="json",
            timezone="UTC",
            enable_utc=True,
        )
    
    def trigger_orchestrator(self, execution_id: str) -> None:
        """Send task to orchestrator to kick off workflow execution"""
        correlation_id = get_correlation_id()
        self.app.send_task(
            "orchestrator.initialize_execution",
            kwargs={"execution_id": execution_id, "correlation_id": correlation_id},
            queue="orchestrator"
        )
    
    def retry_failed_workflow(self, execution_id: str) -> None:
        """Send task to orchestrator to retry a failed workflow"""
        correlation_id = get_correlation_id()
        self.app.send_task(
            "orchestrator.retry_failed_workflow",
            kwargs={"execution_id": execution_id, "correlation_id": correlation_id},
            queue="orchestrator"
        )
