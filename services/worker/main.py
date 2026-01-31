"""Worker service for task execution."""

import logging
from celery.exceptions import SoftTimeLimitExceeded
from services.worker.infra.broker import create_celery_app
from services.worker.handlers.registry import (
    ensure_idempotency, 
    increment_run_counter, 
    get_task_handler, 
    list_task_types
)
from shared.exceptions import TaskError
from shared.logging_config import setup_logging, set_correlation_id
import services.worker.handlers.external
import services.worker.handlers.llm

setup_logging("worker")

celery_app = create_celery_app()


def create_worker_task(task_type: str):
    @celery_app.task(name=f"worker.{task_type}", bind=True)
    def worker_task(self, execution_id: str, node_id: str, config: dict, correlation_id: str = ""):
        if correlation_id:
            set_correlation_id(correlation_id)
        
        logging.info(f"Starting task execution", extra={
            "execution_id": execution_id,
            "node_id": node_id,
            "task_type": task_type
        })
        
        if not ensure_idempotency(execution_id, node_id):
            logging.warning(f"Duplicate task execution detected", extra={
                "execution_id": execution_id,
                "node_id": node_id
            })
            return {"outputs": {}}
        
        increment_run_counter(execution_id, node_id)
        
        try:
            result = get_task_handler(task_type)(execution_id, node_id, config)
            logging.info(f"Task execution completed", extra={
                "execution_id": execution_id,
                "node_id": node_id,
                "task_type": task_type
            })
            return result
        except SoftTimeLimitExceeded:
            logging.error(f"Task timed out", extra={
                "execution_id": execution_id,
                "node_id": node_id,
                "task_type": task_type
            })
            error = TaskError(
                error_type="TIMEOUT",
                error_message=f"Task {task_type} exceeded time limit for node {node_id}",
                is_retryable=False,
                context={"execution_id": execution_id, "node_id": node_id, "task_type": task_type}
            )
            raise Exception(error.error_message)
    
    return worker_task


for task_type in list_task_types():
    create_worker_task(task_type)


if __name__ == "__main__":
    celery_app.worker_main([
        "worker",
        "--loglevel=info",
        "-Q", "worker",
        "--concurrency=8"
    ])
