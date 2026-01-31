"""Orchestrator service for workflow execution."""

import logging
from services.orchestrator.infra.broker import create_celery_app
from services.orchestrator.infra.redis_store import RedisStore
from services.orchestrator.engine.orchestrator import OrchestratorEngine
from shared.logging_config import setup_logging, set_correlation_id

setup_logging("orchestrator")

celery_app = create_celery_app()
orchestrator = OrchestratorEngine(RedisStore().get_client(), celery_app)


@celery_app.task(name="orchestrator.initialize_execution", bind=True)
def initialize_execution(self, execution_id: str, correlation_id: str = ""):
    if correlation_id:
        set_correlation_id(correlation_id)
    
    logging.info(f"Initializing workflow execution", extra={"execution_id": execution_id})
    orchestrator.initialize_execution(execution_id)


@celery_app.task(name="orchestrator.on_node_success", bind=True)
def on_node_success(self, result: dict, execution_id: str, node_id: str, correlation_id: str = ""):
    if correlation_id:
        set_correlation_id(correlation_id)
    
    logging.info(f"Node completed successfully", extra={"execution_id": execution_id, "node_id": node_id})
    orchestrator.on_node_success(execution_id, node_id, result or {})


@celery_app.task(name="orchestrator.on_node_failure", bind=True)
def on_node_failure(self, execution_id: str, node_id: str, error: str = None, correlation_id: str = ""):
    if correlation_id:
        set_correlation_id(correlation_id)
    
    logging.error(f"Node failed", extra={"execution_id": execution_id, "node_id": node_id, "error": error})
    orchestrator.on_node_failure(execution_id, node_id, error or "Unknown error")


@celery_app.task(name="orchestrator.retry_failed_workflow", bind=True)
def retry_failed_workflow(self, execution_id: str, correlation_id: str = ""):
    if correlation_id:
        set_correlation_id(correlation_id)
    
    logging.info(f"Retrying failed workflow", extra={"execution_id": execution_id})
    orchestrator.retry_failed_workflow(execution_id)


if __name__ == "__main__":
    celery_app.worker_main([
        "worker",
        "--loglevel=info",
        "-Q", "orchestrator",
        "--concurrency=4"
    ])
