"""Structured exception hierarchy for the orchestration engine."""

from typing import Optional, Dict, Any
from pydantic import BaseModel


class TaskError(BaseModel):
    """Structured error returned by worker tasks"""
    error_type: str
    error_message: str
    http_status_code: Optional[int] = None
    retry_after_seconds: Optional[int] = None
    is_retryable: bool = False
    context: Dict[str, Any] = {}

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(exclude_none=True)


class WorkflowError(Exception):
    """Base exception for workflow errors"""
    
    def __init__(self, message: str, execution_id: str = "", **context):
        self.message = message
        self.execution_id = execution_id
        self.context = context
        super().__init__(message)


class NodeExecutionError(WorkflowError):
    pass


class TemplateResolutionError(WorkflowError):
    pass


class TimeoutError(NodeExecutionError):
    pass


class RetryExhaustedError(NodeExecutionError):
    pass


class ValidationError(WorkflowError):
    pass


class CycleDetectedError(WorkflowError):
    pass
