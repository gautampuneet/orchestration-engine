"""Shared types for API, Orchestrator, and Worker services."""

from enum import Enum
from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field, field_validator
from shared.constants import (
    DEFAULT_NODE_TIMEOUT_SECONDS,
    MIN_NODE_TIMEOUT_SECONDS,
    MAX_NODE_TIMEOUT_SECONDS
)


class NodeState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class WorkflowStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class NodeConfig(BaseModel):
    id: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    dependencies: List[str] = Field(default_factory=list)
    timeout_seconds: Optional[int] = Field(default=DEFAULT_NODE_TIMEOUT_SECONDS)

    @field_validator('timeout_seconds')
    @classmethod
    def validate_timeout(cls, v: Optional[int]) -> int:
        if v is None:
            return DEFAULT_NODE_TIMEOUT_SECONDS
        if v < MIN_NODE_TIMEOUT_SECONDS or v > MAX_NODE_TIMEOUT_SECONDS:
            raise ValueError(
                f"timeout_seconds must be between {MIN_NODE_TIMEOUT_SECONDS} and {MAX_NODE_TIMEOUT_SECONDS}"
            )
        return v


class WorkflowDAG(BaseModel):
    nodes: List[NodeConfig]


class WorkflowStatusResponse(BaseModel):
    execution_id: str
    status: WorkflowStatus
    total_nodes: int
    completed_nodes: int
    node_states: Dict[str, str]
    error: Optional[str] = None
    failed_node: Optional[str] = None


class WorkflowResultsResponse(BaseModel):
    execution_id: str
    status: WorkflowStatus
    outputs: Dict[str, Dict[str, Any]]
