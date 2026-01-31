"""API request/response models."""

from pydantic import BaseModel, Field, model_validator
from typing import Optional, List, Dict, Any


class CreateWorkflowRequest(BaseModel):
    """Request body for creating a new workflow"""
    name: Optional[str] = None
    nodes: List[Dict[str, Any]]


class CreateWorkflowResponse(BaseModel):
    execution_id: str
    name: Optional[str] = None


class TriggerWorkflowRequest(BaseModel):
    """Optional node parameter overrides when triggering a workflow"""
    node_params: Optional[Dict[str, Dict[str, Any]]] = None


class TriggerWorkflowResponse(BaseModel):
    execution_id: str
    status: str
    message: str
