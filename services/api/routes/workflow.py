"""Workflow API routes."""

from fastapi import APIRouter, HTTPException, status
from services.api.domain.models import (
    CreateWorkflowRequest,
    CreateWorkflowResponse,
    TriggerWorkflowRequest,
    TriggerWorkflowResponse
)
from services.api.domain.validation import validate_dag, DAGValidationError
from services.api.infra.redis_store import RedisStore
from services.api.infra.broker import BrokerClient
from shared.types import WorkflowStatusResponse, WorkflowResultsResponse, WorkflowStatus
import uuid


router = APIRouter()
redis_store = RedisStore()
broker = BrokerClient()


def apply_node_overrides(dag: dict, node_params: dict) -> dict:
    """Merge node parameter overrides into the DAG config"""
    nodes = dag.get("nodes", [])
    node_ids = {node["id"] for node in nodes}
    
    for node_id in node_params.keys():
        if node_id not in node_ids:
            raise ValueError(f"Cannot override non-existent node: {node_id}")
    
    updated_nodes = []
    for node in nodes:
        if node["id"] in node_params:
            updated_node = node.copy()
            config = updated_node.get("config", {}).copy()
            config.update(node_params[node["id"]])
            updated_node["config"] = config
            updated_nodes.append(updated_node)
        else:
            updated_nodes.append(node)
    
    return {"nodes": updated_nodes}


@router.post("/workflow", response_model=CreateWorkflowResponse, status_code=status.HTTP_201_CREATED)
async def create_workflow(request: CreateWorkflowRequest):
    try:
        validate_dag(request.nodes)
        
        execution_id = str(uuid.uuid4())
        redis_store.store_dag(execution_id, {"nodes": request.nodes})
        
        return CreateWorkflowResponse(execution_id=execution_id, name=request.name)
    except DAGValidationError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))


@router.post("/workflow/trigger/{execution_id}", response_model=TriggerWorkflowResponse)
async def trigger_workflow(execution_id: str, request: TriggerWorkflowRequest = None):
    dag = redis_store.get_dag(execution_id)
    if not dag:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                          detail=f"Workflow {execution_id} not found")
    
    meta = redis_store.get_workflow_meta(execution_id)
    
    # Handle re-triggering based on current workflow state
    if meta:
        workflow_status = meta.get("status")
        
        if workflow_status == WorkflowStatus.RUNNING.value:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Workflow is already running. Cannot trigger again."
            )
        
        if workflow_status == WorkflowStatus.COMPLETED.value:
            node_states = redis_store.get_node_states(execution_id)
            completed_count = sum(1 for state in node_states.values() if state == "COMPLETED")
            
            return TriggerWorkflowResponse(
                execution_id=execution_id,
                status="ALREADY_COMPLETED",
                message=f"Workflow already completed successfully. All {completed_count} node(s) completed. Use '/workflows/{execution_id}/results' to fetch results."
            )
        
        if workflow_status == WorkflowStatus.FAILED.value:
            node_states = redis_store.get_node_states(execution_id)
            failed_count = sum(1 for state in node_states.values() if state == "FAILED")
            completed_count = sum(1 for state in node_states.values() if state == "COMPLETED")
            
            broker.retry_failed_workflow(execution_id)
            
            return TriggerWorkflowResponse(
                execution_id=execution_id,
                status="RETRYING",
                message=f"Retrying {failed_count} failed node(s). {completed_count} node(s) already completed and will be skipped."
            )
    
    # Apply config overrides if provided
    if request and request.node_params:
        try:
            dag = apply_node_overrides(dag, request.node_params)
            validate_dag(dag.get("nodes", []))
            redis_store.store_dag(execution_id, dag)
        except ValueError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
        except DAGValidationError as e:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, 
                              detail=f"Invalid DAG after applying overrides: {str(e)}")
    
    broker.trigger_orchestrator(execution_id)
    return TriggerWorkflowResponse(execution_id=execution_id, status="TRIGGERED", 
                                   message="Workflow execution initiated")


@router.get("/workflows/{execution_id}", response_model=WorkflowStatusResponse)
async def get_workflow_status(execution_id: str):
    meta = redis_store.get_workflow_meta(execution_id)
    
    # If meta doesn't exist, check if DAG exists
    if not meta:
        dag = redis_store.get_dag(execution_id)
        if not dag:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                              detail=f"Workflow {execution_id} not found")
        
        # DAG exists but not yet initialized - return PENDING status
        return WorkflowStatusResponse(
            execution_id=execution_id,
            status=WorkflowStatus.PENDING,
            total_nodes=len(dag.get("nodes", [])),
            completed_nodes=0,
            node_states={},
            error=None,
            failed_node=None
        )
    
    return WorkflowStatusResponse(
        execution_id=execution_id,
        status=WorkflowStatus(meta["status"]),
        total_nodes=meta["total_nodes"],
        completed_nodes=meta["completed_nodes"],
        node_states=redis_store.get_node_states(execution_id),
        error=meta.get("error"),
        failed_node=meta.get("failed_node")
    )


@router.get("/workflows/{execution_id}/results", response_model=WorkflowResultsResponse)
async def get_workflow_results(execution_id: str):
    meta = redis_store.get_workflow_meta(execution_id)
    if not meta:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, 
                          detail=f"Workflow {execution_id} not found")
    
    return WorkflowResultsResponse(
        execution_id=execution_id,
        status=WorkflowStatus(meta["status"]),
        outputs=redis_store.get_outputs(execution_id)
    )
