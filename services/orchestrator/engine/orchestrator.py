"""Orchestrator engine for workflow execution."""

import json
import time
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from services.orchestrator.engine.template import TemplateResolver
from services.orchestrator.engine.retry_handler import RetryHandler
from shared.types import NodeState, WorkflowStatus
from shared.utils import generate_task_id
from shared.logging_config import get_correlation_id


@dataclass
class NodeConfig:
    node_id: str
    task_type: str
    config: Dict[str, Any]
    dependencies: List[str]
    timeout_seconds: int = 300


@dataclass
class WorkflowDAG:
    execution_id: str
    nodes: Dict[str, NodeConfig]
    adjacency: Dict[str, List[str]]
    deps_count: Dict[str, int]


class OrchestratorEngine:
    
    def __init__(self, redis_client, celery_app):
        self.redis = redis_client
        self.celery = celery_app
        self.template_resolver = TemplateResolver(redis_client)
        self.retry_handler = RetryHandler(redis_client)
    
    def initialize_execution(self, execution_id: str) -> None:
        dag_data = self.redis.get(f"exec:{execution_id}:dag")
        
        if not dag_data:
            raise ValueError(f"DAG not found for execution {execution_id}")
        
        workflow = self._parse_dag(execution_id, json.loads(dag_data))
        
        meta = {
            "execution_id": execution_id,
            "status": WorkflowStatus.RUNNING.value,
            "total_nodes": len(workflow.nodes),
            "completed_nodes": 0,
        }
        self.redis.set(f"exec:{execution_id}:meta", json.dumps(meta))
        
        pipe = self.redis.pipeline()
        for node_id in workflow.nodes:
            pipe.hset(f"exec:{execution_id}:node_state", node_id, NodeState.PENDING.value)
        pipe.execute()
        
        pipe = self.redis.pipeline()
        for node_id, count in workflow.deps_count.items():
            pipe.hset(f"exec:{execution_id}:deps_remaining", node_id, count)
        pipe.execute()
        
        pipe = self.redis.pipeline()
        for parent_id, children in workflow.adjacency.items():
            if children:
                pipe.sadd(f"exec:{execution_id}:children:{parent_id}", *children)
        pipe.execute()
        
        pipe = self.redis.pipeline()
        for node_id, node_config in workflow.nodes.items():
            config_data = {
                "task_type": node_config.task_type,
                "config": node_config.config,
                "dependencies": node_config.dependencies,
                "timeout_seconds": node_config.timeout_seconds
            }
            pipe.hset(f"exec:{execution_id}:node_configs", node_id, json.dumps(config_data))
        pipe.execute()
        
        root_nodes = [nid for nid, cnt in workflow.deps_count.items() if cnt == 0]
        for node_id in root_nodes:
            self._dispatch_node(workflow, node_id)
    
    def _parse_dag(self, execution_id: str, dag: Dict[str, Any]) -> WorkflowDAG:
        nodes, adjacency, deps_count = {}, {}, {}
        
        for node_data in dag.get("nodes", []):
            node_id = node_data["id"]
            dependencies = node_data.get("dependencies", [])
            timeout = node_data.get("timeout_seconds", 300)
            
            node_config = NodeConfig(
                node_id=node_id,
                task_type=node_data["handler"],
                config=node_data.get("config", {}),
                dependencies=dependencies,
                timeout_seconds=timeout
            )
            nodes[node_id] = node_config
            deps_count[node_id] = len(dependencies)
            adjacency[node_id] = []
        
        for node_id, node_config in nodes.items():
            for parent_id in node_config.dependencies:
                if parent_id in adjacency:
                    adjacency[parent_id].append(node_id)
        
        return WorkflowDAG(execution_id, nodes, adjacency, deps_count)
    
    def _dispatch_node(self, workflow: WorkflowDAG, node_id: str) -> None:
        execution_id = workflow.execution_id
        
        # Don't dispatch new nodes if workflow already failed
        meta_str = self.redis.get(f"exec:{execution_id}:meta")
        if meta_str:
            meta = json.loads(meta_str)
            if meta.get("status") == WorkflowStatus.FAILED.value:
                return
        
        # Prevent duplicate dispatch with Redis set
        if self.redis.sadd(f"exec:{execution_id}:dispatched", node_id) == 0:
            return
        
        self.redis.hset(f"exec:{execution_id}:node_state", node_id, NodeState.RUNNING.value)
        
        node_config = workflow.nodes[node_id]
        
        try:
            resolved_config = self.template_resolver.resolve(execution_id, node_config.config)
        except Exception as e:
            self.on_node_failure(execution_id, node_id, str(e))
            return
        
        timeout = node_config.timeout_seconds
        soft_timeout = max(timeout - 30, timeout // 2) if timeout > 30 else timeout
        correlation_id = get_correlation_id()
        
        logging.info(f"Dispatching node", extra={
            "execution_id": execution_id,
            "node_id": node_id,
            "task_type": node_config.task_type,
            "timeout": timeout
        })
        
        self.celery.send_task(
            f"worker.{node_config.task_type}",
            kwargs={"execution_id": execution_id, "node_id": node_id, "config": resolved_config, "correlation_id": correlation_id},
            task_id=generate_task_id(execution_id, node_id),
            queue="worker",
            time_limit=timeout,
            soft_time_limit=soft_timeout,
            link=self.celery.signature('orchestrator.on_node_success', kwargs={"execution_id": execution_id, "node_id": node_id, "correlation_id": correlation_id},
                                      queue="orchestrator"),
            link_error=self.celery.signature('orchestrator.on_node_failure', args=[execution_id, node_id],
                                            kwargs={"correlation_id": correlation_id},
                                            immutable=True, queue="orchestrator")
        )
    
    def on_node_success(self, execution_id: str, node_id: str, result: Dict[str, Any]) -> None:
        # Don't process success if workflow already failed
        meta_str = self.redis.get(f"exec:{execution_id}:meta")
        if meta_str:
            meta = json.loads(meta_str)
            if meta.get("status") == WorkflowStatus.FAILED.value:
                return
        
        # Deduplicate callbacks - prevents double-processing from Celery retries or broker redelivery
        completion_key = f"exec:{execution_id}:completion_processed:{node_id}"
        if not self.redis.setnx(completion_key, "1"):
            return
        
        self.redis.expire(completion_key, 604800)  # 7 days
        self.redis.hset(f"exec:{execution_id}:node_state", node_id, NodeState.COMPLETED.value)
        
        outputs = result.get("outputs", {})
        if outputs:
            pipe = self.redis.pipeline()
            for key, value in outputs.items():
                pipe.hset(f"exec:{execution_id}:outputs", f"{node_id}.{key}", json.dumps(value))
            pipe.execute()
        
        children = self.redis.smembers(f"exec:{execution_id}:children:{node_id}")
        
        if not children:
            self._check_workflow_completion(execution_id)
            return
        
        # Atomic counter decrement - dispatch child only when all its dependencies are done
        for child_id in children:
            child_id_str = child_id.decode('utf-8')
            remaining = self.redis.hincrby(f"exec:{execution_id}:deps_remaining", child_id_str, -1)
            if remaining == 0:
                self._dispatch_child_node(execution_id, child_id_str)
        
        self._check_workflow_completion(execution_id)
    
    def on_node_failure(self, execution_id: str, node_id: str, error: str) -> None:
        """Handles failed nodes and decides whether to retry them"""
        should_retry, delay = self.retry_handler.should_retry_node(execution_id, node_id, error)
        
        if should_retry and delay is not None:
            self._retry_node(execution_id, node_id, delay)
            return
        
        self._mark_node_failed(execution_id, node_id, error)
    
    def _mark_node_failed(self, execution_id: str, node_id: str, error: str) -> None:
        """Marks node as failed and propagates failure to downstream nodes"""
        self.redis.hset(f"exec:{execution_id}:node_state", node_id, NodeState.FAILED.value)
        self._cascade_failure_to_descendants(execution_id, node_id)
        
        meta_str = self.redis.get(f"exec:{execution_id}:meta")
        if meta_str:
            meta = json.loads(meta_str)
            meta.update({"status": WorkflowStatus.FAILED.value, "error": error, "failed_node": node_id})
            self.redis.set(f"exec:{execution_id}:meta", json.dumps(meta))
    
    def _cascade_failure_to_descendants(self, execution_id: str, failed_node_id: str) -> None:
        """BFS to mark all downstream nodes as failed"""
        queue = [failed_node_id]
        visited = set()
        
        while queue:
            current_node = queue.pop(0)
            if current_node in visited:
                continue
            visited.add(current_node)
            
            children = self.redis.smembers(f"exec:{execution_id}:children:{current_node}")
            for child in children:
                child_id = child.decode('utf-8')
                current_state = self.redis.hget(f"exec:{execution_id}:node_state", child_id)
                if current_state and current_state.decode('utf-8') == NodeState.PENDING.value:
                    self.redis.hset(f"exec:{execution_id}:node_state", child_id, NodeState.FAILED.value)
                    queue.append(child_id)
    
    def _retry_node(self, execution_id: str, node_id: str, delay: float) -> None:
        """Schedules a retry for a failed node"""
        node_config_str = self.redis.hget(f"exec:{execution_id}:node_configs", node_id)
        if not node_config_str:
            return
        
        data = json.loads(node_config_str)
        node_config = NodeConfig(
            node_id, 
            data["task_type"], 
            data["config"], 
            data["dependencies"],
            data.get("timeout_seconds", 300)
        )
        
        self.redis.hset(f"exec:{execution_id}:node_state", node_id, NodeState.RUNNING.value)
        
        try:
            resolved_config = self.template_resolver.resolve(execution_id, node_config.config)
        except Exception as e:
            self._mark_node_failed(execution_id, node_id, str(e))
            return
        
        timeout = node_config.timeout_seconds
        soft_timeout = max(timeout - 30, timeout // 2) if timeout > 30 else timeout
        correlation_id = get_correlation_id()
        
        logging.info(f"Retrying node after {delay}s delay", extra={
            "execution_id": execution_id,
            "node_id": node_id,
            "delay": delay
        })
        
        self.celery.send_task(
            f"worker.{node_config.task_type}",
            kwargs={"execution_id": execution_id, "node_id": node_id, "config": resolved_config, "correlation_id": correlation_id},
            task_id=generate_task_id(execution_id, node_id),
            queue="worker",
            countdown=delay,
            time_limit=timeout,
            soft_time_limit=soft_timeout,
            link=self.celery.signature('orchestrator.on_node_success', kwargs={"execution_id": execution_id, "node_id": node_id, "correlation_id": correlation_id},
                                      queue="orchestrator"),
            link_error=self.celery.signature('orchestrator.on_node_failure', args=[execution_id, node_id],
                                            kwargs={"correlation_id": correlation_id},
                                            immutable=True, queue="orchestrator")
        )
    
    def _dispatch_child_node(self, execution_id: str, node_id: str) -> None:
        """Kicks off a child node once its dependencies are done"""
        node_config_str = self.redis.hget(
            f"exec:{execution_id}:node_configs",
            node_id
        )
        if not node_config_str:
            return
        
        data = json.loads(node_config_str)
        node_config = NodeConfig(
            node_id, 
            data["task_type"], 
            data["config"], 
            data["dependencies"],
            data.get("timeout_seconds", 300)
        )
        workflow = WorkflowDAG(execution_id, {node_id: node_config}, {}, {})
        self._dispatch_node(workflow, node_id)
    
    def _check_workflow_completion(self, execution_id: str) -> None:
        node_states = self.redis.hgetall(f"exec:{execution_id}:node_state")
        if not node_states:
            return
        
        total = len(node_states)
        completed = sum(1 for s in node_states.values() if s.decode('utf-8') == NodeState.COMPLETED.value)
        failed = sum(1 for s in node_states.values() if s.decode('utf-8') == NodeState.FAILED.value)
        
        meta_str = self.redis.get(f"exec:{execution_id}:meta")
        if not meta_str:
            return
        
        meta = json.loads(meta_str)
        
        if meta.get("status") == WorkflowStatus.FAILED.value:
            return
        
        meta["completed_nodes"] = completed
        
        if failed > 0:
            meta["status"] = WorkflowStatus.FAILED.value
        elif completed == total:
            meta["status"] = WorkflowStatus.COMPLETED.value
        
        self.redis.set(f"exec:{execution_id}:meta", json.dumps(meta))
    
    def retry_failed_workflow(self, execution_id: str) -> None:
        """Smart retry - only re-runs failed nodes, keeps completed ones intact"""
        node_states = self.redis.hgetall(f"exec:{execution_id}:node_state")
        if not node_states:
            return
        
        # Find which nodes actually failed
        failed_nodes = [
            node_id.decode('utf-8') 
            for node_id, state in node_states.items() 
            if state.decode('utf-8') == NodeState.FAILED.value
        ]
        
        if not failed_nodes:
            return
        
        meta_str = self.redis.get(f"exec:{execution_id}:meta")
        if meta_str:
            meta = json.loads(meta_str)
            meta["status"] = WorkflowStatus.RUNNING.value
            meta.pop("error", None)
            meta.pop("failed_node", None)
            self.redis.set(f"exec:{execution_id}:meta", json.dumps(meta))
        
        # Reset failed nodes so they can run again
        pipe = self.redis.pipeline()
        for node_id in failed_nodes:
            pipe.hset(f"exec:{execution_id}:node_state", node_id, NodeState.PENDING.value)
            pipe.srem(f"exec:{execution_id}:dispatched", node_id)
            pipe.delete(f"exec:{execution_id}:completion_processed:{node_id}")
        pipe.execute()
        
        for node_id in failed_nodes:
            self.retry_handler.reset_retry_count(execution_id, node_id)
        
        for node_id in failed_nodes:
            node_config_str = self.redis.hget(f"exec:{execution_id}:node_configs", node_id)
            if not node_config_str:
                continue
            
            node_data = json.loads(node_config_str)
            dependencies = node_data.get("dependencies", [])
            
            # Only count dependencies that still need to run (completed ones can be reused)
            pending_deps = 0
            for dep_id in dependencies:
                dep_state = self.redis.hget(f"exec:{execution_id}:node_state", dep_id)
                if dep_state:
                    state_str = dep_state.decode('utf-8')
                    if state_str == NodeState.PENDING.value or state_str == NodeState.FAILED.value:
                        pending_deps += 1
            
            self.redis.hset(f"exec:{execution_id}:deps_remaining", node_id, pending_deps)
        
        # Kick off failed nodes whose dependencies are already done
        for node_id in failed_nodes:
            remaining = int(self.redis.hget(f"exec:{execution_id}:deps_remaining", node_id) or 0)
            if remaining == 0:
                node_config_str = self.redis.hget(f"exec:{execution_id}:node_configs", node_id)
                if node_config_str:
                    data = json.loads(node_config_str)
                    node_config = NodeConfig(
                        node_id,
                        data["task_type"],
                        data["config"],
                        data["dependencies"],
                        data.get("timeout_seconds", 300)
                    )
                    workflow = WorkflowDAG(execution_id, {node_id: node_config}, {}, {})
                    self._dispatch_node(workflow, node_id)
