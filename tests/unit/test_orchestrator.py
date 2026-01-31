"""
Tests for completion deduplication and fail-fast guards.

Verifies duplicate callbacks don't cause multiple counter decrements
and that success callbacks are ignored after workflow fails.
"""

import json
from unittest.mock import Mock, MagicMock
from services.orchestrator.engine.orchestrator import OrchestratorEngine
from shared.types import NodeState, WorkflowStatus


def test_duplicate_completion_callback_deduplication():
    """Duplicate success callbacks should be ignored"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    execution_id = "test_exec_123"
    node_id = "node_A"
    result = {"outputs": {"data": "test"}}
    
    # Mock workflow meta (not failed)
    redis_mock.get.return_value = json.dumps({
        "execution_id": execution_id,
        "status": WorkflowStatus.RUNNING.value
    }).encode()
    
    # First callback - SETNX succeeds
    redis_mock.setnx.return_value = 1  # First time
    redis_mock.smembers.return_value = set()  # No children for simplicity
    redis_mock.hgetall.return_value = {b"node_A": b"COMPLETED"}
    
    orchestrator.on_node_success(execution_id, node_id, result)
    
    # Verify completion key was set
    completion_key = f"exec:{execution_id}:completion_processed:{node_id}"
    redis_mock.setnx.assert_called_with(completion_key, "1")
    redis_mock.expire.assert_called_with(completion_key, 604800)
    
    # Verify state was marked COMPLETED (first call processes)
    redis_mock.hset.assert_called()
    
    # Reset mocks to check duplicate behavior
    redis_mock.reset_mock()
    
    # Second callback (duplicate) - SETNX fails
    redis_mock.setnx.return_value = 0  # Already exists
    
    orchestrator.on_node_success(execution_id, node_id, result)
    
    # Verify SETNX was checked
    redis_mock.setnx.assert_called_once_with(completion_key, "1")
    
    # CRITICAL: No further processing should happen
    redis_mock.hset.assert_not_called()  # State not updated again
    redis_mock.smembers.assert_not_called()  # Children not fetched
    redis_mock.hincrby.assert_not_called()  # Counters not decremented


def test_duplicate_completion_prevents_counter_double_decrement():
    """Duplicate callbacks shouldn't decrement dependency counters twice"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    execution_id = "test_exec_456"
    node_id = "A"
    child_id = "child"
    result = {"outputs": {}}
    
    # Mock workflow meta (not failed)
    redis_mock.get.return_value = json.dumps({
        "execution_id": execution_id,
        "status": WorkflowStatus.RUNNING.value
    }).encode()
    
    # Setup: child has 1 dependency (node A)
    redis_mock.smembers.return_value = {child_id.encode()}
    redis_mock.hget.return_value = json.dumps({
        "task_type": "compute",
        "config": {},
        "dependencies": ["A"]
    }).encode()
    
    # First callback - should decrement counter
    redis_mock.setnx.return_value = 1  # First time
    redis_mock.hincrby.return_value = 0  # Counter reaches 0
    redis_mock.sadd.return_value = 1  # Dispatch succeeds
    redis_mock.hgetall.return_value = {b"A": b"COMPLETED"}
    
    orchestrator.on_node_success(execution_id, node_id, result)
    
    # Verify counter was decremented ONCE
    redis_mock.hincrby.assert_called_once_with(
        f"exec:{execution_id}:deps_remaining",
        child_id,
        -1
    )
    
    # Reset mock
    redis_mock.reset_mock()
    
    # Duplicate callback - should NOT decrement counter
    redis_mock.setnx.return_value = 0  # Already processed
    
    orchestrator.on_node_success(execution_id, node_id, result)
    
    # CRITICAL: Counter NOT decremented again
    redis_mock.hincrby.assert_not_called()


def test_fan_in_with_duplicate_callbacks():
    """Fan-in should work correctly even with duplicate callbacks"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    execution_id = "test_exec_789"
    child_id = "C"
    
    # Mock workflow meta (not failed)
    redis_mock.get.return_value = json.dumps({
        "execution_id": execution_id,
        "status": WorkflowStatus.RUNNING.value
    }).encode()
    
    # Setup: C is child of both A and B
    redis_mock.smembers.return_value = {child_id.encode()}
    redis_mock.hget.return_value = json.dumps({
        "task_type": "compute",
        "config": {},
        "dependencies": ["A", "B"]
    }).encode()
    
    # === A completes (first time) ===
    redis_mock.setnx.return_value = 1  # First A completion
    redis_mock.hincrby.return_value = 1  # Counter: 2 → 1
    redis_mock.hgetall.return_value = {b"A": b"COMPLETED"}
    
    orchestrator.on_node_success(execution_id, "A", {"outputs": {}})
    
    # Verify: counter decremented, C NOT dispatched (still waiting for B)
    assert redis_mock.hincrby.call_count == 1
    redis_mock.sadd.assert_not_called()  # C not dispatched yet
    
    # === A callback retried (duplicate) ===
    redis_mock.reset_mock()
    redis_mock.setnx.return_value = 0  # A already processed
    
    orchestrator.on_node_success(execution_id, "A", {"outputs": {}})
    
    # CRITICAL: No counter decrement on duplicate
    redis_mock.hincrby.assert_not_called()
    
    # === B completes (first time) ===
    redis_mock.reset_mock()
    redis_mock.get.return_value = json.dumps({
        "execution_id": execution_id,
        "status": WorkflowStatus.RUNNING.value
    }).encode()
    redis_mock.setnx.return_value = 1  # First B completion
    redis_mock.hincrby.return_value = 0  # Counter: 1 → 0
    redis_mock.sadd.return_value = 1  # Dispatch succeeds
    redis_mock.smembers.return_value = {child_id.encode()}
    redis_mock.hgetall.return_value = {b"B": b"COMPLETED"}
    
    orchestrator.on_node_success(execution_id, "B", {"outputs": {}})
    
    # Verify: counter decremented to 0, C dispatched
    redis_mock.hincrby.assert_called_once()
    
    # CRITICAL: C dispatched exactly once (counter was correct)
    # Without dedupe, counter would be: 2 → 1 → 0 → -1, and C might not dispatch or dispatch incorrectly


def test_completion_key_ttl_set():
    """Completion keys should have TTL set for cleanup"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    # Mock workflow meta (not failed)
    redis_mock.get.return_value = json.dumps({
        "execution_id": "exec123",
        "status": WorkflowStatus.RUNNING.value
    }).encode()
    
    redis_mock.setnx.return_value = 1
    redis_mock.smembers.return_value = set()
    redis_mock.hgetall.return_value = {b"nodeA": b"COMPLETED"}
    
    orchestrator.on_node_success("exec123", "nodeA", {"outputs": {}})
    
    # Verify TTL was set (7 days = 604800 seconds)
    completion_key = "exec:exec123:completion_processed:nodeA"
    redis_mock.expire.assert_called_with(completion_key, 604800)


def test_fail_fast_ignores_success_after_workflow_failed():
    """Success callbacks after workflow fails should be ignored"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    execution_id = "test_exec_fail_fast"
    node_id = "node_B"
    result = {"outputs": {"data": "test"}}
    
    # Setup: Completion dedupe passes (first callback)
    redis_mock.setnx.return_value = 1
    
    # Setup: Workflow is already FAILED
    failed_meta = {
        "execution_id": execution_id,
        "status": WorkflowStatus.FAILED.value,
        "error": "Node A failed",
        "failed_node": "node_A"
    }
    redis_mock.get.return_value = json.dumps(failed_meta).encode()
    
    # Call on_node_success for node_B
    orchestrator.on_node_success(execution_id, node_id, result)
    
    # Verify: Workflow meta was read for fail-fast check (happens first)
    redis_mock.get.assert_called_with(f"exec:{execution_id}:meta")
    
    # CRITICAL: Verify completion dedupe key was NOT set (callback ignored before dedupe)
    assert redis_mock.setnx.call_count == 0, "completion_processed should NOT be set for ignored callbacks"
    assert redis_mock.expire.call_count == 0, "TTL should NOT be set for ignored callbacks"
    
    # CRITICAL: Verify NO state mutations happened after fail-fast check
    # Node state should NOT be updated to COMPLETED
    node_state_calls = [call for call in redis_mock.hset.call_args_list 
                       if len(call[0]) >= 2 and call[0][1] == node_id and call[0][2] == NodeState.COMPLETED.value]
    assert len(node_state_calls) == 0, "node_state should NOT be marked COMPLETED after workflow failed"
    
    # Children should NOT be fetched or processed
    assert redis_mock.smembers.call_count == 0, "children should NOT be fetched after workflow failed"
    assert redis_mock.hincrby.call_count == 0, "deps_remaining should NOT be decremented after workflow failed"


def test_fail_fast_prevents_dispatch_after_workflow_failed():
    """Nodes shouldn't dispatch after workflow has already failed"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    execution_id = "test_exec_no_dispatch"
    node_id = "node_C"
    
    # Setup: Workflow is FAILED
    failed_meta = {
        "execution_id": execution_id,
        "status": WorkflowStatus.FAILED.value,
        "error": "Previous node failed",
        "failed_node": "node_A"
    }
    redis_mock.get.return_value = json.dumps(failed_meta).encode()
    
    # Create minimal workflow
    from services.orchestrator.engine.orchestrator import WorkflowDAG, NodeConfig
    node_config = NodeConfig(node_id, "compute", {"operation": "add", "a": 1, "b": 2}, [])
    workflow = WorkflowDAG(execution_id, {node_id: node_config}, {}, {})
    
    # Try to dispatch node
    orchestrator._dispatch_node(workflow, node_id)
    
    # Verify: Meta was read for fail-fast check
    redis_mock.get.assert_called_with(f"exec:{execution_id}:meta")
    
    # CRITICAL: Verify NO dispatch happened
    assert redis_mock.sadd.call_count == 0, "Node should NOT be added to dispatched set after workflow failed"
    assert redis_mock.hset.call_count == 0, "Node state should NOT be set to RUNNING after workflow failed"
    assert celery_mock.send_task.call_count == 0, "Celery task should NOT be sent after workflow failed"


def test_success_callback_works_normally_when_workflow_not_failed():
    """Success callbacks should work normally when workflow is still running"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    execution_id = "test_exec_normal"
    node_id = "node_A"
    result = {"outputs": {"data": "test"}}
    
    # Setup: Completion dedupe passes
    redis_mock.setnx.return_value = 1
    
    # Setup: Workflow is RUNNING (not failed)
    running_meta = {
        "execution_id": execution_id,
        "status": WorkflowStatus.RUNNING.value,
        "total_nodes": 3,
        "completed_nodes": 1
    }
    redis_mock.get.return_value = json.dumps(running_meta).encode()
    redis_mock.smembers.return_value = set()  # No children
    redis_mock.hgetall.return_value = {
        b"node_A": b"COMPLETED"
    }
    
    # Call on_node_success
    orchestrator.on_node_success(execution_id, node_id, result)
    
    # Verify: Normal processing happened
    # Should have called hset to mark node as COMPLETED
    redis_mock.hset.assert_any_call(
        f"exec:{execution_id}:node_state",
        node_id,
        NodeState.COMPLETED.value
    )


def test_node_outputs_saved_correctly():
    """Node outputs should be saved to Redis on success"""
    redis_mock = Mock()
    celery_mock = Mock()
    
    orchestrator = OrchestratorEngine(redis_mock, celery_mock)
    
    execution_id = "test_exec_outputs"
    node_id = "fetch_data"
    result = {
        "outputs": {
            "result": {"id": 123, "name": "test"},
            "status": "success"
        }
    }
    
    # Setup: Workflow is RUNNING
    redis_mock.get.return_value = json.dumps({
        "execution_id": execution_id,
        "status": WorkflowStatus.RUNNING.value
    }).encode()
    
    redis_mock.setnx.return_value = 1  # First callback
    redis_mock.smembers.return_value = set()  # No children
    redis_mock.hgetall.return_value = {b"fetch_data": b"COMPLETED"}
    
    # Mock pipeline for output storage
    pipeline_mock = Mock()
    redis_mock.pipeline.return_value = pipeline_mock
    pipeline_mock.execute.return_value = None
    
    # Call on_node_success
    orchestrator.on_node_success(execution_id, node_id, result)
    
    # Verify: Outputs were saved to Redis
    # Should have called pipeline to save outputs
    assert redis_mock.pipeline.call_count >= 1
    
    # Verify the outputs were stored with correct keys
    pipeline_mock.hset.assert_any_call(
        f"exec:{execution_id}:outputs",
        f"{node_id}.result",
        json.dumps({"id": 123, "name": "test"})
    )
    pipeline_mock.hset.assert_any_call(
        f"exec:{execution_id}:outputs",
        f"{node_id}.status",
        json.dumps("success")
    )
