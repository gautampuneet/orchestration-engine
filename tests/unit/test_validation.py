"""
Unit tests for DAG validation.
"""

import pytest
from services.api.domain.validation import (
    validate_dag,
    has_cycle,
    validate_node_config,
    DAGValidationError
)


def test_validate_simple_dag():
    """Checks that a basic DAG validates correctly"""
    nodes = [
        {"id": "node1", "handler": "compute", "config": {}, "dependencies": []},
        {"id": "node2", "handler": "compute", "config": {}, "dependencies": ["node1"]}
    ]
    
    adjacency, deps_count = validate_dag(nodes)
    
    assert "node1" in adjacency
    assert "node2" in adjacency
    assert deps_count["node1"] == 0
    assert deps_count["node2"] == 1
    assert "node2" in adjacency["node1"]


def test_validate_dag_with_cycle():
    """Makes sure cycles get caught"""
    nodes = [
        {"id": "node1", "handler": "compute", "config": {}, "dependencies": ["node2"]},
        {"id": "node2", "handler": "compute", "config": {}, "dependencies": ["node1"]}
    ]
    
    with pytest.raises(DAGValidationError, match="cycle"):
        validate_dag(nodes)


def test_validate_dag_missing_dependency():
    """Catches references to nodes that don't exist"""
    nodes = [
        {"id": "node1", "handler": "compute", "config": {}, "dependencies": ["node_nonexistent"]}
    ]
    
    with pytest.raises(DAGValidationError, match="non-existent"):
        validate_dag(nodes)


def test_validate_dag_no_root():
    """Rejects DAGs with no entry points"""
    nodes = [
        {"id": "node1", "handler": "compute", "config": {}, "dependencies": ["node2"]},
        {"id": "node2", "handler": "compute", "config": {}, "dependencies": ["node1"]}
    ]
    
    with pytest.raises(DAGValidationError):
        validate_dag(nodes)


def test_validate_node_config_valid():
    """Valid node config should pass without errors"""
    node = {
        "id": "node1",
        "handler": "compute",
        "config": {"operation": "add", "a": 1, "b": 2},
        "dependencies": []
    }
    
    validate_node_config(node)  # Should not raise


def test_validate_node_config_missing_id():
    """Node without ID should fail validation"""
    node = {
        "handler": "compute",
        "config": {}
    }
    
    with pytest.raises(DAGValidationError, match="id"):
        validate_node_config(node)


def test_validate_node_config_invalid_type():
    """Invalid handler type should be rejected"""
    node = {
        "id": "node1",
        "handler": "invalid_type",
        "config": {}
    }
    
    with pytest.raises(DAGValidationError, match="invalid handler type"):
        validate_node_config(node)


def test_has_cycle_simple():
    """Detects a basic two-node cycle"""
    adjacency = {
        "node1": ["node2"],
        "node2": ["node1"]
    }
    node_ids = {"node1", "node2"}
    
    assert has_cycle(adjacency, node_ids) is True


def test_has_cycle_no_cycle():
    """No cycle in a simple chain"""
    adjacency = {
        "node1": ["node2"],
        "node2": []
    }
    node_ids = {"node1", "node2"}
    
    assert has_cycle(adjacency, node_ids) is False


def test_fan_out_fan_in():
    """Fan-out/fan-in pattern should validate properly"""
    nodes = [
        {"id": "root", "handler": "compute", "config": {}, "dependencies": []},
        {"id": "branch1", "handler": "compute", "config": {}, "dependencies": ["root"]},
        {"id": "branch2", "handler": "compute", "config": {}, "dependencies": ["root"]},
        {"id": "merge", "handler": "aggregate", "config": {}, "dependencies": ["branch1", "branch2"]}
    ]
    
    adjacency, deps_count = validate_dag(nodes)
    
    assert deps_count["root"] == 0
    assert deps_count["branch1"] == 1
    assert deps_count["branch2"] == 1
    assert deps_count["merge"] == 2
    assert len(adjacency["root"]) == 2
    assert "merge" in adjacency["branch1"]
    assert "merge" in adjacency["branch2"]
