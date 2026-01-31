"""DAG validation and cycle detection."""

import json
import re
from typing import Dict, List, Set, Tuple, Any
from collections import deque
from shared.constants import (
    ALLOWED_HANDLER_TYPES,
    MAX_NODES_PER_WORKFLOW,
    MAX_CONFIG_SIZE_BYTES,
    MAX_TEMPLATE_LENGTH,
    MIN_NODE_TIMEOUT_SECONDS,
    MAX_NODE_TIMEOUT_SECONDS
)


class DAGValidationError(Exception):
    pass


def validate_dag(nodes: List[Dict[str, Any]]) -> Tuple[Dict[str, List[str]], Dict[str, int]]:
    if not nodes:
        raise DAGValidationError("DAG must contain at least one node")
    
    # Check workflow size limit
    if len(nodes) > MAX_NODES_PER_WORKFLOW:
        raise DAGValidationError(f"Workflow exceeds maximum node limit: {len(nodes)} > {MAX_NODES_PER_WORKFLOW}")
    
    node_ids, node_map = set(), {}
    for node in nodes:
        # Validate each node
        validate_node_config(node)
        
        node_id = node.get("id")
        if not node_id:
            raise DAGValidationError("All nodes must have an 'id' field")
        
        if node_id in node_ids:
            raise DAGValidationError(f"Duplicate node ID: {node_id}")
        
        node_ids.add(node_id)
        node_map[node_id] = node
    
    adjacency = {nid: [] for nid in node_ids}
    deps_count = {}
    
    for node_id, node in node_map.items():
        dependencies = node.get("dependencies", [])
        deps_count[node_id] = len(dependencies)
        
        for dep_id in dependencies:
            if dep_id not in node_ids:
                raise DAGValidationError(f"Node '{node_id}' depends on non-existent node '{dep_id}'")
            adjacency[dep_id].append(node_id)
    
    if has_cycle(adjacency, node_ids):
        raise DAGValidationError("DAG contains a cycle")
    
    if not [nid for nid, cnt in deps_count.items() if cnt == 0]:
        raise DAGValidationError("DAG must have at least one root node")
    return adjacency, deps_count


def has_cycle(adjacency: Dict[str, List[str]], node_ids: Set[str]) -> bool:
    in_degree = {nid: 0 for nid in node_ids}
    for children in adjacency.values():
        for child in children:
            in_degree[child] += 1
    
    queue = deque([nid for nid, deg in in_degree.items() if deg == 0])
    processed = 0
    
    while queue:
        node_id = queue.popleft()
        processed += 1
        for child in adjacency[node_id]:
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)
    
    return processed != len(node_ids)


def validate_node_config(node: Dict[str, Any]) -> None:
    """Checks if node config is valid and secure"""
    for field in ["id", "handler"]:
        if field not in node:
            raise DAGValidationError(f"Node missing required field: {field}")
    
    node_id = node["id"]
    handler = node.get("handler")
    
    if handler not in ALLOWED_HANDLER_TYPES:
        raise DAGValidationError(
            f"Node '{node_id}' has invalid handler type: '{handler}'. "
            f"Allowed handlers: {', '.join(sorted(ALLOWED_HANDLER_TYPES))}"
        )
    
    if "dependencies" in node and not isinstance(node["dependencies"], list):
        raise DAGValidationError(f"Node '{node_id}' dependencies must be a list")
    
    if "config" in node:
        config_str = json.dumps(node["config"])
        config_size = len(config_str.encode('utf-8'))
        if config_size > MAX_CONFIG_SIZE_BYTES:
            raise DAGValidationError(
                f"Node '{node_id}' config exceeds size limit: {config_size} > {MAX_CONFIG_SIZE_BYTES} bytes"
            )
        validate_templates_in_config(node_id, node["config"])
    
    if "timeout_seconds" in node:
        timeout = node["timeout_seconds"]
        if not isinstance(timeout, int) or timeout < MIN_NODE_TIMEOUT_SECONDS or timeout > MAX_NODE_TIMEOUT_SECONDS:
            raise DAGValidationError(
                f"Node '{node_id}' timeout_seconds must be between {MIN_NODE_TIMEOUT_SECONDS} and {MAX_NODE_TIMEOUT_SECONDS}"
            )


def validate_templates_in_config(node_id: str, config: Dict[str, Any]) -> None:
    """Check all template strings in config for security issues"""
    template_pattern = re.compile(r'\{\{.*?\}\}')
    
    def check_value(value: Any, path: str = "") -> None:
        if isinstance(value, str):
            templates = template_pattern.findall(value)
            for template in templates:
                if len(template) > MAX_TEMPLATE_LENGTH:
                    raise DAGValidationError(
                        f"Node '{node_id}' has template exceeding length limit at {path}: {len(template)} > {MAX_TEMPLATE_LENGTH}"
                    )
                
                # Block patterns that could be used for code injection
                dangerous_patterns = ['__import__', 'eval', 'exec', 'compile', 'open', 'file']
                template_lower = template.lower()
                for pattern in dangerous_patterns:
                    if pattern in template_lower:
                        raise DAGValidationError(
                            f"Node '{node_id}' has template with forbidden pattern '{pattern}' at {path}"
                        )
        
        elif isinstance(value, dict):
            for k, v in value.items():
                check_value(v, f"{path}.{k}" if path else k)
        
        elif isinstance(value, list):
            for i, item in enumerate(value):
                check_value(item, f"{path}[{i}]")
    
    check_value(config)
