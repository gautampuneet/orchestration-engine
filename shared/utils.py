"""Shared utilities."""


def generate_task_id(execution_id: str, node_id: str) -> str:
    return f"{execution_id}:{node_id}"
