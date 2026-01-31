"""Worker task registry with idempotency checks."""

from typing import Dict, Any, Callable, List
import redis
import os
from shared.constants import REDIS_KEY_TTL_SECONDS

redis_client = redis.Redis.from_url(
    os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    decode_responses=False
)


def ensure_idempotency(execution_id: str, node_id: str) -> bool:
    """Prevents duplicate task execution using Redis SETNX"""
    key = f"exec:{execution_id}:taskdone:{execution_id}:{node_id}"
    was_set = redis_client.setnx(key, "1")
    
    if was_set:
        redis_client.expire(key, REDIS_KEY_TTL_SECONDS)
        return True
    
    return False


def increment_run_counter(execution_id: str, node_id: str) -> int:
    """Tracks how many times a node actually runs (for exactly-once verification)"""
    key = f"exec:{execution_id}:runs:{node_id}"
    count = redis_client.incr(key)
    redis_client.expire(key, REDIS_KEY_TTL_SECONDS)
    return count



TaskHandler = Callable[[str, str, Dict[str, Any]], Dict[str, Any]]
_task_registry: Dict[str, TaskHandler] = {}


def register_task(task_type: str):
    def decorator(func: TaskHandler):
        _task_registry[task_type] = func
        return func
    return decorator


def get_task_handler(task_type: str) -> TaskHandler:
    if task_type not in _task_registry:
        raise ValueError(f"Unknown task type: {task_type}")
    return _task_registry[task_type]


def list_task_types() -> List[str]:
    return list(_task_registry.keys())
