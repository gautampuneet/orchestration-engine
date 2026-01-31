"""Retry handler for intelligent automatic retry logic."""

import json
import logging
from typing import Optional, Dict, Any, Tuple
from shared.exceptions import TaskError
from shared.constants import (
    MAX_RETRY_ATTEMPTS,
    INITIAL_RETRY_DELAY_SECONDS,
    MAX_RETRY_DELAY_SECONDS,
    EXTERNAL_HANDLERS
)


class RetryHandler:
    """Decides when to retry failed tasks and calculates backoff delays"""
    
    def __init__(self, redis_client):
        self.redis = redis_client
    
    def should_retry_node(
        self, 
        execution_id: str, 
        node_id: str, 
        error: str
    ) -> Tuple[bool, Optional[float]]:
        """Checks if node should be retried and calculates delay"""
        # Parse error as TaskError
        task_error = self._parse_task_error(error)
        if not task_error:
            logging.info(
                "Node error is not retryable (not a structured TaskError)",
                extra={"execution_id": execution_id, "node_id": node_id}
            )
            return False, None
        
        # Get node config to check handler type
        node_config = self._get_node_config(execution_id, node_id)
        if not node_config:
            logging.warning(
                "Cannot find node config for retry decision",
                extra={"execution_id": execution_id, "node_id": node_id}
            )
            return False, None
        
        task_type = node_config.get("task_type")
        
        # Check if retry is applicable
        if not self._is_retryable(task_error, task_type):
            logging.info(
                "Node not eligible for retry",
                extra={
                    "execution_id": execution_id,
                    "node_id": node_id,
                    "task_type": task_type,
                    "is_retryable": task_error.is_retryable,
                    "is_external": task_type in EXTERNAL_HANDLERS
                }
            )
            return False, None
        
        # Check retry count
        retry_count = self._get_retry_count(execution_id, node_id)
        if retry_count >= MAX_RETRY_ATTEMPTS:
            logging.warning(
                "Maximum retry attempts reached",
                extra={
                    "execution_id": execution_id,
                    "node_id": node_id,
                    "retry_count": retry_count,
                    "max_attempts": MAX_RETRY_ATTEMPTS
                }
            )
            return False, None
        
        # Increment retry count
        self._increment_retry_count(execution_id, node_id)
        
        # Calculate backoff delay
        delay = self._calculate_backoff_delay(retry_count, task_error)
        
        logging.info(
            "Node will be retried",
            extra={
                "execution_id": execution_id,
                "node_id": node_id,
                "retry_attempt": retry_count + 1,
                "delay_seconds": delay
            }
        )
        
        return True, delay
    
    def _parse_task_error(self, error: str) -> Optional[TaskError]:
        try:
            error_dict = json.loads(error)
            if isinstance(error_dict, dict) and "error_type" in error_dict:
                return TaskError(**error_dict)
        except (json.JSONDecodeError, Exception) as e:
            logging.debug(f"Failed to parse error as TaskError: {e}")
        return None
    
    def _get_node_config(self, execution_id: str, node_id: str) -> Optional[Dict[str, Any]]:
        node_config_str = self.redis.hget(f"exec:{execution_id}:node_configs", node_id)
        if not node_config_str:
            return None
        
        try:
            return json.loads(node_config_str)
        except json.JSONDecodeError:
            return None
    
    def _is_retryable(self, task_error: TaskError, task_type: str) -> bool:
        return (
            task_error.is_retryable and
            task_type in EXTERNAL_HANDLERS
        )
    
    def _get_retry_count(self, execution_id: str, node_id: str) -> int:
        retry_count = self.redis.get(f"exec:{execution_id}:retry:{node_id}")
        return int(retry_count) if retry_count else 0
    
    def _increment_retry_count(self, execution_id: str, node_id: str) -> None:
        self.redis.incr(f"exec:{execution_id}:retry:{node_id}")
    
    def _calculate_backoff_delay(self, retry_count: int, task_error: TaskError) -> float:
        """Exponential backoff with Retry-After header support"""
        if task_error.retry_after_seconds:
            # Honor Retry-After header (e.g., from 429 responses)
            delay = min(task_error.retry_after_seconds, MAX_RETRY_DELAY_SECONDS)
            logging.debug(
                f"Using Retry-After header delay: {delay}s",
                extra={"retry_after": task_error.retry_after_seconds}
            )
        else:
            # Exponential backoff: 1s, 2s, 4s, 8s, ...
            delay = min(
                INITIAL_RETRY_DELAY_SECONDS * (2 ** retry_count),
                MAX_RETRY_DELAY_SECONDS
            )
            logging.debug(
                f"Using exponential backoff delay: {delay}s",
                extra={"retry_count": retry_count}
            )
        
        return delay
    
    def reset_retry_count(self, execution_id: str, node_id: str) -> None:
        """Resets retry counter when doing a full workflow retry"""
        self.redis.delete(f"exec:{execution_id}:retry:{node_id}")
