"""LLM and external task handlers."""

import time
import random
import json
from typing import Dict, Any
from services.worker.handlers.registry import register_task
from shared.exceptions import TaskError


@register_task("llm_service")
def llm_service_handler(execution_id: str, node_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    # Simulate occasional transient errors for testing (10% chance)
    if random.random() < 0.1:
        error_type = random.choice([429, 503, 500])
        
        retry_after = None
        if error_type == 429:
            retry_after = random.randint(5, 15)
        
        error = TaskError(
            error_type="LLM_SERVICE_ERROR",
            error_message=f"LLM service returned {error_type}",
            http_status_code=error_type,
            is_retryable=True,
            retry_after_seconds=retry_after,
            context={"model": config.get("model", "gpt-3.5-turbo")}
        )
        raise Exception(json.dumps(error.to_dict()))
    
    time.sleep(random.uniform(1, 2))
    prompt = config.get("prompt", "")
    return {
        "outputs": {
            "result": f"Mock LLM response for prompt: {prompt[:50]}...",
            "model": config.get("model", "gpt-3.5-turbo"),
            "tokens": len(prompt.split())
        }
    }


@register_task("call_external_service")
def call_external_service_handler(execution_id: str, node_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mock external service handler for testing.
    
    Config params:
    - url: The full URL to call (required)
    - method: HTTP method (GET, POST, etc.) - defaults to GET
    - body: Request body for POST/PUT requests
    - headers: Optional headers dict
    """
    url = config.get("url", "")
    method = config.get("method", "GET")
    
    # Simulate occasional transient errors for testing (10% chance)
    if random.random() < 0.1:
        error_type = random.choice([502, 503, 504])
        error = TaskError(
            error_type="EXTERNAL_SERVICE_ERROR",
            error_message=f"External service returned {error_type}",
            http_status_code=error_type,
            is_retryable=True,
            context={"url": url, "method": method}
        )
        raise Exception(json.dumps(error.to_dict()))
    
    time.sleep(random.uniform(1, 2))
    return {
        "outputs": {
            "result": {
                "id": node_id,
                "url": url,
                "method": method,
                "timestamp": time.time(),
                "message": "Mock external service response"
            },
            "status": "success"
        }
    }
