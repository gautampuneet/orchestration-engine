"""External task handlers."""

import json
import time
from typing import Dict, Any
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError
from services.worker.handlers.registry import register_task
from shared.exceptions import TaskError
from shared.constants import RETRYABLE_HTTP_STATUS_CODES


@register_task("http_request")
def http_request_handler(execution_id: str, node_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    url = config.get("url")
    if not url:
        raise ValueError("URL required")
    
    try:
        response = requests.request(
            config.get("method", "GET"),
            url,
            headers=config.get("headers", {}),
            json=config.get("body"),
            timeout=config.get("timeout", 30)
        )
        
        # Check for retryable HTTP errors
        if response.status_code in RETRYABLE_HTTP_STATUS_CODES:
            retry_after = None
            if response.status_code == 429:
                # Extract Retry-After header (can be in seconds or HTTP date)
                retry_after_header = response.headers.get("Retry-After")
                if retry_after_header and retry_after_header.isdigit():
                    retry_after = int(retry_after_header)
            
            error = TaskError(
                error_type="HTTP_ERROR",
                error_message=f"HTTP {response.status_code}: {response.reason}",
                http_status_code=response.status_code,
                is_retryable=True,
                retry_after_seconds=retry_after,
                context={"url": url, "method": config.get("method", "GET")}
            )
            raise Exception(json.dumps(error.to_dict()))
        
        response.raise_for_status()
        
        return {
            "outputs": {
                "result": response.json() if "application/json" in response.headers.get("content-type", "") else response.text,
                "status_code": response.status_code,
                "headers": dict(response.headers)
            }
        }
    
    except (Timeout, ConnectionError) as e:
        # Network errors are retryable
        error = TaskError(
            error_type="NETWORK_ERROR",
            error_message=f"Network error: {str(e)}",
            is_retryable=True,
            context={"url": url, "error_class": type(e).__name__}
        )
        raise Exception(json.dumps(error.to_dict()))
    
    except RequestException as e:
        # Other request errors (4xx client errors) are not retryable
        error = TaskError(
            error_type="REQUEST_ERROR",
            error_message=f"Request failed: {str(e)}",
            is_retryable=False,
            context={"url": url}
        )
        raise Exception(json.dumps(error.to_dict()))


@register_task("transform")
def transform_handler(execution_id: str, node_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    data = config.get("input")
    transform_type = config.get("transform_type")
    
    transforms = {
        "uppercase": lambda d: str(d).upper(),
        "lowercase": lambda d: str(d).lower(),
        "json_parse": lambda d: json.loads(d),
        "json_stringify": lambda d: json.dumps(d)
    }
    
    output = transforms.get(transform_type, lambda d: d)(data)
    return {"outputs": {"result": output}}


@register_task("aggregate")
def aggregate_handler(execution_id: str, node_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    inputs = config.get("inputs", [])
    agg_type = config.get("aggregate_type", "list")
    
    aggregators = {
        "sum": lambda: sum(float(x) for x in inputs if x is not None),
        "concat": lambda: "".join(str(x) for x in inputs if x is not None),
        "list": lambda: inputs
    }
    
    result = aggregators.get(agg_type, lambda: inputs)()
    return {"outputs": {"result": result, "count": len(inputs)}}


@register_task("compute")
def compute_handler(execution_id: str, node_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
    a, b = float(config.get("a")), float(config.get("b"))
    
    ops = {
        "add": a + b,
        "subtract": a - b,
        "multiply": a * b,
        "divide": a / b
    }
    
    result = ops.get(config.get("operation"), a)
    return {"outputs": {"result": result}}
