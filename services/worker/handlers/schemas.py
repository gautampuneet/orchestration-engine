"""Pydantic schemas for handler configuration validation."""

from typing import Union, Dict, Any, Literal, Optional
from pydantic import BaseModel, Field, field_validator


class HttpRequestConfig(BaseModel):
    """Config schema for http_request handler"""
    url: str
    method: Literal["GET", "POST", "PUT", "DELETE", "PATCH"] = "GET"
    headers: Optional[Dict[str, str]] = None
    body: Optional[Dict[str, Any]] = None
    timeout: int = Field(default=30, ge=1, le=300)
    
    class Config:
        extra = "forbid"


class TransformConfig(BaseModel):
    """Config schema for transform handler"""
    input: Any
    transform_type: Literal["uppercase", "lowercase", "json_parse", "json_stringify"]
    
    class Config:
        extra = "forbid"


class AggregateConfig(BaseModel):
    """Config schema for aggregate handler"""
    inputs: list
    aggregate_type: Literal["sum", "concat", "list"] = "list"
    
    class Config:
        extra = "forbid"


class ComputeConfig(BaseModel):
    """Config schema for compute handler"""
    operation: Literal["add", "subtract", "multiply", "divide"]
    a: Union[float, int]
    b: Union[float, int]
    
    @field_validator('b')
    @classmethod
    def validate_division_by_zero(cls, v, values):
        if 'operation' in values.data and values.data['operation'] == 'divide' and v == 0:
            raise ValueError("Cannot divide by zero")
        return v
    
    class Config:
        extra = "forbid"


class LlmServiceConfig(BaseModel):
    """Config schema for llm_service handler"""
    prompt: str = Field(min_length=1, max_length=10000)
    model: str = "gpt-3.5-turbo"
    
    class Config:
        extra = "forbid"


class CallExternalServiceConfig(BaseModel):
    """Config schema for call_external_service handler"""
    service: str
    input: Optional[Any] = None
    
    class Config:
        extra = "forbid"


# Handler schema registry
HANDLER_SCHEMAS = {
    "http_request": HttpRequestConfig,
    "transform": TransformConfig,
    "aggregate": AggregateConfig,
    "compute": ComputeConfig,
    "llm_service": LlmServiceConfig,
    "call_external_service": CallExternalServiceConfig,
}


def validate_handler_config(handler_type: str, config: Dict[str, Any]) -> None:
    """Validates handler config against its Pydantic schema"""
    if handler_type not in HANDLER_SCHEMAS:
        raise ValueError(f"Unknown handler type: {handler_type}")
    
    schema = HANDLER_SCHEMAS[handler_type]
    
    try:
        schema(**config)
    except Exception as e:
        raise ValueError(f"Invalid configuration for handler '{handler_type}': {str(e)}")
