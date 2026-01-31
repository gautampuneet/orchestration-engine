"""FastAPI middleware for correlation ID handling."""

import uuid
import logging
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from shared.logging_config import set_correlation_id, get_correlation_id


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Handles correlation ID extraction/generation for request tracking"""
    
    async def dispatch(self, request: Request, call_next):
        correlation_id = request.headers.get('X-Correlation-ID', str(uuid.uuid4()))
        set_correlation_id(correlation_id)
        
        logging.info(
            "Incoming request",
            extra={
                "method": request.method,
                "path": request.url.path,
                "client": request.client.host if request.client else "unknown"
            }
        )
        
        response = await call_next(request)
        response.headers['X-Correlation-ID'] = correlation_id
        
        logging.info(
            "Outgoing response",
            extra={
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code
            }
        )
        
        return response
