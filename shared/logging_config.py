"""Centralized logging configuration with correlation ID support."""

import logging
import sys
from contextvars import ContextVar
from pythonjsonlogger import jsonlogger

correlation_id_var: ContextVar[str] = ContextVar('correlation_id', default='')


class CorrelationIdFilter(logging.Filter):
    """Adds correlation_id to all log records"""
    
    def filter(self, record):
        record.correlation_id = correlation_id_var.get('')
        return True


def setup_logging(service_name: str) -> None:
    """Sets up JSON logging with correlation ID support"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers = []
    
    json_handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(correlation_id)s %(message)s',
        rename_fields={
            'asctime': 'timestamp',
            'name': 'logger',
            'levelname': 'level',
        }
    )
    
    json_handler.setFormatter(formatter)
    json_handler.addFilter(CorrelationIdFilter())
    logger.addHandler(json_handler)
    logging.info(f"{service_name} logging configured with JSON format and correlation ID support")


def set_correlation_id(correlation_id: str) -> None:
    correlation_id_var.set(correlation_id)


def get_correlation_id() -> str:
    return correlation_id_var.get('')
