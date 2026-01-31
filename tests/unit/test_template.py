"""
Unit tests for template resolution.
"""

import pytest
import json
from unittest.mock import Mock
from services.orchestrator.engine.template import TemplateResolver, TemplateResolutionError


def test_resolve_simple_template():
    """Basic template resolution works"""
    redis_mock = Mock()
    redis_mock.hgetall.return_value = {
        b"node1.result": json.dumps(42).encode('utf-8')
    }
    
    resolver = TemplateResolver(redis_mock)
    
    config = {
        "input": "{{ node1.result }}"
    }
    
    resolved = resolver.resolve("exec123", config)
    
    assert resolved["input"] == 42
    redis_mock.hgetall.assert_called_once_with("exec:exec123:outputs")


def test_resolve_multiple_templates():
    """Multiple templates in same config resolve correctly"""
    redis_mock = Mock()
    redis_mock.hgetall.return_value = {
        b"node1.result": json.dumps(10).encode('utf-8'),
        b"node2.result": json.dumps(20).encode('utf-8')
    }
    
    resolver = TemplateResolver(redis_mock)
    
    config = {
        "a": "{{ node1.result }}",
        "b": "{{ node2.result }}"
    }
    
    resolved = resolver.resolve("exec123", config)
    
    assert resolved["a"] == 10
    assert resolved["b"] == 20


def test_resolve_template_not_found():
    """Should error if template references missing output"""
    redis_mock = Mock()
    redis_mock.hgetall.return_value = {}  # No outputs available
    
    resolver = TemplateResolver(redis_mock)
    
    config = {
        "input": "{{ node1.result }}"
    }
    
    with pytest.raises(TemplateResolutionError, match="Template resolution failed"):
        resolver.resolve("exec123", config)


def test_resolve_no_templates():
    """Config without templates passes through unchanged"""
    redis_mock = Mock()
    redis_mock.hgetall.return_value = {}
    
    resolver = TemplateResolver(redis_mock)
    
    config = {
        "value": 42,
        "name": "test"
    }
    
    resolved = resolver.resolve("exec123", config)
    
    assert resolved == config


def test_resolve_nested_config():
    """Templates in nested objects work"""
    redis_mock = Mock()
    redis_mock.hgetall.return_value = {
        b"node1.output": json.dumps("test_value").encode('utf-8')
    }
    
    resolver = TemplateResolver(redis_mock)
    
    config = {
        "nested": {
            "value": "{{ node1.output }}"
        }
    }
    
    resolved = resolver.resolve("exec123", config)
    
    assert resolved["nested"]["value"] == "test_value"
