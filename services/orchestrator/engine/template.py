"""Template resolution for {{ node_id.output_key }} syntax using Jinja2."""

import json
from typing import Dict, Any
from jinja2 import Environment, BaseLoader, TemplateSyntaxError, UndefinedError
from jinja2.sandbox import SandboxedEnvironment


class TemplateResolutionError(Exception):
    pass


class TemplateResolver:
    
    def __init__(self, redis_client):
        self.redis = redis_client
        
        # Use sandboxed Jinja2 environment for security
        self.jinja_env = SandboxedEnvironment(
            loader=BaseLoader(),
            autoescape=False,
            # Disable dangerous features
            trim_blocks=True,
            lstrip_blocks=True,
        )
    
    def resolve(self, execution_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """Resolves {{ node_id.output_key }} templates in config using Jinja2"""
        # Load all outputs for this execution
        outputs_raw = self.redis.hgetall(f"exec:{execution_id}:outputs")
        
        # Parse outputs into a nested dict structure
        outputs = {}
        for key, value in outputs_raw.items():
            key_str = key.decode('utf-8')
            value_str = value.decode('utf-8')
            
            # Parse node_id.output_key
            if '.' in key_str:
                node_id, output_key = key_str.split('.', 1)
                
                if node_id not in outputs:
                    outputs[node_id] = {}
                
                # Try to parse JSON value
                try:
                    outputs[node_id][output_key] = json.loads(value_str)
                except json.JSONDecodeError:
                    outputs[node_id][output_key] = value_str
        
        # Recursively resolve templates in config
        return self._resolve_recursive(config, outputs)
    
    def _resolve_recursive(self, value: Any, context: Dict[str, Any]) -> Any:
        """Recursively walks through config to resolve all templates"""
        
        if isinstance(value, str):
            # Check if string contains template
            if '{{' in value and '}}' in value:
                try:
                    template = self.jinja_env.from_string(value)
                    resolved = template.render(context)
                    
                    if resolved.startswith(('{', '[', '"')) or resolved in ('true', 'false', 'null'):
                        try:
                            return json.loads(resolved)
                        except json.JSONDecodeError:
                            pass
                    else:
                        # Try to parse as number
                        try:
                            # Check if it's an integer
                            if '.' not in resolved:
                                return int(resolved)
                            else:
                                return float(resolved)
                        except (ValueError, TypeError):
                            pass
                    
                    return resolved
                
                except (TemplateSyntaxError, UndefinedError) as e:
                    raise TemplateResolutionError(f"Template resolution failed: {str(e)}")
            
            return value
        
        elif isinstance(value, dict):
            return {k: self._resolve_recursive(v, context) for k, v in value.items()}
        
        elif isinstance(value, list):
            return [self._resolve_recursive(item, context) for item in value]
        
        else:
            return value
