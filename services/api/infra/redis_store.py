"""
Redis store for API service.
"""

import redis
import json
from typing import Optional, Dict, Any
import os


class RedisStore:
    """Redis client wrapper for API service"""
    
    def __init__(self, redis_url: Optional[str] = None):
        url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.client = redis.Redis.from_url(url, decode_responses=False)
    
    def store_dag(self, execution_id: str, dag: Dict[str, Any]) -> None:
        key = f"exec:{execution_id}:dag"
        self.client.set(key, json.dumps(dag))
        self.client.expire(key, 604800)
    
    def get_dag(self, execution_id: str) -> Optional[Dict[str, Any]]:
        key = f"exec:{execution_id}:dag"
        data = self.client.get(key)
        if data:
            return json.loads(data)
        return None
    
    def get_workflow_meta(self, execution_id: str) -> Optional[Dict[str, Any]]:
        key = f"exec:{execution_id}:meta"
        data = self.client.get(key)
        if data:
            return json.loads(data)
        return None
    
    def get_node_states(self, execution_id: str) -> Dict[str, str]:
        key = f"exec:{execution_id}:node_state"
        states = self.client.hgetall(key)
        return {
            k.decode('utf-8'): v.decode('utf-8')
            for k, v in states.items()
        }
    
    def get_outputs(self, execution_id: str) -> Dict[str, Any]:
        key = f"exec:{execution_id}:outputs"
        outputs = self.client.hgetall(key)
        
        result = {}
        for k, v in outputs.items():
            key_str = k.decode('utf-8')
            value_data = json.loads(v.decode('utf-8'))
            
            # Keys are stored as "node_id.output_key"
            if '.' in key_str:
                node_id, output_key = key_str.split('.', 1)
                if node_id not in result:
                    result[node_id] = {}
                result[node_id][output_key] = value_data
        
        return result
