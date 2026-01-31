"""
Redis store for Orchestrator service.
"""

import redis
import os


class RedisStore:
    """Redis client wrapper for Orchestrator service"""
    
    def __init__(self, redis_url: str = None):
        url = redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
        self.client = redis.Redis.from_url(url, decode_responses=False)
    
    def get_client(self):
        return self.client
