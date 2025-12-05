"""
In-memory storage fallback for development when Redis is not available.
This allows testing the API without Redis setup.
"""

import json
from typing import Dict, Any, List, Optional
from datetime import datetime

# In-memory storage
_memory_storage = {
    "users": {},
    "api_keys": {},
    "user_api_keys": {},
    "exchange_keys": {}
}

class MemoryStorage:
    """In-memory storage for development."""
    
    def __init__(self):
        self._storage = _memory_storage
    
    async def set(self, key: str, value: str) -> bool:
        """Set a value in memory storage."""
        try:
            self._storage[key] = value
            return True
        except Exception:
            return False
    
    async def get(self, key: str) -> Optional[str]:
        """Get a value from memory storage."""
        return self._storage.get(key)
    
    async def delete(self, key: str) -> bool:
        """Delete a value from memory storage."""
        try:
            if key in self._storage:
                del self._storage[key]
            return True
        except Exception:
            return False
    
    async def sadd(self, key: str, *values) -> int:
        """Add values to a set."""
        if key not in self._storage:
            self._storage[key] = set()
        elif not isinstance(self._storage[key], set):
            self._storage[key] = set()
        
        for value in values:
            self._storage[key].add(value)
        return len(self._storage[key])
    
    async def smembers(self, key: str) -> set:
        """Get all members of a set."""
        return self._storage.get(key, set())
    
    async def srem(self, key: str, *values) -> int:
        """Remove values from a set."""
        if key not in self._storage:
            return 0
        
        if not isinstance(self._storage[key], set):
            return 0
        
        removed = 0
        for value in values:
            if value in self._storage[key]:
                self._storage[key].remove(value)
                removed += 1
        
        return removed
    
    async def sismember(self, key: str, value: str) -> bool:
        """Check if value is in set."""
        if key not in self._storage:
            return False
        
        if not isinstance(self._storage[key], set):
            return False
        
        return value in self._storage[key]

# Global memory storage instance (singleton)
_memory_storage_instance = None

async def get_storage_client():
    """Get storage client (Redis or memory fallback)."""
    global _memory_storage_instance
    from app.utils.redis_client import get_redis_client
    
    redis_client = await get_redis_client()
    if redis_client:
        return redis_client
    else:
        if _memory_storage_instance is None:
            _memory_storage_instance = MemoryStorage()
        return _memory_storage_instance
