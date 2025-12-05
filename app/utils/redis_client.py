# app/utils/redis_client.py

import os
import redis
import redis.asyncio as aioredis


def _build_kwargs_from_env():
    host = os.getenv("REDIS_HOST", "127.0.0.1")
    port = int(os.getenv("REDIS_PORT", "6379"))
    password = os.getenv("REDIS_PASSWORD")
    kwargs = {"host": host, "port": port, "decode_responses": True}
    if password:
        kwargs["password"] = password
    return kwargs


# Synchronous Redis client used by REST routes
redis_client = redis.Redis(**_build_kwargs_from_env())

# Async Redis client for new API key management
_async_redis_client = None

async def get_redis_client():
    """Get async Redis client (singleton pattern)."""
    global _async_redis_client
    
    # For development, always use in-memory storage
    # Uncomment the Redis code below for production
    return None
    
    # Redis connection code (commented out for development)
    # if _async_redis_client is None:
    #     try:
    #         _async_redis_client = aioredis.Redis(**_build_kwargs_from_env())
    #         # Test connection
    #         await _async_redis_client.ping()
    #     except Exception as e:
    #         print(f"⚠️ Redis connection failed: {e}")
    #         print("🔄 Using in-memory fallback for development")
    #         _async_redis_client = None
    # return _async_redis_client

async def close_redis_client():
    """Close async Redis client."""
    global _async_redis_client
    if _async_redis_client:
        await _async_redis_client.close()
        _async_redis_client = None
