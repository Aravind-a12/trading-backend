# app/utils/redis_client.py

import os
import redis


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
