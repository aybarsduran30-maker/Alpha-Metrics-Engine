import os
import json
import redis.asyncio as aioredis
from typing import Optional, Any

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

redis_pool = aioredis.ConnectionPool.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=20
)

async def get_redis_client() -> aioredis.Redis:
    return aioredis.Redis(connection_pool=redis_pool)

async def get_cached_metrics(key: str) -> Optional[Any]:
    client = await get_redis_client()
    data = await client.get(key)
    if data:
        return json.loads(data)
    return None

async def set_cached_metrics(key: str, value: Any, ttl: int = 300) -> None:
    client = await get_redis_client()
    await client.set(key, json.dumps(value), ex=ttl)

async def check_rate_limit(client_id: str, limit: int = 60, window: int = 60) -> bool:
    client = await get_redis_client()
    key = f"rate_limit:{client_id}"
    current_count = await client.incr(key)
    if current_count == 1:
        await client.expire(key, window)
    return current_count <= limit
