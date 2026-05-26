from __future__ import annotations

import os

import redis.asyncio as aioredis

_client: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _client
    if _client is not None:
        return _client
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    _client = aioredis.from_url(url, decode_responses=True)
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
