from __future__ import annotations

import redis.asyncio as redis
from .config import settings


_redis: redis.Redis | None = None


def get_redis() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.from_url(settings.redis_url, encoding="utf-8", decode_responses=True)
    return _redis 