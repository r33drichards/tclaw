from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from typing import Any, AsyncIterator

import redis.asyncio as aioredis

from tclaw.redis_client import get_redis

STREAM_MAXLEN = 5000


def _stream_key(session_id: str) -> str:
    return f"session:{session_id}:deltas"


async def publish_delta(session_id: str, text: str) -> None:
    r = get_redis()
    event = json.dumps({"type": "delta", "text": text})
    await r.xadd(
        _stream_key(session_id), {"data": event}, maxlen=STREAM_MAXLEN, approximate=True
    )


async def publish_thinking(session_id: str, text: str) -> None:
    r = get_redis()
    event = json.dumps({"type": "thinking", "text": text})
    await r.xadd(
        _stream_key(session_id), {"data": event}, maxlen=STREAM_MAXLEN, approximate=True
    )


async def publish_tool_call(session_id: str, name: str) -> None:
    r = get_redis()
    event = json.dumps({"type": "tool_call", "name": name})
    await r.xadd(
        _stream_key(session_id), {"data": event}, maxlen=STREAM_MAXLEN, approximate=True
    )


async def publish_turn_end(session_id: str) -> None:
    r = get_redis()
    event = json.dumps({"type": "turn_end"})
    await r.xadd(
        _stream_key(session_id), {"data": event}, maxlen=STREAM_MAXLEN, approximate=True
    )


@dataclass
class StreamEntry:
    id: str
    event: dict[str, Any]


async def subscribe_deltas(
    session_id: str,
    from_id: str,
    cancel: asyncio.Event,
) -> AsyncIterator[StreamEntry]:
    """Async generator that yields stream entries via XREAD BLOCK.

    Uses a dedicated Redis connection (blocking commands can't share the
    singleton).  Iteration ends when *cancel* is set.
    """
    url = os.environ.get("REDIS_URL", "redis://localhost:6379")
    conn = aioredis.from_url(url, decode_responses=True)
    cursor = from_id

    try:
        while not cancel.is_set():
            try:
                reply = await conn.xread(
                    {_stream_key(session_id): cursor}, block=5000, count=100
                )
            except Exception:
                if cancel.is_set():
                    return
                raise

            if not reply:
                continue

            for _stream_name, entries in reply:
                for entry_id, fields in entries:
                    cursor = entry_id
                    raw = fields.get("data")
                    if not raw:
                        continue
                    try:
                        event = json.loads(raw)
                    except (json.JSONDecodeError, TypeError):
                        continue
                    yield StreamEntry(id=entry_id, event=event)
    finally:
        await conn.aclose()
