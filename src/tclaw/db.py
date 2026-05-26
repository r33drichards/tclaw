from __future__ import annotations

import os
from typing import Any

import asyncpg

from tclaw.types import Msg

_pool: asyncpg.Pool | None = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool
    dsn = os.environ.get("DATABASE_URL", "postgres://localhost:5432/chat")
    _pool = await asyncpg.create_pool(dsn=dsn)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS messages (
  id          BIGSERIAL PRIMARY KEY,
  session_id  TEXT        NOT NULL,
  role        TEXT        NOT NULL CHECK (role IN ('user','assistant')),
  content     TEXT        NOT NULL,
  user_id     TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS messages_session_id_idx ON messages (session_id, id);
CREATE INDEX IF NOT EXISTS messages_user_session_idx ON messages (user_id, session_id, id);

CREATE TABLE IF NOT EXISTS sessions (
  id          TEXT        PRIMARY KEY,
  user_id     TEXT        NOT NULL,
  title       TEXT,
  archived    BOOLEAN     NOT NULL DEFAULT FALSE,
  system_prompt TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions (user_id, updated_at DESC);
CREATE INDEX IF NOT EXISTS sessions_user_active_idx ON sessions (user_id, archived, updated_at DESC);

CREATE TABLE IF NOT EXISTS mcp_servers (
  id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id        TEXT        NOT NULL,
  name           TEXT        NOT NULL,
  url            TEXT,
  transport      TEXT        NOT NULL DEFAULT 'http',
  command        TEXT,
  args           JSONB       NOT NULL DEFAULT '[]'::jsonb,
  allowed_tools  JSONB       NOT NULL DEFAULT '[]'::jsonb,
  enabled        BOOLEAN     NOT NULL DEFAULT true,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, name)
);
CREATE INDEX IF NOT EXISTS mcp_servers_user_enabled_idx
  ON mcp_servers (user_id) WHERE enabled;

CREATE TABLE IF NOT EXISTS memories (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    TEXT        NOT NULL,
  tier       TEXT        NOT NULL CHECK (tier IN ('working','short_term','long_term')),
  key        TEXT        NOT NULL,
  content    TEXT        NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (user_id, tier, key)
);
CREATE INDEX IF NOT EXISTS memories_user_tier_idx ON memories (user_id, tier);
"""


async def ensure_schema() -> None:
    pool = await get_pool()
    await pool.execute(SCHEMA_SQL)


# ---------------------------------------------------------------------------
# Messages
# ---------------------------------------------------------------------------


async def append_message(
    session_id: str, role: str, content: str, user_id: str
) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO messages (session_id, role, content, user_id) VALUES ($1, $2, $3, $4)",
        session_id,
        role,
        content,
        user_id,
    )


async def get_messages(session_id: str, user_id: str | None = None) -> list[Msg]:
    pool = await get_pool()
    if user_id:
        rows = await pool.fetch(
            """SELECT m.role, m.content
                 FROM messages m
                 JOIN sessions s ON s.id = m.session_id
                WHERE m.session_id = $1 AND s.user_id = $2
                ORDER BY m.id ASC""",
            session_id,
            user_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT role, content FROM messages WHERE session_id = $1 ORDER BY id ASC",
            session_id,
        )
    return [Msg(role=r["role"], content=r["content"]) for r in rows]


# ---------------------------------------------------------------------------
# Sessions
# ---------------------------------------------------------------------------


async def get_sessions(
    user_id: str, *, include_archived: bool = False
) -> list[dict[str, Any]]:
    pool = await get_pool()
    if include_archived:
        rows = await pool.fetch(
            "SELECT id, title, updated_at FROM sessions WHERE user_id = $1 ORDER BY updated_at DESC",
            user_id,
        )
    else:
        rows = await pool.fetch(
            "SELECT id, title, updated_at FROM sessions WHERE user_id = $1 AND archived = FALSE ORDER BY updated_at DESC",
            user_id,
        )
    return [dict(r) for r in rows]


async def create_session(
    user_id: str,
    session_id: str,
    title: str | None = None,
    system_prompt: str | None = None,
) -> None:
    pool = await get_pool()
    await pool.execute(
        """INSERT INTO sessions (id, user_id, title, system_prompt)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (id) DO NOTHING""",
        session_id,
        user_id,
        title,
        system_prompt,
    )


async def session_belongs_to(session_id: str, user_id: str) -> bool:
    pool = await get_pool()
    row = await pool.fetchval(
        "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = $1 AND user_id = $2)",
        session_id,
        user_id,
    )
    return bool(row)


async def touch_session(session_id: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE sessions SET updated_at = now() WHERE id = $1", session_id
    )


async def rename_session(session_id: str, user_id: str, title: str) -> bool:
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE sessions SET title = $3, updated_at = now() WHERE id = $1 AND user_id = $2",
        session_id,
        user_id,
        title,
    )
    return result != "UPDATE 0"


async def set_session_archived(
    session_id: str, user_id: str, archived: bool
) -> bool:
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE sessions SET archived = $3, updated_at = now() WHERE id = $1 AND user_id = $2",
        session_id,
        user_id,
        archived,
    )
    return result != "UPDATE 0"


async def delete_session(session_id: str, user_id: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            owns = await conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM sessions WHERE id = $1 AND user_id = $2)",
                session_id,
                user_id,
            )
            if not owns:
                return False
            await conn.execute(
                "DELETE FROM messages WHERE session_id = $1", session_id
            )
            await conn.execute(
                "DELETE FROM sessions WHERE id = $1 AND user_id = $2",
                session_id,
                user_id,
            )
            return True


# ---------------------------------------------------------------------------
# MCP servers
# ---------------------------------------------------------------------------

MCP_COLS = "id, user_id, name, url, transport, command, args, allowed_tools, enabled, created_at, updated_at"


async def list_mcp_servers(user_id: str) -> list[dict[str, Any]]:
    pool = await get_pool()
    rows = await pool.fetch(
        f"SELECT {MCP_COLS} FROM mcp_servers WHERE user_id = $1 ORDER BY created_at ASC",
        user_id,
    )
    return [dict(r) for r in rows]


async def list_enabled_mcp_servers(user_id: str) -> list[dict[str, Any]]:
    pool = await get_pool()
    rows = await pool.fetch(
        f"SELECT {MCP_COLS} FROM mcp_servers WHERE user_id = $1 AND enabled ORDER BY created_at ASC",
        user_id,
    )
    return [dict(r) for r in rows]


class McpNameTakenError(Exception):
    pass


async def create_mcp_server(
    user_id: str,
    *,
    name: str,
    url: str | None = None,
    transport: str = "http",
    command: str | None = None,
    args: list[str] | None = None,
    allowed_tools: list[str] | None = None,
    enabled: bool = True,
) -> dict[str, Any]:
    pool = await get_pool()
    import json

    try:
        row = await pool.fetchrow(
            f"""INSERT INTO mcp_servers (user_id, name, url, transport, command, args, allowed_tools, enabled)
               VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8)
               RETURNING {MCP_COLS}""",
            user_id,
            name,
            url,
            transport,
            command,
            json.dumps(args or []),
            json.dumps(allowed_tools or []),
            enabled,
        )
        return dict(row)  # type: ignore[arg-type]
    except asyncpg.UniqueViolationError:
        raise McpNameTakenError(f"mcp server name already exists: {name}")


async def delete_mcp_server(server_id: str, user_id: str) -> bool:
    pool = await get_pool()
    result = await pool.execute(
        "DELETE FROM mcp_servers WHERE id = $1::uuid AND user_id = $2",
        server_id,
        user_id,
    )
    return result != "DELETE 0"


# ---------------------------------------------------------------------------
# Memories
# ---------------------------------------------------------------------------


async def set_memory(
    user_id: str, tier: str, key: str, content: str
) -> dict[str, Any]:
    pool = await get_pool()
    row = await pool.fetchrow(
        """INSERT INTO memories (user_id, tier, key, content)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (user_id, tier, key)
           DO UPDATE SET content = $4, updated_at = now()
           RETURNING id, user_id, tier, key, content, created_at, updated_at""",
        user_id,
        tier,
        key,
        content,
    )
    return dict(row)  # type: ignore[arg-type]


async def get_memory(user_id: str, key: str) -> dict[str, Any] | None:
    pool = await get_pool()
    row = await pool.fetchrow(
        "SELECT id, user_id, tier, key, content, created_at, updated_at FROM memories WHERE user_id = $1 AND key = $2",
        user_id,
        key,
    )
    return dict(row) if row else None


async def delete_memory(user_id: str, key: str) -> bool:
    pool = await get_pool()
    result = await pool.execute(
        "DELETE FROM memories WHERE user_id = $1 AND key = $2", user_id, key
    )
    return result != "DELETE 0"


async def list_memories(
    user_id: str, tier: str | None = None
) -> list[dict[str, Any]]:
    pool = await get_pool()
    if tier:
        rows = await pool.fetch(
            "SELECT id, user_id, tier, key, content, created_at, updated_at FROM memories WHERE user_id = $1 AND tier = $2 ORDER BY key",
            user_id,
            tier,
        )
    else:
        rows = await pool.fetch(
            "SELECT id, user_id, tier, key, content, created_at, updated_at FROM memories WHERE user_id = $1 ORDER BY tier, key",
            user_id,
        )
    return [dict(r) for r in rows]


async def search_memories(
    user_id: str, query: str, tier: str | None = None
) -> list[dict[str, Any]]:
    pattern = f"%{query}%"
    pool = await get_pool()
    if tier:
        rows = await pool.fetch(
            """SELECT id, user_id, tier, key, content, created_at, updated_at FROM memories
              WHERE user_id = $1 AND tier = $2 AND (key ILIKE $3 OR content ILIKE $3)
              ORDER BY updated_at DESC LIMIT 20""",
            user_id,
            tier,
            pattern,
        )
    else:
        rows = await pool.fetch(
            """SELECT id, user_id, tier, key, content, created_at, updated_at FROM memories
              WHERE user_id = $1 AND (key ILIKE $2 OR content ILIKE $2)
              ORDER BY updated_at DESC LIMIT 20""",
            user_id,
            pattern,
        )
    return [dict(r) for r in rows]


async def load_memory_context(user_id: str) -> str:
    working = await list_memories(user_id, "working")
    short_term = await list_memories(user_id, "short_term")

    parts: list[str] = []

    if working:
        parts.append("[Working Memory]")
        for m in working:
            parts.append(f"{m['key']}: {m['content']}")

    if short_term:
        parts.append("")
        parts.append("[Short-term Memory — use memory_get to read full content]")
        for m in short_term:
            preview = m["content"][:120] + "..." if len(m["content"]) > 120 else m["content"]
            parts.append(f"{m['key']}: {preview}")

    return "\n".join(parts)
