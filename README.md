# tclaw

Durable chat agent powered by the [OpenAI Agents SDK](https://github.com/openai/openai-agents-python) and [Temporal](https://temporal.io) workflows. One workflow per session, webhook for message delivery.

A Python rewrite of [ted](https://github.com/r33drichards/ted), replacing the Claude Agent SDK with the OpenAI Agents SDK.

## Prereqs

- Python 3.11+
- `temporal` CLI (`brew install temporal`)
- Redis 7+
- Postgres 14+
- `OPENAI_API_KEY` set

## Run locally

Three terminals:

```bash
# Terminal 1 — Temporal dev server
temporal server start-dev

# Terminal 2 — worker
export OPENAI_API_KEY=sk-...
python -m tclaw.worker

# Terminal 3 — webhook
python -m tclaw.webhook
```

## Smoke test

```bash
# Send a message — creates the session workflow
curl -X POST http://localhost:8787/message \
  -H 'content-type: application/json' \
  -H 'X-User-ID: test-user' \
  -d '{"sessionId":"test-1","msg":"hello, who are you?"}'

# Stream responses
curl -N http://localhost:8787/sessions/test-1/stream \
  -H 'X-User-ID: test-user'

# Fetch committed history
curl http://localhost:8787/sessions/test-1/messages \
  -H 'X-User-ID: test-user'
```

## HTTP API

- `POST /message` — `{ sessionId, msg }` + `X-User-ID` header. Delivers the message to the session workflow (starting it if absent).
- `GET /sessions` — List user's sessions.
- `GET /sessions/:id/messages` — Committed turn history from Postgres.
- `GET /sessions/:id/stream` — SSE of live generation deltas. Supports `Last-Event-ID` / `?from=`.
- `PATCH /sessions/:id` — `{ title?, archived? }` to rename or archive.
- `DELETE /sessions/:id` — Delete session and signal workflow close.

## Tests

```bash
pip install -e ".[dev]"
pytest
```

## Architecture

- `src/tclaw/workflows.py` — `ChatSession` workflow. Long-running loop with signal-based inbox.
- `src/tclaw/inbox.py` — `drain_inbox` helper (coalesces queued messages into one user turn).
- `src/tclaw/activities.py` — `stream_agent_turn` (OpenAI Agents SDK), `persist_turn`, `generate_title`.
- `src/tclaw/memory_tools.py` — Agent function tools for memory CRUD and MCP server management.
- `src/tclaw/publish.py` — Redis Streams fan-out of streaming deltas.
- `src/tclaw/db.py` — Postgres pool + schema (messages, sessions, mcp_servers, memories).
- `src/tclaw/webhook.py` — FastAPI HTTP API.
- `src/tclaw/worker.py` — Temporal worker bootstrap.
- `src/tclaw/irc_bridge.py` — IRC bridge (separate service).
