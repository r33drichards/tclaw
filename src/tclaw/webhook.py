from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import Depends, FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from temporalio.client import Client

from tclaw import db
from tclaw.publish import subscribe_deltas
from tclaw.workflows import ChatSession

_temporal_client: Client | None = None


async def _get_temporal_client() -> Client:
    global _temporal_client
    if _temporal_client is None:
        address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
        namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
        _temporal_client = await Client.connect(address, namespace=namespace)
    return _temporal_client


TASK_QUEUE = os.environ.get("TASK_QUEUE", "chat")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    await db.ensure_schema()
    yield
    await db.close_pool()


app = FastAPI(lifespan=lifespan)


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------


async def require_user_id(x_user_id: str | None = Header(None)) -> str:
    if not x_user_id:
        raise HTTPException(status_code=401, detail="X-User-ID required")
    return x_user_id


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


class MessageBody(BaseModel):
    sessionId: str
    msg: str


@app.post("/message")
async def post_message(
    body: MessageBody,
    user_id: str = Depends(require_user_id),
) -> JSONResponse:
    if not body.sessionId or not body.msg:
        raise HTTPException(400, "sessionId and msg required")

    # Ensure session exists for this user
    exists = await db.session_belongs_to(body.sessionId, user_id)
    if not exists:
        await db.create_session(user_id, body.sessionId)
        if not await db.session_belongs_to(body.sessionId, user_id):
            raise HTTPException(403, "session belongs to another user")

    client = await _get_temporal_client()
    workflow_id = f"chat:{body.sessionId}"

    # Signal-with-start: start workflow if not running, then signal
    await client.start_workflow(
        ChatSession.run,
        args=[body.sessionId, None, user_id],
        id=workflow_id,
        task_queue=TASK_QUEUE,
        start_signal="user_message",
        start_signal_args=[body.msg],
    )

    return JSONResponse({"ok": True})


@app.get("/sessions")
async def list_sessions(
    user_id: str = Depends(require_user_id),
) -> JSONResponse:
    sessions = await db.get_sessions(user_id)
    # Serialize datetimes
    for s in sessions:
        if "updated_at" in s:
            s["updated_at"] = str(s["updated_at"])
    return JSONResponse({"sessions": sessions})


@app.get("/sessions/{session_id}/messages")
async def get_messages(
    session_id: str,
    user_id: str = Depends(require_user_id),
) -> JSONResponse:
    if not await db.session_belongs_to(session_id, user_id):
        raise HTTPException(404, "not found")
    messages = await db.get_messages(session_id, user_id)
    return JSONResponse(
        {
            "sessionId": session_id,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
        }
    )


class PatchSessionBody(BaseModel):
    title: str | None = None
    archived: bool | None = None


@app.patch("/sessions/{session_id}")
async def patch_session(
    session_id: str,
    body: PatchSessionBody,
    user_id: str = Depends(require_user_id),
) -> JSONResponse:
    if body.title is None and body.archived is None:
        raise HTTPException(400, "title or archived required")

    if body.title is not None:
        ok = await db.rename_session(session_id, user_id, body.title)
        if not ok:
            raise HTTPException(404, "not found")
    if body.archived is not None:
        ok = await db.set_session_archived(session_id, user_id, body.archived)
        if not ok:
            raise HTTPException(404, "not found")
    return JSONResponse({"ok": True})


@app.delete("/sessions/{session_id}")
async def delete_session(
    session_id: str,
    user_id: str = Depends(require_user_id),
) -> JSONResponse:
    if not await db.session_belongs_to(session_id, user_id):
        raise HTTPException(404, "not found")

    # Try to close the workflow
    try:
        client = await _get_temporal_client()
        handle = client.get_workflow_handle(f"chat:{session_id}")
        await handle.signal(ChatSession.close)
    except Exception:
        pass

    ok = await db.delete_session(session_id, user_id)
    if not ok:
        raise HTTPException(404, "not found")
    return JSONResponse({"ok": True})


@app.get("/sessions/{session_id}/stream")
async def stream_session(
    session_id: str,
    request: Request,
    user_id: str = Depends(require_user_id),
    from_id: str | None = None,
) -> EventSourceResponse:
    if not await db.session_belongs_to(session_id, user_id):
        raise HTTPException(404, "not found")

    last_event_id = request.headers.get("Last-Event-ID")
    cursor = last_event_id or from_id or "$"

    async def event_generator() -> AsyncIterator[dict[str, str]]:
        cancel = asyncio.Event()

        # Watch for client disconnect
        async def watch_disconnect() -> None:
            while not await request.is_disconnected():
                await asyncio.sleep(0.5)
            cancel.set()

        watcher = asyncio.create_task(watch_disconnect())

        try:
            async for entry in subscribe_deltas(session_id, cursor, cancel):
                import json

                yield {"id": entry.id, "data": json.dumps(entry.event)}
        finally:
            cancel.set()
            watcher.cancel()

    return EventSourceResponse(event_generator())


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------


def main() -> None:
    import uvicorn

    port = int(os.environ.get("PORT", os.environ.get("WEBHOOK_PORT", "8787")))
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
