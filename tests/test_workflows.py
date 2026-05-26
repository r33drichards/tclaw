"""Integration tests for the ChatSession workflow.

Uses Temporal's Python test environment with mocked activities.
"""
from __future__ import annotations

import asyncio
import uuid

import pytest
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from tclaw.types import AgentTurnResult, GenerateTitleReq, Msg, PersistTurnReq, StreamReq
from tclaw.workflows import ChatSession


# ---------------------------------------------------------------------------
# Fake activities
# ---------------------------------------------------------------------------


async def fake_stream_agent_turn(req: StreamReq) -> AgentTurnResult:
    """Echo the last user message."""
    last = req.history[-1] if req.history else Msg(role="user", content="")
    return AgentTurnResult(text=f"echo: {last.content}")


async def fake_persist_turn(req: PersistTurnReq) -> None:
    pass


async def fake_generate_title(req: GenerateTitleReq) -> None:
    pass


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.fixture
async def env() -> WorkflowEnvironment:
    async with await WorkflowEnvironment.start_local() as env:
        yield env


@pytest.mark.asyncio
async def test_single_message_and_close(env: WorkflowEnvironment) -> None:
    task_queue = f"test-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[ChatSession],
        activities=[fake_stream_agent_turn, fake_persist_turn, fake_generate_title],
    ):
        session_id = str(uuid.uuid4())
        handle = await env.client.start_workflow(
            ChatSession.run,
            args=[session_id, None, "test-user"],
            id=f"chat:{session_id}",
            task_queue=task_queue,
        )

        # Send a message
        await handle.signal(ChatSession.user_message, "hello")

        # Poll transcript until assistant reply appears
        transcript: list[Msg] = []
        for _ in range(50):
            transcript = await handle.query(ChatSession.transcript)
            if any(m.role == "assistant" for m in transcript):
                break
            await asyncio.sleep(0.1)

        assert transcript == [
            Msg(role="user", content="hello"),
            Msg(role="assistant", content="echo: hello"),
        ]

        # Close
        await handle.signal(ChatSession.close)
        await handle.result()


@pytest.mark.asyncio
async def test_message_coalescing(env: WorkflowEnvironment) -> None:
    """Messages queued during an in-flight generation merge into one turn."""
    release_first: asyncio.Event = asyncio.Event()
    call_count = 0

    async def slow_agent(req: StreamReq) -> AgentTurnResult:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            await release_first.wait()
            return AgentTurnResult(text="first-reply")
        last = req.history[-1] if req.history else Msg(role="user", content="")
        return AgentTurnResult(text=f"reply-to: {last.content}")

    task_queue = f"test-{uuid.uuid4()}"

    async with Worker(
        env.client,
        task_queue=task_queue,
        workflows=[ChatSession],
        activities=[slow_agent, fake_persist_turn, fake_generate_title],
    ):
        session_id = str(uuid.uuid4())
        handle = await env.client.start_workflow(
            ChatSession.run,
            args=[session_id, None, "test-user"],
            id=f"chat:{session_id}",
            task_queue=task_queue,
        )

        # Send first message
        await handle.signal(ChatSession.user_message, "msg-1")
        await asyncio.sleep(0.3)

        # Queue two more while first generation is in-flight
        await handle.signal(ChatSession.user_message, "msg-2")
        await handle.signal(ChatSession.user_message, "msg-3")

        # Release the first generation
        release_first.set()

        # Wait for second generation
        transcript: list[Msg] = []
        for _ in range(100):
            transcript = await handle.query(ChatSession.transcript)
            if sum(1 for m in transcript if m.role == "assistant") >= 2:
                break
            await asyncio.sleep(0.1)

        assert transcript == [
            Msg(role="user", content="msg-1"),
            Msg(role="assistant", content="first-reply"),
            Msg(role="user", content="msg-2\n\nmsg-3"),
            Msg(role="assistant", content="reply-to: msg-2\n\nmsg-3"),
        ]

        await handle.signal(ChatSession.close)
        await handle.result()
