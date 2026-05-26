from __future__ import annotations

import os
import re

from agents import Agent, Runner
from agents.mcp import MCPServerStdio
from openai.types.responses import ResponseTextDeltaEvent
from temporalio import activity

from tclaw import db
from tclaw.memory_tools import _make_memory_tools
from tclaw.publish import (
    publish_delta,
    publish_tool_call,
    publish_turn_end,
)
from tclaw.types import AgentTurnResult, GenerateTitleReq, PersistTurnReq, StreamReq

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
TITLE_MODEL = os.environ.get("OPENAI_TITLE_MODEL", "gpt-4o-mini")


@activity.defn
async def stream_agent_turn(req: StreamReq) -> AgentTurnResult:
    """Run one assistant turn using the OpenAI Agents SDK.

    Creates an Agent with memory tools, streams the response, publishes
    deltas to Redis, heartbeats on each chunk, and returns the final text.
    """
    memory_ctx = await db.load_memory_context(req.user_id)
    memory_tools = _make_memory_tools(req.user_id)

    system_parts: list[str] = [
        "You are a helpful assistant.",
        "You can manage your own memory using the memory_set/get/delete/list/search tools.",
        "You can add and remove MCP tool servers using mcp_add, mcp_list, mcp_remove.",
    ]
    if req.system_prompt:
        system_parts.insert(0, req.system_prompt)
    if memory_ctx:
        system_parts.append(memory_ctx)

    # Build conversation input: the history formatted for the agent, plus
    # the last user message as the primary input.
    last_user_msg = ""
    for m in reversed(req.history):
        if m.role == "user":
            last_user_msg = m.content
            break

    runno = MCPServerStdio(
        params={"command": "npx", "args": ["-y", "@runno/mcp"]},
        cache_tools_list=True,
    )

    last_text = ""
    try:
        async with runno:
            agent = Agent(
                name="tclaw",
                instructions="\n\n".join(system_parts),
                model=MODEL,
                tools=memory_tools,
                mcp_servers=[runno],
            )

            result = Runner.run_streamed(
                agent,
                input=last_user_msg,
            )

            async for event in result.stream_events():
                activity.heartbeat()

                if event.type == "raw_response_event":
                    # Text deltas for streaming to the client
                    if isinstance(event.data, ResponseTextDeltaEvent):
                        if event.data.delta:
                            await publish_delta(req.session_id, event.data.delta)
                elif event.type == "run_item_stream_event":
                    # Tool call notifications
                    if hasattr(event, "item") and getattr(event.item, "type", None) == "tool_call_item":
                        tool_name = getattr(event.item, "name", None) or "unknown"
                        await publish_tool_call(req.session_id, tool_name)

            last_text = result.final_output or ""

    finally:
        await publish_turn_end(req.session_id)

    return AgentTurnResult(text=last_text)


@activity.defn
async def persist_turn(req: PersistTurnReq) -> None:
    """Write a message to Postgres and touch the session timestamp."""
    await db.append_message(req.session_id, req.role, req.content, req.user_id)
    await db.touch_session(req.session_id)


@activity.defn
async def generate_title(req: GenerateTitleReq) -> None:
    """Generate a short title for the session from the first user message."""
    try:
        agent = Agent(
            name="title-generator",
            instructions=(
                "Summarise the following message as a concise 3-6 word chat "
                "title. Reply with ONLY the title text, no quotes, no "
                "punctuation, no leading 'Title:'."
            ),
            model=TITLE_MODEL,
            tools=[],
        )
        result = await Runner.run(agent, input=req.user_message)
        title = result.final_output or ""
        title = re.sub(r'^["\'"`]+|["\'"`]+$', "", title)
        title = title.rstrip(".").strip()[:80]
        if not title:
            return
        await db.rename_session(req.session_id, req.user_id, title)
    except Exception:
        pass  # best-effort
