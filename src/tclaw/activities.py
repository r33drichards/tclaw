from __future__ import annotations

import logging
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

logger = logging.getLogger(__name__)

MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o")
TITLE_MODEL = os.environ.get("OPENAI_TITLE_MODEL", "gpt-4o-mini")

# Disable OpenAI Agents SDK tracing (fails with 403 on ZDR orgs)
os.environ.setdefault("OPENAI_AGENTS_DISABLE_TRACING", "1")


@activity.defn
async def stream_agent_turn(req: StreamReq) -> AgentTurnResult:
    """Run one assistant turn using the OpenAI Agents SDK.

    Creates an Agent with memory tools, streams the response, publishes
    deltas to Redis, heartbeats on each chunk, and returns the final text.
    """
    logger.info("stream_agent_turn starting for %s", req.session_id)
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

    import asyncio

    activity.heartbeat("running agent")

    last_text = ""
    try:
        agent = Agent(
            name="tclaw",
            instructions="\n\n".join(system_parts),
            model=MODEL,
            tools=memory_tools,
        )

        result = Runner.run_streamed(
            agent,
            input=last_user_msg,
        )

        async def _consume_stream() -> str:
            async for event in result.stream_events():
                activity.heartbeat()

                if event.type == "raw_response_event":
                    if isinstance(event.data, ResponseTextDeltaEvent):
                        if event.data.delta:
                            await publish_delta(req.session_id, event.data.delta)
                elif event.type == "run_item_stream_event":
                    if hasattr(event, "item") and getattr(event.item, "type", None) == "tool_call_item":
                        tool_name = getattr(event.item, "name", None) or "unknown"
                        await publish_tool_call(req.session_id, tool_name)

            return result.final_output or ""

        last_text = await asyncio.wait_for(_consume_stream(), timeout=120)
        logger.info("stream_agent_turn completed for %s", req.session_id)

    except asyncio.TimeoutError:
        logger.error("stream_agent_turn timed out for %s", req.session_id)
        last_text = result.final_output or "(response timed out)"
    except Exception as exc:
        logger.error("Agent turn failed: %s", exc, exc_info=True)
        raise
    finally:
        await publish_turn_end(req.session_id)

    return AgentTurnResult(text=last_text)


@activity.defn
async def persist_turn(req: PersistTurnReq) -> None:
    """Write a message to Postgres and touch the session timestamp."""
    logger.info("persist_turn: %s %s", req.session_id, req.role)
    await db.append_message(req.session_id, req.role, req.content, req.user_id)
    await db.touch_session(req.session_id)
    logger.info("persist_turn done: %s", req.session_id)


@activity.defn
async def generate_title(req: GenerateTitleReq) -> None:
    """Generate a short title for the session from the first user message."""
    try:
        import asyncio

        logger.info("generate_title starting for %s", req.session_id)
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
        result = await asyncio.wait_for(
            Runner.run(agent, input=req.user_message),
            timeout=30,
        )
        title = result.final_output or ""
        title = re.sub(r'^["\'"`]+|["\'"`]+$', "", title)
        title = title.rstrip(".").strip()[:80]
        logger.info("generate_title done for %s: %s", req.session_id, title)
        if not title:
            return
        await db.rename_session(req.session_id, req.user_id, title)
    except Exception as exc:
        logger.warning("generate_title failed for %s: %s", req.session_id, exc)
