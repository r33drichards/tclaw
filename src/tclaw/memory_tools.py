"""Function tools for agent memory and MCP server management.

These are passed to the OpenAI Agent as tools so it can manage its own
memory and tool-server configuration.  They run inside the activity
context (not the workflow), so they can do I/O directly.
"""
from __future__ import annotations

from agents import FunctionTool, RunContextWrapper
from typing import Any

from tclaw import db


def _make_memory_tools(user_id: str) -> list[FunctionTool]:
    """Build a set of memory function-tools bound to *user_id*."""

    async def memory_set(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        import json

        args = json.loads(args_json)
        tier = args["tier"]
        key = args["key"]
        content = args["content"]
        await db.set_memory(user_id, tier, key, content)
        return f'Memory "{key}" saved to {tier}.'

    async def memory_get(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        import json

        args = json.loads(args_json)
        mem = await db.get_memory(user_id, args["key"])
        if not mem:
            return f'No memory found with key "{args["key"]}".'
        return f"[{mem['tier']}] {mem['key']}:\n{mem['content']}"

    async def memory_delete(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        import json

        args = json.loads(args_json)
        ok = await db.delete_memory(user_id, args["key"])
        return f'Deleted "{args["key"]}".' if ok else f'No memory "{args["key"]}".'

    async def memory_list(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        import json

        args = json.loads(args_json)
        tier = args.get("tier")
        mems = await db.list_memories(user_id, tier)
        if not mems:
            return "No memories found."
        lines = []
        for m in mems:
            preview = m["content"][:80] + "..." if len(m["content"]) > 80 else m["content"]
            lines.append(f"[{m['tier']}] {m['key']}: {preview}")
        return "\n".join(lines)

    async def memory_search(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        import json

        args = json.loads(args_json)
        results = await db.search_memories(user_id, args["query"], args.get("tier"))
        if not results:
            return f'No memories matching "{args["query"]}".'
        lines = []
        for m in results:
            preview = m["content"][:80] + "..." if len(m["content"]) > 80 else m["content"]
            lines.append(f"[{m['tier']}] {m['key']}: {preview}")
        return "\n".join(lines)

    async def mcp_add(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        import json

        args = json.loads(args_json)
        name = args["name"]
        url = args.get("url")
        command = args.get("command")
        cmd_args = args.get("args", [])
        if not url and not command:
            return "Provide either url (HTTP) or command (stdio)."
        transport = "stdio" if command else "http"
        try:
            await db.create_mcp_server(
                user_id,
                name=name,
                url=url,
                transport=transport,
                command=command,
                args=cmd_args,
            )
        except db.McpNameTakenError:
            return f'Server "{name}" already exists.'
        label = f"{command} {' '.join(cmd_args)}" if command else url
        return f'Added MCP server "{name}" ({transport}: {label}). It will be available on the next turn.'

    async def mcp_list_fn(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        servers = await db.list_mcp_servers(user_id)
        if not servers:
            return "No MCP servers configured."
        lines = []
        for s in servers:
            if s["transport"] == "stdio" and s.get("command"):
                label = f"{s['command']} {' '.join(s.get('args') or [])}"
            else:
                label = s.get("url") or "unknown"
            status = "enabled" if s["enabled"] else "disabled"
            lines.append(f"{s['name']} — {s['transport']}: {label} ({status})")
        return "\n".join(lines)

    async def mcp_remove(
        ctx: RunContextWrapper[Any], args_json: str
    ) -> str:
        import json

        args = json.loads(args_json)
        servers = await db.list_mcp_servers(user_id)
        target = next((s for s in servers if s["name"] == args["name"]), None)
        if not target:
            return f'No server named "{args["name"]}".'
        await db.delete_mcp_server(str(target["id"]), user_id)
        return f'Removed "{args["name"]}".'

    return [
        FunctionTool(
            name="memory_set",
            description="Create or update a memory. working = always in context, short_term = index in context, long_term = searchable.",
            params_json_schema={
                "type": "object",
                "properties": {
                    "tier": {
                        "type": "string",
                        "enum": ["working", "short_term", "long_term"],
                    },
                    "key": {"type": "string"},
                    "content": {"type": "string"},
                },
                "required": ["tier", "key", "content"],
                "additionalProperties": False,
            },
            on_invoke_tool=memory_set,
            strict_json_schema=True,
        ),
        FunctionTool(
            name="memory_get",
            description="Read the full content of a memory by key.",
            params_json_schema={
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
                "additionalProperties": False,
            },
            on_invoke_tool=memory_get,
            strict_json_schema=True,
        ),
        FunctionTool(
            name="memory_delete",
            description="Delete a memory by key.",
            params_json_schema={
                "type": "object",
                "properties": {"key": {"type": "string"}},
                "required": ["key"],
                "additionalProperties": False,
            },
            on_invoke_tool=memory_delete,
            strict_json_schema=True,
        ),
        FunctionTool(
            name="memory_list",
            description="List all memories, optionally filtered by tier.",
            params_json_schema={
                "type": "object",
                "properties": {
                    "tier": {
                        "type": "string",
                        "enum": ["working", "short_term", "long_term"],
                    },
                },
                "required": [],
                "additionalProperties": False,
            },
            on_invoke_tool=memory_list,
            strict_json_schema=False,
        ),
        FunctionTool(
            name="memory_search",
            description="Search memories by keyword across keys and content.",
            params_json_schema={
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "tier": {
                        "type": "string",
                        "enum": ["working", "short_term", "long_term"],
                    },
                },
                "required": ["query"],
                "additionalProperties": False,
            },
            on_invoke_tool=memory_search,
            strict_json_schema=False,
        ),
        FunctionTool(
            name="mcp_add",
            description="Add an MCP tool server. For HTTP provide url; for stdio provide command and args. Available next turn.",
            params_json_schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Short identifier"},
                    "url": {"type": "string", "description": "HTTP(S) URL"},
                    "command": {"type": "string", "description": "Command for stdio"},
                    "args": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Args for stdio command",
                    },
                },
                "required": ["name"],
                "additionalProperties": False,
            },
            on_invoke_tool=mcp_add,
            strict_json_schema=False,
        ),
        FunctionTool(
            name="mcp_list",
            description="List all configured MCP tool servers.",
            params_json_schema={
                "type": "object",
                "properties": {},
                "required": [],
                "additionalProperties": False,
            },
            on_invoke_tool=mcp_list_fn,
            strict_json_schema=False,
        ),
        FunctionTool(
            name="mcp_remove",
            description="Remove an MCP tool server by name.",
            params_json_schema={
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
                "additionalProperties": False,
            },
            on_invoke_tool=mcp_remove,
            strict_json_schema=True,
        ),
    ]
