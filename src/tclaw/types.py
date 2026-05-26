from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Msg:
    role: str  # "user" | "assistant"
    content: str


@dataclass
class StreamReq:
    session_id: str
    history: list[Msg]
    user_id: str
    system_prompt: str | None = None


@dataclass
class AgentTurnResult:
    text: str


@dataclass
class PersistTurnReq:
    session_id: str
    role: str
    content: str
    user_id: str


@dataclass
class GenerateTitleReq:
    session_id: str
    user_message: str
    user_id: str
