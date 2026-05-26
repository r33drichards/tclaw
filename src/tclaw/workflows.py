from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from tclaw.inbox import drain_inbox
    from tclaw.types import (
        AgentTurnResult,
        GenerateTitleReq,
        Msg,
        PersistTurnReq,
        StreamReq,
    )

@workflow.defn
class ChatSession:
    """Long-running chat session — one per user session.

    Receives messages via signals, coalesces them into user turns, invokes
    the agent activity, persists results, and auto-generates a title on
    the first turn.
    """

    def __init__(self) -> None:
        self._inbox: list[str] = []
        self._history: list[Msg] = []
        self._closed = False
        self._title_generated = False
        self._session_id = ""
        self._user_id = ""

    # -- signals & queries --

    @workflow.signal
    async def user_message(self, msg: str) -> None:
        self._inbox.append(msg)

    @workflow.signal
    async def close(self) -> None:
        self._closed = True

    @workflow.query
    def transcript(self) -> list[Msg]:
        return list(self._history)

    # -- main loop --

    @workflow.run
    async def run(
        self,
        session_id: str,
        seed_history: list[Msg] | None = None,
        user_id: str = "",
    ) -> None:
        self._session_id = session_id
        self._user_id = user_id
        self._history = list(seed_history or [])
        self._title_generated = bool(self._history)

        retry = RetryPolicy(maximum_attempts=3)

        while not self._closed:
            await workflow.wait_condition(
                lambda: len(self._inbox) > 0 or self._closed
            )
            if self._closed:
                break

            user_turn = drain_inbox(self._inbox, self._history)

            # Persist the user turn
            if user_turn is not None:
                await workflow.execute_activity(
                    "persist_turn",
                    PersistTurnReq(
                        session_id=session_id,
                        role="user",
                        content=user_turn,
                        user_id=user_id,
                    ),
                    start_to_close_timeout=timedelta(seconds=30),
                    retry_policy=retry,
                )

            # Run the agent
            result = await workflow.execute_activity(
                "stream_agent_turn",
                StreamReq(
                    session_id=session_id,
                    history=list(self._history),
                    user_id=user_id,
                ),
                result_type=AgentTurnResult,
                start_to_close_timeout=timedelta(minutes=10),
                heartbeat_timeout=timedelta(seconds=60),
                retry_policy=retry,
            )

            self._history.append(Msg(role="assistant", content=result.text))

            # Persist the assistant turn
            await workflow.execute_activity(
                "persist_turn",
                PersistTurnReq(
                    session_id=session_id,
                    role="assistant",
                    content=result.text,
                    user_id=user_id,
                ),
                start_to_close_timeout=timedelta(seconds=30),
                retry_policy=retry,
            )

            # Auto-generate title on first turn
            if not self._title_generated and user_turn is not None:
                self._title_generated = True
                await workflow.execute_activity(
                    "generate_title",
                    GenerateTitleReq(
                        session_id=session_id,
                        user_message=user_turn,
                        user_id=user_id,
                    ),
                    start_to_close_timeout=timedelta(seconds=60),
                    retry_policy=retry,
                )

            # Guard against unbounded workflow history
            if workflow.info().is_continue_as_new_suggested():
                workflow.continue_as_new(
                    args=[session_id, self._history, user_id]
                )
