from __future__ import annotations

from tclaw.types import Msg


def drain_inbox(inbox: list[str], history: list[Msg]) -> str | None:
    """Drain *inbox* into *history* as a single user turn.

    Mutates both lists.  Returns the coalesced content, or ``None`` if the
    inbox was empty (history is unchanged in that case).

    Multiple queued messages are joined with double-newlines because the
    model API requires alternating user/assistant roles.
    """
    if not inbox:
        return None
    combined = "\n\n".join(inbox)
    inbox.clear()
    history.append(Msg(role="user", content=combined))
    return combined
