from tclaw.inbox import drain_inbox
from tclaw.types import Msg


def test_drain_empty_inbox() -> None:
    inbox: list[str] = []
    history: list[Msg] = []
    result = drain_inbox(inbox, history)
    assert result is None
    assert history == []
    assert inbox == []


def test_drain_single_message() -> None:
    inbox = ["hello"]
    history: list[Msg] = []
    result = drain_inbox(inbox, history)
    assert result == "hello"
    assert history == [Msg(role="user", content="hello")]
    assert inbox == []


def test_drain_multiple_messages_coalesced() -> None:
    inbox = ["msg-1", "msg-2", "msg-3"]
    history: list[Msg] = []
    result = drain_inbox(inbox, history)
    assert result == "msg-1\n\nmsg-2\n\nmsg-3"
    assert history == [Msg(role="user", content="msg-1\n\nmsg-2\n\nmsg-3")]
    assert inbox == []


def test_drain_appends_to_existing_history() -> None:
    inbox = ["follow-up"]
    history = [
        Msg(role="user", content="first"),
        Msg(role="assistant", content="reply"),
    ]
    result = drain_inbox(inbox, history)
    assert result == "follow-up"
    assert len(history) == 3
    assert history[-1] == Msg(role="user", content="follow-up")


def test_drain_clears_inbox() -> None:
    inbox = ["a", "b"]
    history: list[Msg] = []
    drain_inbox(inbox, history)
    assert inbox == []
    # Calling again should return None
    assert drain_inbox(inbox, history) is None
