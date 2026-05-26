"""IRC bridge — connects an IRC channel to tclaw via the HTTP API.

Mirrors ted's irc-bridge.ts: listens on a channel, forwards messages to the
webhook, and relays streamed responses back to IRC.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import ssl
from typing import AsyncGenerator

import httpx

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def chunk_for_irc(text: str, max_bytes: int = 400) -> list[str]:
    """Split *text* into IRC-safe PRIVMSG payloads (no CR/LF, ≤ *max_bytes*)."""
    oneline = " ".join(text.replace("\r\n", " ").replace("\n", " ").split()).strip()
    if not oneline:
        return []

    out: list[str] = []
    buf = ""
    for word in oneline.split(" "):
        if not word:
            continue
        candidate = f"{buf} {word}" if buf else word
        if len(candidate.encode()) <= max_bytes:
            buf = candidate
            continue
        if buf:
            out.append(buf)
        if len(word.encode()) <= max_bytes:
            buf = word
        else:
            b = word.encode()
            while len(b) > max_bytes:
                out.append(b[:max_bytes].decode("utf-8", errors="ignore"))
                b = b[max_bytes:]
            buf = b.decode("utf-8", errors="ignore")
    if buf:
        out.append(buf)
    return out


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class Config:
    def __init__(self) -> None:
        def must(name: str) -> str:
            v = os.environ.get(name)
            if not v:
                raise RuntimeError(f"{name} is required")
            return v

        self.server = must("IRC_SERVER")
        self.port = int(os.environ.get("IRC_PORT", "6667"))
        self.tls = os.environ.get("IRC_TLS", "false").lower() == "true"
        self.nick = os.environ.get("IRC_NICK", "tclaw-bot")
        channel = must("IRC_CHANNEL")
        if not channel.startswith("#") and not channel.startswith("&"):
            raise RuntimeError("IRC_CHANNEL must start with # or &")
        self.channel = channel
        self.session_id = os.environ.get("IRC_SESSION_ID", f"irc-{channel[1:]}")
        self.user_id = must("IRC_USER_ID")
        self.webhook_url = os.environ.get("WEBHOOK_URL", "http://localhost:8787")
        self.password = os.environ.get("IRC_PASSWORD")


# ---------------------------------------------------------------------------
# Webhook helpers
# ---------------------------------------------------------------------------


async def post_to_webhook(
    http: httpx.AsyncClient, cfg: Config, msg: str
) -> None:
    resp = await http.post(
        f"{cfg.webhook_url}/message",
        json={"sessionId": cfg.session_id, "msg": msg},
        headers={"X-User-ID": cfg.user_id},
    )
    resp.raise_for_status()


async def read_sse(
    http: httpx.AsyncClient, cfg: Config
) -> AsyncGenerator[dict, None]:
    """Minimal SSE parser over an httpx streaming response."""
    url = f"{cfg.webhook_url}/sessions/{cfg.session_id}/stream"
    async with http.stream("GET", url, headers={"X-User-ID": cfg.user_id}) as resp:
        resp.raise_for_status()
        data_lines: list[str] = []
        async for line in resp.aiter_lines():
            if line == "":
                if data_lines:
                    raw = "\n".join(data_lines)
                    data_lines = []
                    try:
                        yield json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                continue
            if line.startswith(":"):
                continue
            if line.startswith("data:"):
                data_lines.append(line[5:].lstrip(" "))


# ---------------------------------------------------------------------------
# Simple asyncio IRC client
# ---------------------------------------------------------------------------


class SimpleIRCClient:
    """Bare-bones async IRC client — enough for a bridge."""

    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    async def connect(self) -> None:
        ssl_ctx = ssl.create_default_context() if self.cfg.tls else None
        self._reader, self._writer = await asyncio.open_connection(
            self.cfg.server, self.cfg.port, ssl=ssl_ctx
        )
        if self.cfg.password:
            self._send(f"PASS {self.cfg.password}")
        self._send(f"NICK {self.cfg.nick}")
        self._send(f"USER {self.cfg.nick} 0 * :{self.cfg.nick}")

    def _send(self, line: str) -> None:
        if self._writer:
            self._writer.write((line + "\r\n").encode())

    async def join(self, channel: str) -> None:
        self._send(f"JOIN {channel}")

    async def say(self, target: str, msg: str) -> None:
        self._send(f"PRIVMSG {target} :{msg}")

    async def quit(self, reason: str = "bye") -> None:
        self._send(f"QUIT :{reason}")
        if self._writer:
            self._writer.close()

    async def read_lines(self) -> AsyncGenerator[str, None]:
        assert self._reader
        buf = ""
        while True:
            data = await self._reader.read(4096)
            if not data:
                return
            buf += data.decode("utf-8", errors="replace")
            while "\r\n" in buf:
                line, buf = buf.split("\r\n", 1)
                yield line


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def stream_to_irc(
    http: httpx.AsyncClient,
    cfg: Config,
    irc: SimpleIRCClient,
) -> None:
    """Subscribe to SSE and relay events to IRC."""
    pending = ""
    thinking = ""
    async for event in read_sse(http, cfg):
        etype = event.get("type")
        if etype == "delta":
            pending += event.get("text", "")
        elif etype == "thinking":
            thinking += event.get("text", "")
        elif etype == "tool_call":
            name = event.get("name", "unknown")
            await irc.say(cfg.channel, f"[using {name}]")
        elif etype == "turn_end":
            if thinking.strip():
                for chunk in chunk_for_irc(f"[thinking] {thinking}"):
                    await irc.say(cfg.channel, chunk)
            if pending.strip():
                for chunk in chunk_for_irc(pending):
                    await irc.say(cfg.channel, chunk)
            pending = ""
            thinking = ""


async def _async_main() -> None:
    logging.basicConfig(level=logging.INFO)
    cfg = Config()
    logger.info(
        "Connecting to %s:%d as %s, joining %s",
        cfg.server,
        cfg.port,
        cfg.nick,
        cfg.channel,
    )

    http = httpx.AsyncClient(timeout=httpx.Timeout(90, connect=10))

    # Prime the session — retry until webhook is reachable
    for attempt in range(1, 100):
        try:
            await post_to_webhook(http, cfg, f"[irc bridge online in {cfg.channel}]")
            break
        except Exception as exc:
            logger.error("prime attempt %d failed: %s", attempt, exc)
            await asyncio.sleep(min(attempt * 2, 15))

    base_nick = cfg.nick

    while True:
        irc = SimpleIRCClient(cfg)
        try:
            await irc.connect()
        except Exception as exc:
            logger.error("IRC connect failed: %s, retrying in 5s", exc)
            await asyncio.sleep(5)
            continue

        await irc.join(cfg.channel)
        stream_task: asyncio.Task | None = None

        try:
            # Read IRC lines in one task, stream SSE in another
            async def handle_irc() -> None:
                registered = False
                async for line in irc.read_lines():
                    # Respond to PING
                    if line.startswith("PING"):
                        irc._send(f"PONG {line[5:]}")
                        continue

                    # Wait for registration
                    if not registered and (" 001 " in line or " 376 " in line):
                        registered = True
                        await irc.join(cfg.channel)
                        logger.info("Registered and joining %s", cfg.channel)
                        continue

                    # Parse PRIVMSG
                    if "PRIVMSG" in line:
                        parts = line.split(" ", 3)
                        if len(parts) >= 4:
                            prefix = parts[0]
                            nick = prefix.split("!")[0].lstrip(":")
                            target = parts[2]
                            msg = parts[3].lstrip(":")
                            if target == cfg.channel and not nick.startswith(base_nick):
                                payload = f"{nick}: {msg}"
                                try:
                                    await post_to_webhook(http, cfg, payload)
                                except Exception as exc:
                                    logger.error("webhook post failed: %s", exc)

            async def handle_stream() -> None:
                while True:
                    try:
                        await stream_to_irc(http, cfg, irc)
                    except Exception as exc:
                        logger.error("stream error: %s", exc)
                        await asyncio.sleep(2)

            stream_task = asyncio.create_task(handle_stream())
            await handle_irc()
        except Exception as exc:
            logger.error("IRC session error: %s", exc)
        finally:
            if stream_task:
                stream_task.cancel()
            try:
                await irc.quit("reconnecting")
            except Exception:
                pass

        logger.info("IRC disconnected, reconnecting in 5s...")
        await asyncio.sleep(5)


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
