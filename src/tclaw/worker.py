from __future__ import annotations

import asyncio
import logging
import os

from temporalio.client import Client
from temporalio.worker import Worker

from tclaw import db
from tclaw.activities import generate_title, persist_turn, stream_agent_turn
from tclaw.workflows import ChatSession


async def _async_main() -> None:
    logging.basicConfig(level=logging.INFO)

    address = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
    namespace = os.environ.get("TEMPORAL_NAMESPACE", "default")
    task_queue = os.environ.get("TASK_QUEUE", "chat")

    await db.ensure_schema()

    client = await Client.connect(address, namespace=namespace)

    worker = Worker(
        client,
        task_queue=task_queue,
        workflows=[ChatSession],
        activities=[stream_agent_turn, persist_turn, generate_title],
    )

    logging.info("Worker listening on task queue %r at %s", task_queue, address)
    try:
        await worker.run()
    except Exception as exc:
        logging.error("Worker crashed: %s", exc, exc_info=True)
        raise


def main() -> None:
    asyncio.run(_async_main())


if __name__ == "__main__":
    main()
