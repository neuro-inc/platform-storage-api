import asyncio
import dataclasses
import logging
from collections.abc import AsyncIterator

import aiohttp
import pytest
from aiohttp import web
from apolo_events_client import (
    ClientMessage,
    ClientMsgTypes,
    EventsClientConfig,
    ServerMsgTypes,
)
from pytest_aiohttp import AiohttpServer
from yarl import URL

log = logging.getLogger()


@dataclasses.dataclass
class Queues:
    income: asyncio.Queue[ClientMsgTypes]
    outcome: asyncio.Queue[ServerMsgTypes]


@pytest.fixture
def queues() -> Queues:
    return Queues(asyncio.Queue(), asyncio.Queue())


@pytest.fixture
async def events_server(
    queues: Queues, aiohttp_server: AiohttpServer
) -> AsyncIterator[URL]:
    async def sender(ws: web.WebSocketResponse) -> None:
        while True:
            msg = await queues.outcome.get()
            await ws.send_str(msg.model_dump_json())

    async def stream(req: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(req)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(sender(ws))
            async for ws_msg in ws:
                assert ws_msg.type == aiohttp.WSMsgType.TEXT
                msg = ClientMessage.model_validate_json(ws_msg.data)
                event = msg.root
                await queues.income.put(event)
        return ws

    app = aiohttp.web.Application()
    app.router.add_get("/apis/events/v1/stream", stream)

    srv = await aiohttp_server(app)
    log.info("Started events test server at %r", srv.make_url("/apis/events"))
    yield srv.make_url("/apis/events")

    log.info("Exit events test server")


@pytest.fixture
def events_config(events_server: URL) -> EventsClientConfig:
    return EventsClientConfig(url=events_server, token="token", name="platform-storage")
