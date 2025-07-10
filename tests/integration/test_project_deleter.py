from datetime import UTC, datetime
from uuid import uuid4

import aiohttp
from apolo_events_client import Ack, EventType, RecvEvent, RecvEvents, StreamType, Tag

from .conftest_auth import _UserFactory
from .conftest_events import Queues


async def test_deleter(
    client: aiohttp.ClientSession,
    regular_user_factory: _UserFactory,
    server_url: str,
    queues: Queues,
) -> None:
    user = await regular_user_factory()
    headers = {"Authorization": "Bearer " + user.token}
    url = f"{server_url}/org/project/path/to/file"
    payload = b"test content"

    async with client.put(url, headers=headers, data=payload) as response:
        assert response.status == 201

    await queues.outcome.put(
        RecvEvents(
            subscr_id=uuid4(),
            events=[
                RecvEvent(
                    tag=Tag("123"),
                    timestamp=datetime.now(tz=UTC),
                    sender="platform-admin",
                    stream=StreamType("platform-admin"),
                    event_type=EventType("project-remove"),
                    org="org",
                    cluster="cluster",
                    project="project",
                    user="user",
                ),
            ],
        )
    )

    ev = await queues.income.get()

    assert isinstance(ev, Ack)
    assert ev.events[StreamType("platform-admin")] == ["123"]

    async with client.head(url, headers=headers) as response:
        assert response.status == 404

    async with client.head(f"{server_url}/org/proj", headers=headers) as response:
        assert response.status == 404
