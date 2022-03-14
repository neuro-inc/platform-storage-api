from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aiohttp.web
import pytest
from yarl import URL

from tests.integration.conftest import ApiRunner


class MockConfigServer:
    def __init__(self) -> None:
        self.clusters: list[Any] = []

    async def list_clusters(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response(self.clusters)


@pytest.fixture(scope="session")
async def mock_config_server() -> MockConfigServer:
    server = MockConfigServer()
    server.clusters = [
        {
            "name": "cluster-on-maintenance",
            "status": "deployed",
            "created_at": "2022-03-14T11:22:59.869249",
            "cloud_provider": {
                "type": "AWS",
                "region": "us-east-1",
                "zones": [],
                "credentials": {
                    "access_key_id": "any",
                    "secret_access_key": "thing",
                },
                "node_pools": [],
                "storage": {
                    "id": "id",
                    "description": "description",
                    "performance_mode": "generalPurpose",
                    "throughput_mode": "bursting",
                    "instances": [{"ready": False}],
                },
            },
        },
        {
            "name": "cluster-with-org-on-maintenance",
            "status": "deployed",
            "created_at": "2022-03-14T11:22:59.869249",
            "cloud_provider": {
                "type": "AWS",
                "region": "us-east-1",
                "zones": [],
                "credentials": {
                    "access_key_id": "any",
                    "secret_access_key": "thing",
                },
                "node_pools": [],
                "storage": {
                    "id": "id",
                    "description": "description",
                    "performance_mode": "generalPurpose",
                    "throughput_mode": "bursting",
                    "instances": [{"ready": True}, {"name": "org", "ready": False}],
                },
            },
        },
    ]
    return server


@pytest.fixture(scope="session")
async def config_url(mock_config_server: MockConfigServer) -> AsyncIterator[URL]:
    app = aiohttp.web.Application()

    app.add_routes(
        (aiohttp.web.get("/api/v1/clusters", mock_config_server.list_clusters),)
    )

    runner = ApiRunner(app, port=8089)
    api_address = await runner.run()
    yield URL(f"http://{api_address.host}:{api_address.port}")
    await runner.close()
