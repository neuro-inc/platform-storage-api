from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

import aiohttp.web
import pytest
from yarl import URL

from tests.integration.conftest import ApiRunner


class MockAdminServer:
    def __init__(self) -> None:
        self.clusters: list[Any] = []
        self.org_clusters: dict[str, Any] = {}

    async def list_clusters(self, request: aiohttp.web.Request) -> aiohttp.web.Response:
        return aiohttp.web.json_response(self.clusters)

    async def list_org_clusters(
        self, request: aiohttp.web.Request
    ) -> aiohttp.web.Response:
        cluster_name = request.match_info["cname"]
        return aiohttp.web.json_response(self.org_clusters[cluster_name])


@pytest.fixture(scope="session")
async def mock_admin_server() -> MockAdminServer:
    server = MockAdminServer()
    server.clusters = [
        {
            "name": "cluster-on-maintenance",
            "default_quota": {},
            "default_credits": None,
            "maintenance": False,
        },
        {
            "name": "cluster-with-org-on-maintenance",
            "default_quota": {},
            "default_credits": None,
            "maintenance": False,
        },
    ]
    server.org_clusters = {
        "cluster-on-maintenance": [],
        "cluster-with-org-on-maintenance": [{"org_name": "org", "maintenance": False}],
    }
    return server


@pytest.fixture(scope="session")
async def admin_url(mock_admin_server: MockAdminServer) -> AsyncIterator[URL]:
    app = aiohttp.web.Application()

    app.add_routes(
        (
            aiohttp.web.get("/api/v1/clusters", mock_admin_server.list_clusters),
            aiohttp.web.get(
                "/api/v1/clusters/{cname}/orgs", mock_admin_server.list_org_clusters
            ),
        )
    )

    runner = ApiRunner(app, port=8090)
    api_address = await runner.run()
    yield URL(f"http://{api_address.host}:{api_address.port}/api/v1")
    await runner.close()
