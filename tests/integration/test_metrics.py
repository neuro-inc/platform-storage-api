from __future__ import annotations

import textwrap
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path

import aiohttp
import pytest
from aiobotocore.client import AioBaseClient
from aioresponses import aioresponses
from fastapi import FastAPI
from yarl import URL

from platform_storage_api.config import MetricsConfig
from platform_storage_api.metrics import create_app
from platform_storage_api.storage_usage import StorageUsageService

from .conftest import run_asgi_app


@pytest.fixture()
def app(metrics_config: MetricsConfig) -> Iterator[FastAPI]:
    with create_app(config=metrics_config) as app:
        yield app


@pytest.fixture()
async def metrics_server(
    app: FastAPI, unused_tcp_port_factory: Callable[[], int]
) -> AsyncIterator[URL]:
    host = "0.0.0.0"
    port = unused_tcp_port_factory()
    async with run_asgi_app(app, host=host, port=port):
        yield URL(f"http://{host}:{port}")


@pytest.fixture()
async def client(metrics_server: URL) -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession(base_url=metrics_server) as client:
        yield client


class TestMetrics:
    async def test_metrics__no_bucket(self, client: aiohttp.ClientSession) -> None:
        response = await client.get("/metrics")

        assert response.status == 200
        data = await response.text()
        assert data == textwrap.dedent(
            """\
            # HELP storage_used_bytes The amount of used storage space in bytes
            # TYPE storage_used_bytes gauge
            """
        )

    async def test_metrics__no_key(
        self,
        client: aiohttp.ClientSession,
        s3_client: AioBaseClient,
        metrics_config: MetricsConfig,
    ) -> None:
        await s3_client.create_bucket(Bucket=metrics_config.aws.metrics_s3_bucket_name)

        response = await client.get("/metrics")

        assert response.status == 200
        data = await response.text()
        assert data == textwrap.dedent(
            """\
            # HELP storage_used_bytes The amount of used storage space in bytes
            # TYPE storage_used_bytes gauge
            """
        )

    async def test_metrics(
        self,
        client: aiohttp.ClientSession,
        storage_usage_service: StorageUsageService,
        cluster_name: str,
        local_tmp_dir_path: Path,
    ) -> None:
        with aioresponses(
            passthrough=["http://0.0.0.0", "http://127.0.0.1"]
        ) as aiohttp_mock:
            aiohttp_mock.get(
                f"http://platform-admin/apis/admin/v1/clusters/{cluster_name}/orgs",
                payload=[],
            )

            (local_tmp_dir_path / "test-project").mkdir()

            await storage_usage_service.upload_storage_usage()

        response = await client.get("/metrics")

        assert response.status == 200
        data = await response.text()
        assert data.startswith(
            textwrap.dedent(
                """\
            # HELP storage_used_bytes The amount of used storage space in bytes
            # TYPE storage_used_bytes gauge
            storage_used_bytes{org_name="no_org",project_name="test-project"}"""
            )
        )
