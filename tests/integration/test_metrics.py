from __future__ import annotations

import asyncio
import textwrap
import time
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path
from unittest import mock

import aiohttp
import pytest
from aiobotocore.client import AioBaseClient
from aioresponses import aioresponses
from fastapi import FastAPI
from yarl import URL

from platform_storage_api.config import MetricsConfig
from platform_storage_api.metrics import create_app
from platform_storage_api.s3_storage import StorageMetricsS3Storage
from platform_storage_api.storage_usage import StorageUsage, StorageUsageService

from .conftest import run_asgi_app


@pytest.fixture
def app(metrics_config: MetricsConfig) -> Iterator[FastAPI]:
    with create_app(config=metrics_config) as app:
        yield app


@pytest.fixture
async def metrics_server(
    app: FastAPI, unused_tcp_port_factory: Callable[[], int]
) -> AsyncIterator[URL]:
    host = "0.0.0.0"
    port = unused_tcp_port_factory()
    async with run_asgi_app(app, host=host, port=port):
        yield URL(f"http://127.0.0.1:{port}")


@pytest.fixture
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
        await s3_client.create_bucket(Bucket=metrics_config.s3.bucket_name)

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
                f"http://platform-admin/apis/admin/v1/clusters/{cluster_name}/projects",
                payload=[
                    {
                        "name": "test-project",
                        "org_name": None,
                        "cluster_name": cluster_name,
                        "default_role": "writer",
                        "is_default": False,
                    },
                ],
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

    async def test_metrics__openmetrics(self, client: aiohttp.ClientSession) -> None:
        response = await client.get(
            "/metrics/", headers={"Accept": "application/openmetrics-text"}
        )

        assert response.status == 200
        assert response.content_type == "application/openmetrics-text"
        data = await response.text()
        assert data.endswith("# EOF\n")

    async def test_ping__slow_storage(self, client: aiohttp.ClientSession) -> None:
        def get_storage_usage() -> StorageUsage:
            time.sleep(3)
            return StorageUsage(projects=[])

        with mock.patch.object(
            StorageMetricsS3Storage, "get_storage_usage", side_effect=get_storage_usage
        ):
            start = time.monotonic()
            metrics_task = asyncio.create_task(client.get("/metrics/"))
            await asyncio.sleep(0.5)
            response = await client.get("/ping")
            elapsed = time.monotonic() - start

            assert response.status == 200
            assert elapsed < 1.5
            assert not metrics_task.done()
            response = await metrics_task
            assert response.status == 200

    async def test_metrics__s3_error(
        self,
        client: aiohttp.ClientSession,
        s3_client: AioBaseClient,
        metrics_config: MetricsConfig,
    ) -> None:
        await s3_client.create_bucket(Bucket=metrics_config.s3.bucket_name)
        await s3_client.put_object(
            Bucket=metrics_config.s3.bucket_name,
            Key=f"{metrics_config.s3.key_prefix}storage_usage.json",
            Body=b"{}",
            StorageClass="GLACIER",
        )

        response = await client.get("/metrics/")

        assert response.status == 500
