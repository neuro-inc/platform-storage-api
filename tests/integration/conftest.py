from __future__ import annotations

import json
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path
from typing import Any, NamedTuple

import aiobotocore.client
import aiohttp
import pytest
import pytest_asyncio
import uvicorn
from neuro_admin_client import AdminClient
from yarl import URL

from platform_storage_api.api import create_app
from platform_storage_api.config import (
    Config,
    MetricsConfig,
    PlatformConfig,
    S3Config,
    StorageConfig,
    StorageMode,
    StorageServerConfig,
)
from platform_storage_api.fs.local import FileSystem
from platform_storage_api.s3_storage import StorageMetricsAsyncS3Storage
from platform_storage_api.storage import SingleStoragePathResolver
from platform_storage_api.storage_usage import StorageUsageService


pytest_plugins = [
    "tests.integration.conftest_docker",
    "tests.integration.conftest_auth",
    "tests.integration.conftest_moto",
]


@asynccontextmanager
async def run_asgi_app(
    app: Any, *, host: str = "0.0.0.0", port: int = 8080
) -> AsyncIterator[None]:
    server = uvicorn.Server(uvicorn.Config(app=app, host=host, port=port))
    server.should_exit = True
    await server.serve()
    try:
        yield
    finally:
        await server.shutdown()


class ApiConfig(NamedTuple):
    host: str
    port: int

    @property
    def endpoint(self) -> str:
        return f"http://{self.host}:{self.port}/api/v1"

    @property
    def storage_base_url(self) -> str:
        return self.endpoint + "/storage"

    @property
    def ping_url(self) -> str:
        return self.endpoint + "/ping"


@pytest.fixture
def server_url(api: ApiConfig) -> str:
    return api.storage_base_url


@pytest.fixture
def multi_storage_server_url(multi_storage_api: ApiConfig) -> str:
    return multi_storage_api.storage_base_url


@pytest.fixture
def platform_config(
    auth_server: URL, admin_token: str, cluster_name: str
) -> PlatformConfig:
    return PlatformConfig(
        auth_url=auth_server,
        admin_url=URL("http://platform-admin/apis/admin/v1"),
        token=admin_token,
        cluster_name=cluster_name,
    )


@pytest.fixture
def config(
    platform_config: PlatformConfig, s3_config: S3Config, local_tmp_dir_path: Path
) -> Config:
    server_config = StorageServerConfig()
    storage_config = StorageConfig(fs_local_base_path=local_tmp_dir_path)
    return Config(
        server=server_config,
        storage=storage_config,
        platform=platform_config,
        s3=s3_config,
    )


@pytest.fixture
def metrics_config(s3_config: S3Config) -> MetricsConfig:
    return MetricsConfig(s3=s3_config)


@pytest.fixture
def multi_storage_config(config: Config) -> Config:
    config = replace(config, storage=replace(config.storage, mode=StorageMode.MULTIPLE))
    Path(config.storage.fs_local_base_path, config.platform.cluster_name).mkdir(
        parents=True, exist_ok=True
    )
    return config


@pytest_asyncio.fixture
async def api(config: Config) -> AsyncIterator[ApiConfig]:
    app = await create_app(config)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host="0.0.0.0", port=8080)
    site = aiohttp.web.TCPSite(runner, api_config.host, api_config.port)
    await site.start()
    yield api_config
    await runner.cleanup()


@pytest_asyncio.fixture
async def multi_storage_api(multi_storage_config: Config) -> AsyncIterator[ApiConfig]:
    app = await create_app(multi_storage_config)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host="0.0.0.0", port=8081)
    site = aiohttp.web.TCPSite(runner, api_config.host, api_config.port)
    await site.start()
    yield api_config
    await runner.cleanup()


@pytest_asyncio.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def cluster_name() -> str:
    return "test-cluster"


async def status_iter_response_to_list(
    response_lines: AsyncIterable[bytes],
) -> list[dict[str, Any]]:
    return [json.loads(line)["FileStatus"] async for line in response_lines]


def get_liststatus_dict(response_json: dict[str, Any]) -> list[Any]:
    return response_json["FileStatuses"]["FileStatus"]


def get_filestatus_dict(response_json: dict[str, Any]) -> dict[str, Any]:
    return response_json["FileStatus"]


@pytest.fixture
async def admin_client() -> AsyncIterator[AdminClient]:
    async with AdminClient(
        base_url=URL("http://platform-admin/apis/admin/v1")
    ) as client:
        yield client


@pytest.fixture
def storage_metrics_s3_storage(
    s3_client: aiobotocore.client.AioBaseClient, s3_config: S3Config
) -> StorageMetricsAsyncS3Storage:
    return StorageMetricsAsyncS3Storage(
        s3_client=s3_client, bucket_name=s3_config.bucket_name
    )


@pytest.fixture
def storage_usage_service(
    config: Config,
    admin_client: AdminClient,
    storage_metrics_s3_storage: StorageMetricsAsyncS3Storage,
    local_fs: FileSystem,
    local_tmp_dir_path: Path,
) -> StorageUsageService:
    return StorageUsageService(
        config=config,
        admin_client=admin_client,
        storage_metrics_s3_storage=storage_metrics_s3_storage,
        fs=local_fs,
        path_resolver=SingleStoragePathResolver(local_tmp_dir_path),
    )
