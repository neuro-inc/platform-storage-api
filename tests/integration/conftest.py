import asyncio
import json
import os
from collections.abc import AsyncIterable, AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from pathlib import Path, PurePath
from typing import Any, NamedTuple, Optional

import aiohttp
import aiohttp.web
import pytest
import pytest_asyncio
from yarl import URL

from platform_storage_api.api import create_app
from platform_storage_api.config import (
    AdminConfig,
    AuthConfig,
    Config,
    CORSConfig,
    PlatformConfigConfig,
    ServerConfig,
    StorageConfig,
    StorageMode,
)

pytest_plugins = [
    "tests.integration.docker",
    "tests.integration.auth",
    "tests.integration.config",
    "tests.integration.admin",
]


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
def config(
    admin_token: str,
    cluster_name: str,
    auth_config: AuthConfig,
    admin_url: URL,
    config_url: URL,
) -> Config:
    server_config = ServerConfig()
    path = PurePath(os.path.realpath("/tmp/np_storage"))
    storage_config = StorageConfig(fs_local_base_path=path)
    return Config(
        server=server_config,
        storage=storage_config,
        auth=auth_config,
        cors=CORSConfig(allowed_origins=["http://localhost:8000"]),
        cluster_name=cluster_name,
        admin=AdminConfig(
            server_endpoint_url=admin_url,
            service_token=auth_config.service_token,
        ),
        platform_config=PlatformConfigConfig(
            server_endpoint_url=config_url,
            service_token=auth_config.service_token,
        ),
    )


@pytest.fixture
def multi_storage_config(config: Config) -> Config:
    config = replace(config, storage=replace(config.storage, mode=StorageMode.MULTIPLE))
    Path(config.storage.fs_local_base_path, "main").mkdir(parents=True, exist_ok=True)
    Path(config.storage.fs_local_base_path, "extra").mkdir(parents=True, exist_ok=True)
    return config


@pytest.fixture
def on_maintenance_cluster_config(
    config: Config, on_maintenance_cluster_name: str
) -> Config:
    config = replace(config, cluster_name=on_maintenance_cluster_name)
    return config


@pytest.fixture
def on_maintenance_org_cluster_config(
    config: Config, on_maintenance_org_cluster_name: str
) -> Config:
    config = replace(config, cluster_name=on_maintenance_org_cluster_name)
    return config


@dataclass(frozen=True)
class ApiAddress:
    host: str
    port: int


@asynccontextmanager
async def create_local_app_server(
    app: aiohttp.web.Application, port: int = 8080
) -> AsyncIterator[ApiAddress]:
    runner = aiohttp.web.AppRunner(app)
    try:
        await runner.setup()
        api_address = ApiAddress("0.0.0.0", port)
        site = aiohttp.web.TCPSite(runner, api_address.host, api_address.port)
        await site.start()
        yield api_address
    finally:
        await runner.shutdown()
        await runner.cleanup()


class ApiRunner:
    def __init__(self, app: aiohttp.web.Application, port: int) -> None:
        self._app = app
        self._port = port

        self._api_address_future: asyncio.Future[ApiAddress] = asyncio.Future()
        self._cleanup_future: asyncio.Future[None] = asyncio.Future()
        self._task: Optional[asyncio.Task[None]] = None

    async def _run(self) -> None:
        async with create_local_app_server(self._app, port=self._port) as api_address:
            self._api_address_future.set_result(api_address)
            await self._cleanup_future

    async def run(self) -> ApiAddress:
        loop = asyncio.get_event_loop()
        self._task = loop.create_task(self._run())
        return await self._api_address_future

    async def close(self) -> None:
        if self._task:
            task = self._task
            self._task = None
            self._cleanup_future.set_result(None)
            await task

    @property
    def closed(self) -> bool:
        return not bool(self._task)


@pytest_asyncio.fixture
async def api(config: Config) -> AsyncIterator[ApiConfig]:
    app = await create_app(config)
    runner = ApiRunner(app, 8080)
    api_address = await runner.run()
    yield ApiConfig(api_address.host, api_address.port)
    await runner.close()


@pytest_asyncio.fixture
async def multi_storage_api(multi_storage_config: Config) -> AsyncIterator[ApiConfig]:
    app = await create_app(multi_storage_config)
    runner = ApiRunner(app, 8081)
    api_address = await runner.run()
    yield ApiConfig(api_address.host, api_address.port)
    await runner.close()


@pytest_asyncio.fixture
async def on_maintenance_cluster_api(
    on_maintenance_cluster_config: Config,
) -> AsyncIterator[ApiConfig]:
    app = await create_app(on_maintenance_cluster_config)
    runner = ApiRunner(app, 8082)
    api_address = await runner.run()
    yield ApiConfig(api_address.host, api_address.port)
    await runner.close()


@pytest_asyncio.fixture
async def on_maintenance_org_cluster_api(
    on_maintenance_org_cluster_config: Config,
) -> AsyncIterator[ApiConfig]:
    app = await create_app(on_maintenance_org_cluster_config)
    runner = ApiRunner(app, 8084)
    api_address = await runner.run()
    yield ApiConfig(api_address.host, api_address.port)
    await runner.close()


@pytest_asyncio.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
def cluster_name() -> str:
    return "test-cluster"


@pytest.fixture
def on_maintenance_cluster_name() -> str:
    return "cluster-on-maintenance"


@pytest.fixture
def on_maintenance_org_cluster_name() -> str:
    return "cluster-with-org-on-maintenance"


async def status_iter_response_to_list(
    response_lines: AsyncIterable[bytes],
) -> list[dict[str, Any]]:
    return [json.loads(line)["FileStatus"] async for line in response_lines]


def get_liststatus_dict(response_json: dict[str, Any]) -> list[Any]:
    return response_json["FileStatuses"]["FileStatus"]


def get_filestatus_dict(response_json: dict[str, Any]) -> dict[str, Any]:
    return response_json["FileStatus"]
