import json
import os
from collections.abc import AsyncIterable, AsyncIterator
from dataclasses import replace
from pathlib import Path, PurePath
from typing import Any, NamedTuple

import aiohttp
import pytest
import pytest_asyncio

from platform_storage_api.api import create_app
from platform_storage_api.config import (
    AuthConfig,
    Config,
    ServerConfig,
    StorageConfig,
    StorageMode,
)

pytest_plugins = [
    "tests.integration.docker",
    "tests.integration.auth",
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
def config(admin_token: str, cluster_name: str, auth_config: AuthConfig) -> Config:
    server_config = ServerConfig()
    path = PurePath(os.path.realpath("/tmp/np_storage"))
    storage_config = StorageConfig(fs_local_base_path=path)
    return Config(
        server=server_config,
        storage=storage_config,
        auth=auth_config,
        cluster_name=cluster_name,
    )


@pytest.fixture
def multi_storage_config(config: Config) -> Config:
    config = replace(config, storage=replace(config.storage, mode=StorageMode.MULTIPLE))
    Path(config.storage.fs_local_base_path, config.cluster_name).mkdir(
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
