import json
import uuid
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, replace
from pathlib import Path, PurePath
from typing import Any, NamedTuple, Optional

import aiohttp
import pytest
from jose import jwt
from neuro_auth_client import AuthClient, User
from neuro_auth_client.client import Cluster
from yarl import URL

from platform_storage_api.api import create_app
from platform_storage_api.config import (
    AuthConfig,
    Config,
    CORSConfig,
    ServerConfig,
    StorageConfig,
    StorageMode,
)


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


_TokenFactory = Callable[[str], str]


@pytest.fixture
def token_factory() -> _TokenFactory:
    def _factory(name: str) -> str:
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    return _factory


@pytest.fixture
def admin_token(token_factory: _TokenFactory) -> str:
    return token_factory("admin")


@dataclass
class _User:
    name: str
    token: str


@pytest.fixture
def server_url(api: ApiConfig) -> str:
    return api.storage_base_url


@pytest.fixture
def multi_storage_server_url(multi_storage_api: ApiConfig) -> str:
    return multi_storage_api.storage_base_url


@pytest.fixture
def config(admin_token: str, cluster_name: str) -> Config:
    server_config = ServerConfig()
    path = PurePath("/tmp/np_storage")
    storage_config = StorageConfig(fs_local_base_path=path)
    auth = AuthConfig(
        server_endpoint_url=URL("http://localhost:5003"), service_token=admin_token
    )
    return Config(
        server=server_config,
        storage=storage_config,
        auth=auth,
        cors=CORSConfig(allowed_origins=["http://localhost:8000"]),
        cluster_name=cluster_name,
    )


@pytest.fixture
def multi_storage_config(config: Config) -> Config:
    config = replace(config, storage=replace(config.storage, mode=StorageMode.MULTIPLE))
    Path(config.storage.fs_local_base_path, "main").mkdir(parents=True, exist_ok=True)
    Path(config.storage.fs_local_base_path, "extra").mkdir(parents=True, exist_ok=True)
    return config


@pytest.fixture
async def auth_client(config: Config, admin_token: str) -> AsyncIterator[AuthClient]:
    async with AuthClient(
        url=config.auth.server_endpoint_url, token=admin_token
    ) as client:
        yield client


_UserFactory = Callable[..., Awaitable[_User]]


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient,
    token_factory: _TokenFactory,
    admin_token: str,
    granter: Callable[[str, Any, User], Awaitable[None]],
    cluster_name: str,
) -> _UserFactory:
    async def _factory(name: Optional[str] = None) -> _User:
        if not name:
            name = str(uuid.uuid4())
        user = User(name=name, clusters=[Cluster(name=cluster_name)])
        await auth_client.add_user(user)
        # Grant permissions to the user home directory
        headers = auth_client._generate_headers(admin_token)
        payload = [
            {"uri": f"storage://{cluster_name}/{name}", "action": "manage"},
        ]
        async with auth_client._request(
            "POST", f"/api/v1/users/{name}/permissions", headers=headers, json=payload
        ) as p:
            assert p.status == 201
        return _User(name=user.name, token=token_factory(user.name))

    return _factory


@pytest.fixture
async def api(config: Config) -> AsyncIterator[ApiConfig]:
    app = await create_app(config)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host="0.0.0.0", port=8080)
    site = aiohttp.web.TCPSite(runner, api_config.host, api_config.port)
    await site.start()
    yield api_config
    await runner.cleanup()


@pytest.fixture
async def multi_storage_api(multi_storage_config: Config) -> AsyncIterator[ApiConfig]:
    app = await create_app(multi_storage_config)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host="0.0.0.0", port=8081)
    site = aiohttp.web.TCPSite(runner, api_config.host, api_config.port)
    await site.start()
    yield api_config
    await runner.cleanup()


@pytest.fixture
async def client() -> AsyncIterator[aiohttp.ClientSession]:
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
async def granter(auth_client: AuthClient, admin_token: str) -> Any:
    async def f(whom: Any, what: Any, sourcer: Any) -> None:
        headers = auth_client._generate_headers(sourcer.token)
        async with auth_client._request(
            "POST", f"/api/v1/users/{whom}/permissions", headers=headers, json=what
        ) as p:
            assert p.status == 201

    return f


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
