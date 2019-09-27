import json
import os
import uuid
from dataclasses import dataclass
from pathlib import PurePath
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Dict,
    List,
    NamedTuple,
    Optional,
)

import aiohttp
import pytest
from jose import jwt
from neuro_auth_client import AuthClient, User
from yarl import URL

from platform_storage_api.api import create_app
from platform_storage_api.config import (
    AuthConfig,
    Config,
    EnvironConfigFactory,
    ServerConfig,
    StorageConfig,
)
from platform_storage_api.fs.local import FileSystem
from platform_storage_api.storage import Storage


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


@pytest.fixture(scope="session")
def in_docker() -> bool:
    return os.path.isfile("/.dockerenv")


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
def server_url(in_docker: bool, api: ApiConfig) -> str:
    if in_docker:
        return "http://storage:5000/api/v1/storage"
    else:
        return api.storage_base_url


@pytest.fixture
def config(in_docker: bool, admin_token: str) -> Config:
    if in_docker:
        return EnvironConfigFactory().create()

    server_config = ServerConfig()
    path = PurePath("/tmp/np_storage")
    storage_config = StorageConfig(fs_local_base_path=path)
    auth = AuthConfig(
        server_endpoint_url=URL("http://localhost:5003"), service_token=admin_token
    )
    return Config(server=server_config, storage=storage_config, auth=auth)


@pytest.fixture
async def auth_client(config: Config, admin_token: str) -> AsyncIterator[AuthClient]:
    async with AuthClient(
        url=config.auth.server_endpoint_url, token=admin_token
    ) as client:
        yield client


_UserFactory = Callable[..., User]


@pytest.fixture
async def regular_user_factory(
    auth_client: AuthClient, token_factory: _TokenFactory
) -> _UserFactory:
    async def _factory(name: Optional[str] = None) -> User:
        if not name:
            name = str(uuid.uuid4())
        user = User(name=name)
        await auth_client.add_user(user)
        return _User(name=user.name, token=token_factory(user.name))

    return _factory


@pytest.fixture
async def storage(local_fs: FileSystem, config: Config) -> Storage:
    return Storage(fs=local_fs, base_path=config.storage.fs_local_base_path)


@pytest.fixture
async def api(
    config: Config, storage: Storage, in_docker: bool
) -> AsyncIterator[ApiConfig]:
    app = await create_app(config, storage)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host="0.0.0.0", port=8080)
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


async def get_iterstatus_list(
    response_lines: AsyncIterable[bytes]
) -> List[Dict[str, Any]]:
    return [json.loads(line)["FileStatus"] async for line in response_lines]


def get_liststatus_dict(response_json: Dict[str, Any]) -> List[Any]:
    return response_json["FileStatuses"]["FileStatus"]


def get_filestatus_dict(response_json: Dict[str, Any]) -> Dict[str, Any]:
    return response_json["FileStatus"]
