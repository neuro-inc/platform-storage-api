from __future__ import annotations

import asyncio
import logging
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from dataclasses import dataclass
from typing import Any

import aiohttp
import pytest
from async_timeout import timeout
from docker import DockerClient
from docker.errors import NotFound as ContainerNotFound
from docker.models.containers import Container
from jose import jwt
from neuro_auth_client import AuthClient, Cluster, User
from yarl import URL

from platform_storage_api.config import AuthConfig

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def auth_image() -> str:
    with open("AUTH_SERVER_IMAGE_NAME") as f:
        return f.read().strip()


@pytest.fixture(scope="session")
def auth_name() -> str:
    return "platform-storage-api-auth"


@pytest.fixture(scope="session")
def auth_jwt_secret() -> str:
    return os.environ.get("NP_JWT_SECRET", "secret")


def _create_url(container: Container, in_docker: bool) -> URL:
    exposed_port = 8080
    if in_docker:
        host, port = container.attrs["NetworkSettings"]["IPAddress"], exposed_port
    else:
        host, port = "0.0.0.0", container.ports[f"{exposed_port}/tcp"][0]["HostPort"]
    return URL(f"http://{host}:{port}")


@pytest.fixture(scope="session")
def _auth_url() -> URL:
    return URL(os.environ.get("AUTH_URL", ""))


@pytest.fixture(scope="session")
def _auth_server(
    docker_client: DockerClient,
    in_docker: bool,
    reuse_docker: bool,
    auth_image: str,
    auth_name: str,
    auth_jwt_secret: str,
    _auth_url: URL,
) -> Iterator[URL]:

    if _auth_url:
        yield _auth_url
        return

    try:
        container = docker_client.containers.get(auth_name)
        if reuse_docker:
            yield _create_url(container, in_docker)
            return
        else:
            container.remove(force=True)
    except ContainerNotFound:
        pass

    # `run` performs implicit `pull`
    container = docker_client.containers.run(
        image=auth_image,
        name=auth_name,
        publish_all_ports=True,
        stdout=False,
        stderr=False,
        detach=True,
        environment={"NP_JWT_SECRET": auth_jwt_secret},
    )
    container.reload()

    yield _create_url(container, in_docker)

    if not reuse_docker:
        container.remove(force=True)


async def wait_for_auth_server(
    url: URL, timeout_s: float = 300, interval_s: float = 1
) -> None:
    last_exc = None
    try:
        async with timeout(timeout_s):
            while True:
                try:
                    async with AuthClient(url=url, token="") as auth_client:
                        await auth_client.ping()
                        break
                except (AssertionError, OSError, aiohttp.ClientError) as exc:
                    last_exc = exc
                logger.debug(f"waiting for {url}: {last_exc}")
                await asyncio.sleep(interval_s)
    except asyncio.TimeoutError:
        pytest.fail(f"failed to connect to {url}: {last_exc}")


@pytest.fixture
async def auth_server(_auth_server: URL) -> AsyncIterator[URL]:
    await wait_for_auth_server(_auth_server)
    yield _auth_server


_TokenFactory = Callable[[str], str]


@pytest.fixture
def token_factory() -> _TokenFactory:
    def _factory(name: str) -> str:
        payload = {"identity": name}
        return jwt.encode(payload, "secret", algorithm="HS256")

    return _factory


@pytest.fixture
def admin_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("admin")


@pytest.fixture
def cluster_token(token_factory: Callable[[str], str]) -> str:
    return token_factory("cluster")


@pytest.fixture
def no_claim_token(auth_jwt_secret: str) -> str:
    payload: dict[str, Any] = {}
    return jwt.encode(payload, auth_jwt_secret, algorithm="HS256")


@pytest.fixture
async def auth_client(auth_server: URL, admin_token: str) -> AsyncIterator[AuthClient]:
    async with AuthClient(url=auth_server, token=admin_token) as client:
        yield client


@pytest.fixture
def auth_config(auth_server: URL, admin_token: str) -> AuthConfig:
    return AuthConfig(server_endpoint_url=auth_server, service_token=admin_token)


@dataclass
class _User:
    name: str
    token: str


_UserFactory = Callable[..., Awaitable[_User]]


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
async def regular_user_factory(
    auth_client: AuthClient,
    token_factory: _TokenFactory,
    admin_token: str,
    cluster_name: str,
) -> _UserFactory:
    async def _factory(
        name: None | str = None, override_cluster_name: None | str = None
    ) -> _User:
        if not name:
            name = str(uuid.uuid4())
        user_cluster_name = override_cluster_name or cluster_name
        user = User(name=name, clusters=[Cluster(name=user_cluster_name)])
        await auth_client.add_user(user)
        # Grant permissions to the user home directory
        headers = auth_client._generate_headers(admin_token)
        payload = [
            {"uri": f"storage://{user_cluster_name}/{name}", "action": "manage"},
        ]
        async with auth_client._request(
            "POST", f"/api/v1/users/{name}/permissions", headers=headers, json=payload
        ) as p:
            assert p.status == 201
        return _User(name=user.name, token=token_factory(user.name))

    return _factory
