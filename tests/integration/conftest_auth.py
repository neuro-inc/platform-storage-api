from __future__ import annotations

import logging
import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from typing import Any

import pytest
from jose import jwt
from neuro_auth_client import AuthClient, User
from pytest_docker.plugin import Services
from yarl import URL

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def auth_server(docker_ip: str, docker_services: Services) -> URL:
    port = docker_services.port_for("platform-auth", 8080)
    return URL(f"http://{docker_ip}:{port}")


@pytest.fixture(scope="session")
def auth_jwt_secret() -> str:
    return os.environ.get("NP_JWT_SECRET", "secret")


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
    async def _factory(name: str | None = None) -> _User:
        if not name:
            name = str(uuid.uuid4())
        user = User(name=name)
        await auth_client.add_user(user)
        # Grant permissions to the user home directory
        headers = auth_client._generate_headers(admin_token)
        payload = [
            {"uri": f"storage://{cluster_name}/{name}", "action": "manage"},
            {"uri": f"storage://{cluster_name}/org", "action": "manage"},
        ]
        async with auth_client._request(
            "POST", f"/api/v1/users/{name}/permissions", headers=headers, json=payload
        ) as p:
            assert p.status == 201
        return _User(name=user.name, token=token_factory(user.name))

    return _factory
