from __future__ import annotations

from collections.abc import AsyncIterator

import aiobotocore.session
import aiohttp
import pytest
from aiobotocore.client import AioBaseClient
from aiobotocore.session import AioSession
from pytest_docker.plugin import Services
from yarl import URL

from platform_storage_api.config import S3Config


@pytest.fixture(scope="session")
def _moto_server(docker_ip: str, docker_services: Services) -> URL:
    port = docker_services.port_for("moto", 5000)
    return URL(f"http://{docker_ip}:{port}")


@pytest.fixture
async def moto_server(_moto_server: URL) -> AsyncIterator[URL]:
    yield _moto_server
    await _reset_moto_server(_moto_server)


async def _reset_moto_server(moto_url: URL) -> None:
    async with aiohttp.ClientSession() as client:
        async with client.post(moto_url / "moto-api/reset"):
            pass


@pytest.fixture
def s3_config(moto_server: URL) -> S3Config:
    return S3Config(
        region="us-east-1",
        access_key_id="test-access-key",
        secret_access_key="test-secret-key",
        endpoint_url=str(moto_server),
        bucket_name="storage-metrics",
    )


@pytest.fixture
def _session() -> AioSession:
    return aiobotocore.session.get_session()


@pytest.fixture
async def s3_client(
    moto_server: URL, s3_config: S3Config, _session: AioSession
) -> AsyncIterator[AioBaseClient]:
    async with _session.create_client(
        "s3",
        region_name=s3_config.region,
        aws_access_key_id=s3_config.access_key_id,
        aws_secret_access_key=s3_config.secret_access_key,
        endpoint_url=str(moto_server),
    ) as client:
        yield client
