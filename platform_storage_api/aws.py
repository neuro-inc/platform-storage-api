from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from typing import Any

import aiobotocore
import aiobotocore.client
import aiobotocore.session
import botocore
import botocore.client
import botocore.session

from .config import AWSConfig


@contextmanager
def create_s3_client(
    session: botocore.session.Session, config: AWSConfig
) -> Iterator[botocore.client.BaseClient]:
    client = session.create_client("s3", **_create_s3_client_kwargs(config))
    yield client
    client.close()


@asynccontextmanager
async def create_async_s3_client(
    session: aiobotocore.session.AioSession, config: AWSConfig
) -> AsyncIterator[aiobotocore.client.AioBaseClient]:
    async with session.create_client(
        "s3", **_create_s3_client_kwargs(config)
    ) as client:
        yield client


def _create_s3_client_kwargs(config: AWSConfig) -> dict[str, Any]:
    kwargs: dict[str, Any] = {
        "region_name": config.region,
        "config": botocore.config.Config(
            retries={"mode": "standard"},  # 3 retries by default
        ),
    }
    if config.access_key_id:
        kwargs["aws_access_key_id"] = config.access_key_id
    if config.secret_access_key:
        kwargs["aws_secret_access_key"] = config.secret_access_key
    if config.s3_endpoint_url:
        kwargs["endpoint_url"] = config.s3_endpoint_url
    return kwargs
