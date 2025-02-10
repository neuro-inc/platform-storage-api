from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Optional

import aiohttp
from apolo_kube_client.client import KubeClient  # type: ignore[import-untyped]

from platform_storage_api.config import KubeConfig


@asynccontextmanager
async def create_kube_client(
    config: KubeConfig,
    trace_configs: Optional[list[aiohttp.TraceConfig]] = None
) -> AsyncIterator[KubeClient]:
    client = KubeClient(
        base_url=config.endpoint_url,
        namespace=config.namespace,
        cert_authority_path=config.cert_authority_path,
        cert_authority_data_pem=config.cert_authority_data_pem,
        auth_type=config.auth_type,
        auth_cert_path=config.auth_cert_path,
        auth_cert_key_path=config.auth_cert_key_path,
        token=config.token,
        token_path=config.token_path,
        conn_timeout_s=config.client_conn_timeout_s,
        read_timeout_s=config.client_read_timeout_s,
        watch_timeout_s=config.client_watch_timeout_s,
        conn_pool_size=config.client_conn_pool_size,
        trace_configs=trace_configs,
    )
    try:
        await client.init()
        yield client
    finally:
        await client.close()


class KubeService:

    def __init__(self, kube_client: KubeClient):
        self._kube = kube_client
