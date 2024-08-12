from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from typing import NoReturn

import aiobotocore
import aiobotocore.session
from neuro_admin_client import AdminClient
from neuro_logging import init_logging, new_trace, setup_sentry

from .aws import create_async_s3_client
from .config import Config, EnvironConfigFactory, StorageMode
from .fs.local import FileSystem, LocalFileSystem
from .s3_storage import StorageMetricsAsyncS3Storage
from .storage import (
    MultipleStoragePathResolver,
    SingleStoragePathResolver,
    StoragePathResolver,
)
from .storage_usage import StorageUsageService

LOGGER = logging.getLogger(__name__)


@dataclass
class App:
    storage_usage_service: StorageUsageService

    @new_trace
    async def upload_storage_usage(self) -> None:
        LOGGER.info("Starting storage usage collection")
        await self.storage_usage_service.upload_storage_usage()
        LOGGER.info("Finished storage usage collection")


def create_path_resolver(config: Config, fs: FileSystem) -> StoragePathResolver:
    if config.storage.mode == StorageMode.SINGLE:
        return SingleStoragePathResolver(config.storage.fs_local_base_path)
    return MultipleStoragePathResolver(
        fs,
        config.storage.fs_local_base_path,
        config.storage.fs_local_base_path / config.platform.cluster_name,
    )


@asynccontextmanager
async def create_app(config: Config) -> AsyncIterator[App]:
    async with AsyncExitStack() as exit_stack:
        session = aiobotocore.session.get_session()
        s3_client = await exit_stack.enter_async_context(
            create_async_s3_client(session, config.aws)
        )

        storage_metrics_s3_storage = StorageMetricsAsyncS3Storage(
            s3_client, config.aws.metrics_s3_bucket_name
        )

        admin_client = await exit_stack.enter_async_context(
            AdminClient(
                base_url=config.platform.admin_url,
                service_token=config.platform.token,
            )
        )

        fs = await exit_stack.enter_async_context(
            LocalFileSystem(
                executor_max_workers=config.storage.fs_local_thread_pool_size
            )
        )

        path_resolver = create_path_resolver(config, fs)

        storage_usage_service = StorageUsageService(
            config=config,
            admin_client=admin_client,
            storage_metrics_s3_storage=storage_metrics_s3_storage,
            fs=fs,
            path_resolver=path_resolver,
        )

        yield App(
            storage_usage_service=storage_usage_service,
        )


class GracefulExitError(SystemExit):
    code = 1


def _raise_graceful_exit() -> NoReturn:
    raise GracefulExitError()


def setup() -> None:
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, _raise_graceful_exit)
    loop.add_signal_handler(signal.SIGTERM, _raise_graceful_exit)


def cleanup() -> None:
    loop = asyncio.get_event_loop()
    loop.remove_signal_handler(signal.SIGINT)
    loop.remove_signal_handler(signal.SIGTERM)


async def run(config: Config) -> None:
    setup()

    try:
        async with create_app(config) as app:
            await app.upload_storage_usage()
    finally:
        cleanup()


def main() -> None:
    init_logging()

    setup_sentry()

    config = EnvironConfigFactory().create()
    LOGGER.info("Loaded config: %s", config)

    asyncio.run(run(config))
