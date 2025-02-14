import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from typing import cast

from aiohttp import web
from apolo_kube_client.client import kube_client_from_config

from platform_storage_api.admission_controller.api import AdmissionControllerApi
from platform_storage_api.admission_controller.app_keys import (
    VOLUME_RESOLVER_KEY,
)
from platform_storage_api.admission_controller.volume_resolver import (
    KubeVolumeResolver,
)
from platform_storage_api.config import Config, KubeConfig
from platform_storage_api.fs.local import LocalFileSystem
from platform_storage_api.storage import create_path_resolver


logger = logging.getLogger(__name__)


async def create_app(config: Config) -> web.Application:
    app = web.Application(
        handler_args={"keepalive_timeout": config.server.keep_alive_timeout_s},
    )

    async def _init_app(app: web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            fs = await exit_stack.enter_async_context(
                LocalFileSystem(
                    executor_max_workers=config.storage.fs_local_thread_pool_size
                )
            )
            path_resolver = create_path_resolver(config, fs)

            kube_config = cast(KubeConfig, config.kube)
            kube_client = await exit_stack.enter_async_context(
                kube_client_from_config(kube_config)
            )
            volume_resolver = await exit_stack.enter_async_context(
                KubeVolumeResolver(
                    kube_client=kube_client,
                    path_resolver=path_resolver,
                )
            )
            app[VOLUME_RESOLVER_KEY] = volume_resolver

            yield

    app.cleanup_ctx.append(_init_app)

    admission_controller_app = web.Application()
    admission_controller_api = AdmissionControllerApi(app, config)
    admission_controller_api.register(admission_controller_app)

    app.add_subapp("/admission-controller", admission_controller_app)

    return app
