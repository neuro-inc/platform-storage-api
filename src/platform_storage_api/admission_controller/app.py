import logging
from collections.abc import AsyncIterator
from contextlib import AsyncExitStack
from typing import cast

from aiohttp import web
from neuro_auth_client import AuthClient

from platform_storage_api.config import Config, KubeConfig

from ..api import ApiHandler, create_path_resolver, handle_exceptions
from ..fs.local import LocalFileSystem
from ..security import AUTH_CLIENT_KEY
from .api import AdmissionControllerApi
from .app_keys import API_V1_KEY, VOLUME_RESOLVER_KEY
from .volume_resolver import KubeVolumeResolver, create_kube_client


logger = logging.getLogger(__name__)


async def create_app(config: Config) -> web.Application:
    app = web.Application(
        middlewares=[handle_exceptions],
        handler_args={"keepalive_timeout": config.server.keep_alive_timeout_s},
    )

    async def _init_app(app: web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            auth_client = await exit_stack.enter_async_context(
                AuthClient(config.platform.auth_url, config.platform.token)
            )
            app[API_V1_KEY][AUTH_CLIENT_KEY] = auth_client

            fs = await exit_stack.enter_async_context(
                LocalFileSystem(
                    executor_max_workers=config.storage.fs_local_thread_pool_size
                )
            )
            path_resolver = create_path_resolver(config, fs)

            kube_config = cast(KubeConfig, config.kube)
            kube_client = await exit_stack.enter_async_context(
                create_kube_client(kube_config)
            )
            volume_resolver = await exit_stack.enter_async_context(
                KubeVolumeResolver(
                    kube_client=kube_client,
                    path_resolver=path_resolver,
                )
            )
            app[API_V1_KEY][VOLUME_RESOLVER_KEY] = volume_resolver

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)
    app[API_V1_KEY] = api_v1_app

    admission_controller_app = web.Application()
    admission_controller_api = AdmissionControllerApi(api_v1_app, config)
    admission_controller_api.register(admission_controller_app)

    api_v1_app.add_subapp("/mutate", admission_controller_app)
    app.add_subapp("/api/v1", api_v1_app)

    return app
