import asyncio
import logging

from aiohttp import web
from neuro_logging import init_logging, setup_sentry

from platform_storage_api.admission_controller.app import create_app
from platform_storage_api.config import Config


logger = logging.getLogger(__name__)


def main() -> None:
    init_logging()
    config = Config.from_environ()
    logging.info("Loaded config: %r", config)

    setup_sentry(
        health_check_url_path="/api/v1/ping",
        ignore_errors=[web.HTTPNotFound],
    )

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(create_app(config))
    web.run_app(app, host=config.server.host, port=config.server.port)


if __name__ == '__main__':
    main()
