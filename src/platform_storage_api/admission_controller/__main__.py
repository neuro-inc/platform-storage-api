import asyncio
import logging
import os
import ssl
import tempfile
from base64 import b64decode
from typing import cast

from aiohttp import web
from neuro_logging import init_logging, setup_sentry

from platform_storage_api.admission_controller.app import create_app
from platform_storage_api.config import AdmissionControllerTlsConfig, Config


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

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    crt_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix='.crt')
    key_file = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix='.key')

    tls_config = cast(
        AdmissionControllerTlsConfig,
        config.admission_controller_tls_config
    )
    try:
        # extract certificates from the env and store in a temp files
        logger.info(tls_config.tls_cert)
        logger.info(tls_config.tls_key)
        crt_file.write(b64decode(tls_config.tls_cert).decode())
        key_file.write(b64decode(tls_config.tls_key).decode())
        crt_file.close()
        key_file.close()

        context.load_cert_chain(
            certfile=crt_file.name,
            keyfile=key_file.name,
        )

        web.run_app(
            app,
            host=config.server.host,
            port=config.server.port,
            ssl_context=context,
        )

    except Exception as e:
        logger.exception("Unhandled error")
        raise e
    finally:
        os.unlink(crt_file.name)
        os.unlink(key_file.name)


if __name__ == "__main__":
    main()
