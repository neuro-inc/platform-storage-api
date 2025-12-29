import asyncio
import logging
import os
import ssl
import tempfile
from base64 import b64decode

import uvloop
from aiohttp import web
from apolo_kube_client import KubeClient
from neuro_logging import init_logging, setup_sentry

from platform_storage_api.admission_controller.app import create_app
from platform_storage_api.config import Config

logger = logging.getLogger(__name__)


async def run() -> None:
    init_logging(health_check_url_path="/ping")
    config = Config.from_environ()
    logging.info("Loaded config: %r", config)

    setup_sentry(
        health_check_url_path="/ping",
        ignore_errors=[web.HTTPNotFound],
    )

    namespace = os.environ["NP_STORAGE_API_K8S_NS"]

    assert config.kube is not None
    # get the necessary certificates from the secrets
    async with KubeClient(config=config.kube) as kube:
        cert_secret_name = config.admission_controller_config.cert_secret_name
        response = await kube.core_v1.secret.get(cert_secret_name, namespace=namespace)
        secrets = response.data
        tls_key = secrets["tls.key"]
        tls_cert = secrets["tls.crt"]

    # create SSL context from obtained certificates
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".crt") as crt_file:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".key") as key_file:
            crt_file.write(b64decode(tls_cert).decode())
            key_file.write(b64decode(tls_key).decode())
            crt_file.flush()
            key_file.flush()
            context.load_cert_chain(
                certfile=crt_file.name,
                keyfile=key_file.name,
            )

    app = await create_app(config)
    runner = web.AppRunner(app)
    done = asyncio.Event()

    try:
        await runner.setup()
        site = web.TCPSite(
            runner,
            config.server.host,
            config.server.port,
            ssl_context=context,
        )
        await site.start()
        await done.wait()  # sleep forever
    except Exception as e:
        logger.exception("Unhandled error")
        raise e
    finally:
        await runner.cleanup()


def main() -> None:
    try:
        uvloop.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
