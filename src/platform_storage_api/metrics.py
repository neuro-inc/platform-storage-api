import asyncio
import logging
from collections.abc import Iterator
from contextlib import ExitStack, contextmanager

import botocore
import botocore.client
import botocore.config
import botocore.session
from fastapi import FastAPI, Request
from fastapi.responses import Response
from neuro_logging import init_logging
from prometheus_client import CollectorRegistry
from prometheus_client.exposition import choose_encoder

from .config import EnvironConfigFactory, MetricsConfig
from .s3 import create_s3_client
from .s3_storage import StorageMetricsS3Storage
from .storage_usage import StorageUsageCollector


@contextmanager
def create_app(config: MetricsConfig) -> Iterator[FastAPI]:
    with ExitStack() as exit_stack:
        session = botocore.session.get_session()
        s3_client = exit_stack.enter_context(
            create_s3_client(session=session, config=config.s3)
        )

        storage_metrics_s3_storage = StorageMetricsS3Storage(
            s3_client,
            bucket_name=config.s3.bucket_name,
            key_prefix=config.s3.key_prefix,
        )

        collector = StorageUsageCollector(
            config=config.s3, storage_metrics_s3_storage=storage_metrics_s3_storage
        )
        registry = CollectorRegistry()
        registry.register(collector)

        app = FastAPI(debug=False)

        @app.get("/metrics/")
        async def metrics(request: Request) -> Response:
            encoder, content_type = choose_encoder(request.headers.get("accept", ""))
            output = await asyncio.to_thread(encoder, registry)
            return Response(output, media_type=content_type)

        @app.get("/ping")
        async def ping() -> Response:
            return Response("Pong")

        yield app


def main() -> None:
    import uvicorn

    init_logging()

    config = EnvironConfigFactory().create_metrics()
    logging.info("Loaded config: %r", config)
    with create_app(config) as app:
        uvicorn.run(
            app,
            host=config.server.host,
            port=config.server.port,
            proxy_headers=True,
            log_config=None,
        )


if __name__ == "__main__":
    main()
