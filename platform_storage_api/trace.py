import functools
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, TypeVar, cast

import aiozipkin
import sentry_sdk
from aiohttp import web
from aiozipkin.span import SpanAbc
from sentry_sdk.integrations.aiohttp import AioHttpIntegration
from yarl import URL


Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


CURRENT_TRACER: ContextVar[aiozipkin.Tracer] = ContextVar("CURRENT_TRACER")
CURRENT_SPAN: ContextVar[SpanAbc] = ContextVar("CURRENT_SPAN")


T = TypeVar("T", bound=Callable[..., Awaitable[Any]])


@asynccontextmanager
async def zipkin_tracing_cm(name: str) -> AsyncIterator[SpanAbc]:
    tracer = CURRENT_TRACER.get(None)  # type: ignore
    if tracer is None:
        # No tracer is set,
        # the call is made from unittest most likely.
        yield None
        return
    try:
        span = CURRENT_SPAN.get()
        child = tracer.new_child(span.context)
    except LookupError:
        child = tracer.new_trace(sampled=False)
    reset_token = CURRENT_SPAN.set(child)
    try:
        with child:
            child.name(name)
            yield child
    finally:
        CURRENT_SPAN.reset(reset_token)


@asynccontextmanager
async def sentry_tracing_cm(
    name: str,
) -> AsyncIterator[Optional[sentry_sdk.tracing.Span]]:
    parent_span = sentry_sdk.Hub.current.scope.span
    if parent_span is None:
        # No tracer is set,
        # the call is made from unittest most likely.
        yield None
    else:
        with parent_span.start_child(op="call", description=name) as child:
            yield child


@asynccontextmanager
async def tracing_cm(name: str) -> AsyncIterator[None]:
    async with zipkin_tracing_cm(name):
        async with sentry_tracing_cm(name):
            yield


def trace(func: T) -> T:
    @functools.wraps(func)
    async def tracer(*args: Any, **kwargs: Any) -> Any:
        name = func.__qualname__
        async with tracing_cm(name):
            return await func(*args, **kwargs)

    return cast(T, tracer)


def notrace(func: T) -> T:
    @functools.wraps(func)
    async def tracer(*args: Any, **kwargs: Any) -> Any:
        with sentry_sdk.Hub.current.configure_scope() as scope:
            transaction = scope.transaction
            if transaction is not None:
                transaction.sampled = False
            return await func(*args, **kwargs)

    return cast(T, tracer)


@web.middleware
async def store_span_middleware(
    request: web.Request, handler: Handler
) -> web.StreamResponse:
    tracer = aiozipkin.get_tracer(request.app)
    span = aiozipkin.request_span(request)
    CURRENT_TRACER.set(tracer)
    CURRENT_SPAN.set(span)
    return await handler(request)


async def create_zipkin_tracer(
    appname: str, host: str, port: int, zipkin_url: URL, sample_rate: float
) -> aiozipkin.Tracer:
    endpoint = aiozipkin.create_endpoint(appname, ipv4=host, port=port)

    return await aiozipkin.create(
        str(zipkin_url / "api/v2/spans"), endpoint, sample_rate=sample_rate
    )


def setup_zipkin(app: web.Application, tracer: aiozipkin.Tracer) -> None:
    aiozipkin.setup(app, tracer)
    app.middlewares.append(store_span_middleware)


def setup_sentry(
    appname: str, cluster_name: str, sentry_url: URL, sample_rate: float
) -> None:
    if sentry_url:
        sentry_sdk.init(
            dsn=str(sentry_url),
            traces_sample_rate=sample_rate,
            integrations=[
                AioHttpIntegration(transaction_style="method_and_path_pattern")
            ],
        )
    sentry_sdk.set_tag("app", appname)
    sentry_sdk.set_tag("cluster", cluster_name)
