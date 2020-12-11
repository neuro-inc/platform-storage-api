import functools
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

import aiozipkin
import sentry_sdk
from aiohttp import web
from aiozipkin.span import SpanAbc
from sentry_sdk.integrations.aiohttp import AioHttpIntegration

from .config import SentryConfig


Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


CURRENT_TRACER: ContextVar[aiozipkin.Tracer] = ContextVar("CURRENT_TRACER")
CURRENT_SPAN: ContextVar[SpanAbc] = ContextVar("CURRENT_SPAN")


@asynccontextmanager
async def zipkin_tracing_cm(name: str) -> AsyncIterator[SpanAbc]:
    tracer = CURRENT_TRACER.get()
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


def trace(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(func)
    async def tracer(*args: Any, **kwargs: Any) -> Any:
        name = func.__qualname__
        async with tracing_cm(name):
            return await func(*args, **kwargs)

    return tracer


def sentry_init(config: SentryConfig, cluster_name: str) -> None:
    if config.url:
        sentry_sdk.init(
            dsn=str(config.url),
            traces_sample_rate=config.sample_rate,
            integrations=[
                AioHttpIntegration(transaction_style="method_and_path_pattern")
            ],
        )
        sentry_sdk.set_tag("cluster", cluster_name)
        sentry_sdk.set_tag("app", "platformstorageapi")


@web.middleware
async def store_span_middleware(
    request: web.Request, handler: Handler
) -> web.StreamResponse:
    tracer = aiozipkin.get_tracer(request.app)
    span = aiozipkin.request_span(request)
    CURRENT_TRACER.set(tracer)
    CURRENT_SPAN.set(span)
    return await handler(request)
