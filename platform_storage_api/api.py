import aiohttp.web


async def create_app():
    app = aiohttp.web.Application()
    app.add_routes((
        aiohttp.web.get('/ping', handle_ping),
    ))
    return app


async def handle_ping(request):
    return aiohttp.web.Response()
