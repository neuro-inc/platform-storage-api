import aiohttp.web


class ApiHandler:
    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/ping', self.handle_ping),
        ))

    async def handle_ping(self, request):
        return aiohttp.web.Response()


class StorageHandler:
    def register(self, app):
        app.add_routes((
            aiohttp.web.get(r'/{path:\.*}', self.handle_put),
        ))

    def handle_put(self, request):
        pass


async def create_app():
    app = aiohttp.web.Application()

    storage_app = aiohttp.web.Application()

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    storage_app = aiohttp.web.Application()
    storage_handler = StorageHandler()
    storage_handler.register(storage_app)

    api_v1_app.add_subapp('/storage', storage_app)
    app.add_subapp('/api/v1', api_v1_app)
    return app
