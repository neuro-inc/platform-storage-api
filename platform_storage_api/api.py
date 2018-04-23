from pathlib import PurePath

import aiohttp.web

from .fs.local import FileSystem, copy_streams
from .storage import Storage


class ApiHandler:
    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/ping', self.handle_ping),
        ))

    async def handle_ping(self, request):
        return aiohttp.web.Response()


class StorageHandler:
    def __init__(self, fs):
        self._fs = fs
        # TODO (A Danshyn 04/23/18): drop the hardcoded path
        self._storage = Storage(fs, '/tmp/np_storage')

    def register(self, app):
        app.add_routes((
            # TODO (A Danshyn 04/23/18): add some unit test for this
            aiohttp.web.put(r'/{path:.*}', self.handle_put),
        ))

    def _get_fs_path_from_request(self, request):
        return PurePath('/', request.match_info['path'])

    async def handle_put(self, request):
        storage_path = self._get_fs_path_from_request(request)
        await self._storage.store(request.content, storage_path)
        return aiohttp.web.Response(status=201)

    async def handle_get(self, request):
        # TODO (A Danshyn 04/23/18): check if exists (likely in some
        # middleware)
        return aiohttp.web.Response(status=200)


async def create_app(fs: FileSystem):
    app = aiohttp.web.Application()

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    storage_app = aiohttp.web.Application()
    storage_handler = StorageHandler(fs=fs)
    storage_handler.register(storage_app)

    api_v1_app.add_subapp('/storage', storage_app)
    app.add_subapp('/api/v1', api_v1_app)
    return app
