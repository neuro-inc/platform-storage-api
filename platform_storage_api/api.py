from pathlib import PurePath

import aiohttp.web

from .fs.local import FileSystem
from .storage import Storage


# TODO (A Danshyn 04/23/18): investigate chunked encoding


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
            # TODO (A Danshyn 04/23/18): add some unit test for path matching
            aiohttp.web.put(r'/{path:.*}', self.handle_put),
            aiohttp.web.get(r'/{path:.*}', self.handle_get),
        ))

    def _get_fs_path_from_request(self, request):
        return PurePath('/', request.match_info['path'])

    async def handle_put(self, request):
        # TODO (A Danshyn 04/23/18): check aiohttp default limits
        storage_path = self._get_fs_path_from_request(request)
        await self._storage.store(request.content, storage_path)
        return aiohttp.web.Response(status=201)

    async def handle_get(self, request):
        # TODO (A Danshyn 04/23/18): check if exists (likely in some
        # middleware)
        storage_path = self._get_fs_path_from_request(request)
        response = aiohttp.web.StreamResponse(status=200)
        await response.prepare(request)
        await self._storage.retrieve(response, storage_path)
        await response.write_eof()

        return response


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
