from enum import Enum
from pathlib import PurePath
from typing import List

import aiohttp.web

from .fs.local import FileStatus, FileSystem
from .storage import Storage


# TODO (A Danshyn 04/23/18): investigate chunked encoding


class ApiHandler:
    def register(self, app):
        app.add_routes((
            aiohttp.web.get('/ping', self.handle_ping),
        ))

    async def handle_ping(self, request):
        return aiohttp.web.Response()


class StorageOperation(str, Enum):
    CREATE = 'CREATE'
    OPEN = 'OPEN'
    LISTSTATUS = 'LISTSTATUS'


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

    def _parse_operation(self, request) -> StorageOperation:
        operation = request.query.get('op', StorageOperation.OPEN.value)
        return StorageOperation(operation)

    async def handle_get(self, request):
        operation = self._parse_operation(request)
        if operation == StorageOperation.OPEN:
            return await self._handle_open(request)
        elif operation == StorageOperation.LISTSTATUS:
            return await self._handle_liststatus(request)
        # TODO (A Danshyn 05/03/18): method not allowed?
        return aiohttp.web.Response(
            status=aiohttp.web.HTTPBadRequest.status_code)

    async def _handle_open(self, request):
        # TODO (A Danshyn 04/23/18): check if exists (likely in some
        # middleware)
        storage_path = self._get_fs_path_from_request(request)
        response = aiohttp.web.StreamResponse(status=200)
        await response.prepare(request)
        await self._storage.retrieve(response, storage_path)
        await response.write_eof()

        return response

    async def _handle_liststatus(self, request):
        storage_path = self._get_fs_path_from_request(request)
        statuses = await self._storage.liststatus(storage_path)
        primitive_statuses = self._convert_file_statuses_to_primitive(
            statuses)
        return aiohttp.web.json_response(primitive_statuses)

    def _convert_file_statuses_to_primitive(
            self, statuses: List[FileStatus]):
        return [
            self._convert_file_status_to_primitive(status)
            for status in statuses]

    def _convert_file_status_to_primitive(self, status: FileStatus):
        return {
            'path': str(status.path),
            'size': status.size,
            'type': status.type,
        }


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
