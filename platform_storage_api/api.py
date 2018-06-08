import asyncio
from enum import Enum
from pathlib import PurePath
import logging
from typing import Optional

import aiohttp.web

from .config import Config
from .fs.local import FileStatus, LocalFileSystem
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
    """Represent all available operations on storage that are exposed via API.

    The CREATE operation handles opening files for writing.
    The OPEN operation handles opening files for reading.
    The LISTSTATUS operation handles non-recursive listing of directories.
    The MKDIRS operation handles recursive creation of directories.
    """
    CREATE = 'CREATE'
    OPEN = 'OPEN'
    LISTSTATUS = 'LISTSTATUS'
    MKDIRS = 'MKDIRS'

    @classmethod
    def values(cls):
        return [item.value for item in cls]


class StorageHandler:
    def __init__(self, storage):
        self._storage = storage

    def register(self, app):
        app.add_routes((
            # TODO (A Danshyn 04/23/18): add some unit test for path matching
            aiohttp.web.put(r'/{path:.*}', self.handle_put),
            aiohttp.web.get(r'/{path:.*}', self.handle_get),
        ))

    def _get_fs_path_from_request(self, request):
        return PurePath('/', request.match_info['path'])

    async def handle_put(self, request):
        operation = self._parse_put_operation(request)
        if operation == StorageOperation.CREATE:
            return await self._handle_create(request)
        elif operation == StorageOperation.MKDIRS:
            return await self._handle_mkdirs(request)
        raise ValueError(f'Illegal operation: {operation}')

    async def _handle_create(self, request):
        # TODO (A Danshyn 04/23/18): check aiohttp default limits
        storage_path = self._get_fs_path_from_request(request)
        await self._storage.store(request.content, storage_path)
        return aiohttp.web.Response(status=201)

    def _parse_operation(self, request) -> Optional[StorageOperation]:
        ops = []

        if 'op' in request.query:
            ops.append(request.query['op'].upper())

        op_values = set(StorageOperation.values())
        param_names = set(name.upper() for name in request.query)
        ops += op_values & param_names

        if len(ops) > 1:
            ops_str = ', '.join(ops)
            raise ValueError(f'Ambiguous operations: {ops_str}')

        if ops:
            return StorageOperation(ops[0])
        return None

    def _parse_put_operation(self, request):
        return self._parse_operation(request) or StorageOperation.CREATE

    def _parse_get_operation(self, request):
        return self._parse_operation(request) or StorageOperation.OPEN

    async def handle_get(self, request):
        operation = self._parse_get_operation(request)
        if operation == StorageOperation.OPEN:
            return await self._handle_open(request)
        elif operation == StorageOperation.LISTSTATUS:
            return await self._handle_liststatus(request)
        raise ValueError(f'Illegal operation: {operation}')

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
        try:
            statuses = await self._storage.liststatus(storage_path)
        except FileNotFoundError:
            return aiohttp.web.Response(
                status=aiohttp.web.HTTPNotFound.status_code)

        primitive_statuses = [
            self._convert_file_status_to_primitive(status)
            for status in statuses]
        return aiohttp.web.json_response(primitive_statuses)

    async def _handle_mkdirs(self, request):
        storage_path = self._get_fs_path_from_request(request)
        try:
            await self._storage.mkdir(storage_path)
        except FileExistsError:
            return aiohttp.web.json_response(
                {'error': 'File exists'},
                status=aiohttp.web.HTTPBadRequest.status_code)
        return aiohttp.web.HTTPCreated()

    def _convert_file_status_to_primitive(self, status: FileStatus):
        return {
            'path': str(status.path),
            'size': status.size,
            'type': status.type,
        }


@aiohttp.web.middleware
async def handle_exceptions(request, handler):
    try:
        return await handler(request)
    except ValueError as e:
        payload = {'error': str(e)}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPBadRequest.status_code)
    except Exception as e:
        msg_str = (
            f'Unexpected exception: {str(e)}. '
            f'Path with query: {request.path_qs}.')
        logging.exception(msg_str)
        payload = {'error': msg_str}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPInternalServerError.status_code)


async def create_app(config: Config, storage: Storage):
    app = aiohttp.web.Application(middlewares=[handle_exceptions])
    app['config'] = config

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    storage_app = aiohttp.web.Application()
    storage_handler = StorageHandler(storage)
    storage_handler.register(storage_app)

    api_v1_app.add_subapp('/storage', storage_app)
    app.add_subapp('/api/v1', api_v1_app)
    return app


def init_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def main():
    init_logging()
    config = Config.from_environ()
    logging.info('Loaded config: %r', config)

    loop = asyncio.get_event_loop()

    fs = LocalFileSystem()
    storage = Storage(fs, config.storage.fs_local_base_path)

    async def _init_storage(app):
        async with fs:
            logging.info('Initializing the storage file system')
            yield
            logging.info('Closing the storage file system')

    app = loop.run_until_complete(create_app(config, storage))
    app.cleanup_ctx.append(_init_storage)
    aiohttp.web.run_app(app, host=config.server.host, port=config.server.port)
