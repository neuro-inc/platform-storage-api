import asyncio
import logging
from enum import Enum
from pathlib import PurePath
from typing import Optional

import aiohttp.web
from aiohttp.web_exceptions import HTTPBadRequest, HTTPUnauthorized
from aiohttp.web_request import Request
from aiohttp_security import check_authorized, check_permission
from async_exit_stack import AsyncExitStack
from neuro_auth_client import AuthClient, Permission, User
from neuro_auth_client.security import AuthScheme, setup_security

from .config import Config
from .fs.local import FileStatus, LocalFileSystem
from .storage import Storage

# TODO (A Danshyn 04/23/18): investigate chunked encoding

logger = logging.getLogger(__name__)


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
    DELETE = 'DELETE'

    @classmethod
    def values(cls):
        return [item.value for item in cls]


class StorageHandler:
    def __init__(self, storage: Storage, config: Config) -> None:
        self._storage = storage
        self._config = config

    def register(self, app):
        app.add_routes((
            # TODO (A Danshyn 04/23/18): add some unit test for path matching
            aiohttp.web.put(r'/{path:.*}', self.handle_put),
            aiohttp.web.get(r'/{path:.*}', self.handle_get),
            aiohttp.web.delete(r'/{path:.*}', self.handle_delete),
        ))

    async def handle_put(self, request: Request):
        operation = self._parse_put_operation(request)
        if operation == StorageOperation.CREATE:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, str(storage_path))
            return await self._handle_create(request, storage_path)
        elif operation == StorageOperation.MKDIRS:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, str(storage_path))
            return await self._handle_mkdirs(storage_path)
        raise ValueError(f'Illegal operation: {operation}')

    async def handle_get(self, request: Request):
        operation = self._parse_get_operation(request)
        if operation == StorageOperation.OPEN:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, str(storage_path))
            return await self._handle_open(request, storage_path)
        elif operation == StorageOperation.LISTSTATUS:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, str(storage_path))
            return await self._handle_liststatus(storage_path)
        raise ValueError(f'Illegal operation: {operation}')

    async def handle_delete(self, request: Request):
        operation = self._parse_delete_operation(request)
        if operation == StorageOperation.DELETE:
            storage_path: PurePath = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, str(storage_path))
            await self._handle_delete(storage_path)
        raise ValueError(f'Illegal operation: {operation}')

    def _get_fs_path_from_request(self, request):
        return PurePath('/', request.match_info['path'])

    async def _handle_create(self, request, storage_path: PurePath):
        # TODO (A Danshyn 04/23/18): check aiohttp default limits
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

    def _parse_put_operation(self, request: Request):
        return self._parse_operation(request) or StorageOperation.CREATE

    def _parse_get_operation(self, request: Request):
        return self._parse_operation(request) or StorageOperation.OPEN

    def _parse_delete_operation(self, request: Request):
        return self._parse_operation(request) or StorageOperation.DELETE

    async def _handle_open(self, request: Request, storage_path: PurePath):
        # TODO (A Danshyn 04/23/18): check if exists (likely in some
        # middleware)
        response = aiohttp.web.StreamResponse(status=200)
        await response.prepare(request)
        await self._storage.retrieve(response, storage_path)
        await response.write_eof()

        return response

    async def _handle_liststatus(self, storage_path: PurePath):
        try:
            statuses = await self._storage.liststatus(storage_path)
        except FileNotFoundError:
            return aiohttp.web.Response(
                status=aiohttp.web.HTTPNotFound.status_code)

        primitive_statuses = [
            self._convert_file_status_to_primitive(status)
            for status in statuses]
        return aiohttp.web.json_response(primitive_statuses)

    async def _handle_mkdirs(self, storage_path: PurePath):
        try:
            await self._storage.mkdir(storage_path)
        except FileExistsError:
            return aiohttp.web.json_response(
                {'error': 'File exists'},
                status=aiohttp.web.HTTPBadRequest.status_code)
        raise aiohttp.web.HTTPCreated()

    async def _handle_delete(self, storage_path: PurePath):
        try:
            await self._storage.remove(storage_path)
        except FileNotFoundError:
            raise aiohttp.web.HTTPNotFound()
        raise aiohttp.web.HTTPNoContent()

    def _convert_file_status_to_primitive(self, status: FileStatus):
        return {
            'path': str(status.path),
            'size': status.size,
            'type': status.type,
        }

    async def _check_user_permissions(self, request, target_path: str) -> None:
        uri = f'storage:/{target_path}'
        if request.method in ('HEAD', 'GET'):
            action = 'read'
        else:  # POST, PUT, PATCH, DELETE
            action = 'write'
        permission = Permission(uri=uri, action=action)
        logger.info(f'Checking {permission}')
        # TODO (Rafa Zubairov): test if user accessing his own data,
        # then use JWT token claims
        try:
            await check_permission(request, action, [permission])
        except HTTPUnauthorized:
            # TODO (Rafa Zubairov): Use tree based approach here
            self._raise_unauthorized()

    def _raise_unauthorized(self) -> None:
        raise HTTPUnauthorized(headers={
            'WWW-Authenticate': f'Bearer realm="{self._config.server.name}"',
        })

    async def _get_user_from_request(self, request: Request) -> User:
        try:
            user_name = await check_authorized(request)
        except ValueError:
            raise HTTPBadRequest()
        except HTTPUnauthorized:
            self._raise_unauthorized()
        return User(name=user_name)


@aiohttp.web.middleware
async def handle_exceptions(request, handler):
    try:
        return await handler(request)
    except ValueError as e:
        payload = {'error': str(e)}
        return aiohttp.web.json_response(
            payload, status=aiohttp.web.HTTPBadRequest.status_code)
    except aiohttp.web.HTTPException:
        raise
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

    async def _init_app(app: aiohttp.web.Application):
        async with AsyncExitStack() as exit_stack:
            logger.info('Initializing Auth Client For Storage API')

            auth_client = await exit_stack.enter_async_context(AuthClient(
                url=config.auth.server_endpoint_url,
                token=config.auth.service_token,
            ))

            await setup_security(
                app=app,
                auth_client=auth_client,
                auth_scheme=AuthScheme.BEARER
            )

            logger.info(f"Auth Client for Storage API Initialized. "
                        f"URL={config.auth.server_endpoint_url}")

            # TODO (Rafa Zubairov): configured service shall ensure that
            # pre-requisites are up and running
            # TODO here we shall test whether AuthClient properly
            # initialized - perform ping
            # TODO here we shall test whether secured-ping works as well
            # TODO in a spin loop we shall do that

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = aiohttp.web.Application()
    api_v1_handler = ApiHandler()
    api_v1_handler.register(api_v1_app)

    storage_app = aiohttp.web.Application()
    storage_handler = StorageHandler(storage, config)
    storage_handler.register(storage_app)

    api_v1_app.add_subapp('/storage', storage_app)
    app.add_subapp('/api/v1', api_v1_app)

    logger.info("Storage API has been initialized, ready to serve.")

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
