import asyncio
import json
import logging
import re
import struct
import time
from collections.abc import AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable
from contextlib import AsyncExitStack
from enum import Enum
from errno import errorcode
from functools import partial
from pathlib import PurePath
from typing import Any, Optional

import aiohttp
import aiohttp_cors
import cbor
import pkg_resources
import uvloop
from aiohttp import web
from aiohttp.web_request import Request
from aiohttp.web_urldispatcher import AbstractRoute
from aiohttp_cors import CorsConfig
from neuro_auth_client import AuthClient
from neuro_auth_client.client import ClientAccessSubTreeView
from neuro_auth_client.security import AuthScheme, setup_security
from neuro_logging import (
    init_logging,
    make_sentry_trace_config,
    make_zipkin_trace_config,
    notrace,
    setup_sentry,
    setup_zipkin,
    setup_zipkin_tracer,
)

from .cache import PermissionsCache
from .config import Config, CORSConfig, StorageMode
from .fs.local import (
    DiskUsageInfo,
    FileStatus,
    FileStatusPermission,
    FileStatusType,
    LocalFileSystem,
)
from .security import AbstractPermissionChecker, AuthAction, PermissionChecker
from .storage import (
    MultipleStoragePathResolver,
    SingleStoragePathResolver,
    Storage,
    StoragePathResolver,
)


uvloop.install()


# TODO (A Danshyn 04/23/18): investigate chunked encoding

logger = logging.getLogger(__name__)

MAX_WS_READ_SIZE = 16 * 2 ** 20  # 16 MiB
MAX_WS_MESSAGE_SIZE = MAX_WS_READ_SIZE + 2 ** 16 + 100


class ApiHandler:
    def register(self, app: web.Application) -> list[AbstractRoute]:
        return app.add_routes((web.get("/ping", self.handle_ping),))

    @notrace
    async def handle_ping(self, request: web.Request) -> web.Response:
        return web.Response()


class StorageOperation(str, Enum):
    """Represent all available operations on storage that are exposed via API.

    The CREATE operation handles opening files for writing.
    The OPEN operation handles opening files for reading.
    The LISTSTATUS operation handles non-recursive listing of directories.
    The GETFILESTATUS operation handles getting statistics for files and directories.
    The MKDIRS operation handles recursive creation of directories.
    The RENAME operation handles moving of files and directories.
    The GETDISKUSAGE operation handles getting total disk usage of the storage.
    The WEBSOCKET operation handles operations via the WebSocket protocol.
    The WEBSOCKET_READ operation handles immutable operations via the WebSocket
    protocol (deprecated).
    The WEBSOCKET_WRITE operation handles mutable operations via the WebSocket
    protocol (deprecated).
    """

    CREATE = "CREATE"
    OPEN = "OPEN"
    LISTSTATUS = "LISTSTATUS"
    GETFILESTATUS = "GETFILESTATUS"
    MKDIRS = "MKDIRS"
    DELETE = "DELETE"
    RENAME = "RENAME"
    WRITE = "WRITE"
    GETDISKUSAGE = "GETDISKUSAGE"
    WEBSOCKET = "WEBSOCKET"
    WEBSOCKET_READ = "WEBSOCKET_READ"
    WEBSOCKET_WRITE = "WEBSOCKET_WRITE"

    @classmethod
    def values(cls) -> list[str]:
        return [item.value for item in cls]


class WSStorageOperation(str, Enum):
    ACK = "ACK"
    ERROR = "ERROR"
    READ = "READ"
    STAT = "STAT"
    LIST = "LIST"
    CREATE = "CREATE"
    WRITE = "WRITE"
    MKDIRS = "MKDIRS"


class StorageHandler:
    def __init__(self, app: web.Application, config: Config) -> None:
        self._app = app
        self._config = config
        self._permission_checker: AbstractPermissionChecker = PermissionChecker(
            app, config
        )
        if config.permission_expiration_interval_s > 0:
            self._permission_checker = PermissionsCache(
                self._permission_checker,
                expiration_interval_s=config.permission_expiration_interval_s,
                forgetting_interval_s=config.permission_forgetting_interval_s,
            )

    def register(self, app: web.Application, cors: CorsConfig) -> None:
        # TODO (A Danshyn 04/23/18): add some unit test for path matching
        path_resource = app.router.add_resource(r"/{path:.*}")
        cors.add(path_resource.add_route("PUT", self.handle_put))
        cors.add(path_resource.add_route("POST", self.handle_post))
        cors.add(path_resource.add_route("HEAD", self.handle_head))
        cors.add(path_resource.add_route("GET", self.handle_get))
        cors.add(path_resource.add_route("DELETE", self.handle_delete))
        cors.add(path_resource.add_route("PATCH", self.handle_patch))

    @property
    def _storage(self) -> Storage:
        return self._app["storage"]

    async def handle_put(self, request: web.Request) -> web.StreamResponse:
        operation = self._parse_put_operation(request)
        if operation == StorageOperation.CREATE:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, storage_path)
            return await self._handle_create(request, storage_path)
        elif operation == StorageOperation.MKDIRS:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, storage_path)
            return await self._handle_mkdirs(storage_path)
        raise ValueError(f"Illegal operation: {operation}")

    async def handle_patch(self, request: web.Request) -> web.StreamResponse:
        operation = self._parse_patch_operation(request)
        if operation == StorageOperation.WRITE:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, storage_path)
            return await self._handle_write(request, storage_path)
        raise ValueError(f"Illegal operation: {operation}")

    def _create_response(self, fstat: FileStatus) -> web.StreamResponse:
        response = web.StreamResponse()
        response.content_length = fstat.size
        response.last_modified = fstat.modification_time  # type: ignore
        response.headers["X-File-Type"] = str(fstat.type)
        response.headers["X-File-Permission"] = fstat.permission.value
        if fstat.type == FileStatusType.FILE:
            response.headers["Accept-Range"] = "bytes"
            response.headers["X-File-Length"] = str(fstat.size)
        return response

    async def handle_head(self, request: web.Request) -> web.StreamResponse:
        storage_path = self._get_fs_path_from_request(request)
        await self._check_user_permissions(request, storage_path)
        try:
            fstat = await self._storage.get_filestatus(storage_path)
        except FileNotFoundError:
            raise web.HTTPNotFound

        return self._create_response(fstat)

    def _accepts_ndjson(self, request: web.Request) -> bool:
        accept = request.headers.get("Accept", "")
        return "application/x-ndjson" in accept

    async def handle_get(self, request: web.Request) -> web.StreamResponse:
        operation = self._parse_get_operation(request)
        if operation == StorageOperation.OPEN:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, storage_path)
            return await self._handle_open(request, storage_path)
        elif operation == StorageOperation.LISTSTATUS:
            storage_path = self._get_fs_path_from_request(request)
            tree = await self._permission_checker.get_user_permissions_tree(
                request, storage_path
            )
            if self._accepts_ndjson(request):
                return await self._handle_iterstatus(request, storage_path, tree)
            else:
                return await self._handle_liststatus(storage_path, tree)
        elif operation == StorageOperation.GETFILESTATUS:
            storage_path = self._get_fs_path_from_request(request)
            action = await self._permission_checker.get_user_permissions(
                request, storage_path
            )
            return await self._handle_getfilestatus(storage_path, action)
        elif operation == StorageOperation.GETDISKUSAGE:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(
                request, storage_path, AuthAction.READ.value
            )
            return await self._handle_getdiskusage()
        elif operation == StorageOperation.WEBSOCKET:
            storage_path = self._get_fs_path_from_request(request)
            tree = await self._permission_checker.get_user_permissions_tree(
                request, storage_path
            )
            return await self._handle_websocket(request, storage_path, tree)
        elif operation == StorageOperation.WEBSOCKET_READ:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(
                request, storage_path, action=AuthAction.READ.value
            )
            return await self._handle_websocket(
                request,
                storage_path,
                ClientAccessSubTreeView(action=AuthAction.READ.value, children={}),
            )
        elif operation == StorageOperation.WEBSOCKET_WRITE:
            storage_path = self._get_fs_path_from_request(request)
            await self._check_user_permissions(
                request, storage_path, action=AuthAction.WRITE.value
            )
            return await self._handle_websocket(
                request,
                storage_path,
                ClientAccessSubTreeView(action=AuthAction.WRITE.value, children={}),
            )
        raise ValueError(f"Illegal operation: {operation}")

    async def handle_delete(self, request: web.Request) -> web.StreamResponse:
        operation = self._parse_delete_operation(request)
        if operation == StorageOperation.DELETE:
            storage_path: PurePath = self._get_fs_path_from_request(request)
            # Microoptimization: non-existing items return 404 regardless of
            # object permissions,
            # no need to wait for user permission checks.
            if not await self._storage.exists(storage_path):
                raise web.HTTPNotFound
            await self._check_user_permissions(request, storage_path)
            if self._accepts_ndjson(request):
                return await self._handle_iterdelete(request, storage_path)
            else:
                await self._handle_delete(storage_path, request)
        raise ValueError(f"Illegal operation: {operation}")

    async def handle_post(self, request: web.Request) -> web.StreamResponse:
        operation = self._parse_post_operation(request)
        if operation == StorageOperation.RENAME:
            storage_path: PurePath = self._get_fs_path_from_request(request)
            await self._check_user_permissions(request, storage_path)
            return await self._handle_rename(storage_path, request)
        raise ValueError(f"Illegal operation: {operation}")

    def _get_fs_path_from_request(self, request: web.Request) -> PurePath:
        user_provided_path = request.match_info.get("path", "")
        return self._storage.sanitize_path(user_provided_path)

    async def _handle_create(
        self, request: web.Request, storage_path: PurePath
    ) -> web.Response:
        # TODO (A Danshyn 04/23/18): check aiohttp default limits
        try:
            await self._storage.store(request.content, storage_path)
        except IsADirectoryError as e:
            raise _http_bad_request("Destination is a directory", errno=e.errno)

        raise web.HTTPCreated

    def _unsupported_headers(
        self, request: web.Request, unsupported: Iterable[str]
    ) -> None:
        for name in unsupported:
            if name in request.headers:
                raise web.HTTPBadRequest(reason=f"Unsupported header {name}")

    def _parse_content_range(self, request: web.Request) -> slice:
        rng_str = request.headers.get("Content-Range")
        if rng_str is None:
            raise web.HTTPBadRequest(reason="Required header Content-Range")
        m = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+|\*)", rng_str)
        if not m:
            raise web.HTTPBadRequest
        start = int(m[1])
        end = int(m[2])
        return slice(start, end + 1)

    async def _handle_write(
        self, request: web.Request, storage_path: PurePath
    ) -> web.Response:
        if request.content_type != "application/octet-stream":
            raise web.HTTPBadRequest(
                reason="Content-Type should be application/octet-stream"
            )
        self._unsupported_headers(
            request, ["If-Match", "If-None-Match", "If-Range", "If-Unmodified-Since"]
        )

        rng = self._parse_content_range(request)

        # TODO (A Danshyn 04/23/18): check aiohttp default limits
        try:
            await self._storage.store(
                request.content,
                storage_path,
                create=False,
                offset=rng.start,
                size=rng.stop - rng.start,
            )
        except FileNotFoundError:
            raise web.HTTPNotFound
        except IsADirectoryError as e:
            raise _http_bad_request("Destination is a directory", errno=e.errno)

        raise web.HTTPOk

    def _parse_operation(self, request: web.Request) -> Optional[StorageOperation]:
        ops = []

        if "op" in request.query:
            ops.append(request.query["op"].upper())

        op_values = set(StorageOperation.values())
        param_names = {name.upper() for name in request.query}
        ops += op_values & param_names

        if len(ops) > 1:
            ops_str = ", ".join(ops)
            raise ValueError(f"Ambiguous operations: {ops_str}")

        if ops:
            return StorageOperation(ops[0])
        return None

    def _parse_put_operation(self, request: web.Request) -> StorageOperation:
        return self._parse_operation(request) or StorageOperation.CREATE

    def _parse_get_operation(self, request: web.Request) -> StorageOperation:
        return self._parse_operation(request) or StorageOperation.OPEN

    def _parse_delete_operation(self, request: web.Request) -> StorageOperation:
        return self._parse_operation(request) or StorageOperation.DELETE

    def _parse_post_operation(self, request: web.Request) -> StorageOperation:
        return self._parse_operation(request) or StorageOperation.RENAME

    def _parse_patch_operation(self, request: web.Request) -> StorageOperation:
        return self._parse_operation(request) or StorageOperation.WRITE

    def _validate_path(self, path: str) -> None:
        if not path:
            return
        parts = path.split("/")
        if ".." in parts:
            raise ValueError(f"path should not contain '..' components: {path!r}")
        if "." in parts:
            raise ValueError(f"path should not contain '.' components: {path!r}")
        if parts and not parts[0]:
            raise ValueError(f"path should be relative: {path!r}")

    async def _handle_websocket(
        self,
        request: web.Request,
        storage_path: PurePath,
        tree: ClientAccessSubTreeView,
    ) -> web.WebSocketResponse:
        if not tree.can_read():
            raise web.HTTPForbidden
        write = tree.can_write()

        ws = web.WebSocketResponse(max_msg_size=MAX_WS_MESSAGE_SIZE)
        await ws.prepare(request)

        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.BINARY:
                if len(msg.data) < 4:
                    await ws.close(code=aiohttp.WSCloseCode.UNSUPPORTED_DATA)
                    break
                try:
                    (hsize,) = struct.unpack("!I", msg.data[:4])
                    payload = cbor.loads(msg.data[4:hsize])
                    op = payload["op"]
                    reqid = payload["id"]
                except Exception as e:
                    await self._ws_send(ws, WSStorageOperation.ERROR, {"error": str(e)})
                    continue
                try:
                    rel_path = payload.get("path", "")
                    self._validate_path(rel_path)
                    path = storage_path / rel_path if rel_path else storage_path
                    await self._handle_websocket_message(
                        ws,
                        storage_path,
                        write,
                        op,
                        reqid,
                        path,
                        payload,
                        msg.data[hsize:],
                    )
                except OSError as e:
                    errmsg = e.strerror or str(e)
                    await self._ws_send_error(ws, op, reqid, errmsg, e.errno)
                except Exception as e:
                    await self._ws_send_error(ws, op, reqid, str(e))
            elif msg.type == aiohttp.WSMsgType.ERROR:
                exc = ws.exception()
                logger.error(
                    f"WS connection closed with exception {exc!s}", exc_info=exc
                )
        return ws

    async def _handle_websocket_message(
        self,
        ws: web.WebSocketResponse,
        storage_path: PurePath,
        write: bool,
        op: str,
        reqid: int,
        path: str,
        payload: dict[str, Any],
        data: bytes,
    ) -> None:
        if op == WSStorageOperation.READ:
            offset = payload["offset"]
            size = payload["size"]
            if size > MAX_WS_READ_SIZE:
                await self._ws_send_error(ws, op, reqid, "Too large read size")
            else:
                data = await self._storage.read(path, offset, size)
                await self._ws_send_ack(ws, op, reqid, data=data)

        elif op == WSStorageOperation.STAT:
            fstat = await self._storage.get_filestatus(path)
            stat_dict = {"FileStatus": self._convert_filestatus_to_primitive(fstat)}
            await self._ws_send_ack(ws, op, reqid, result=stat_dict)

        elif op == WSStorageOperation.LIST:
            statuses = await self._storage.liststatus(path)
            primitive_statuses = {
                "FileStatuses": {
                    "FileStatus": [
                        self._convert_filestatus_to_primitive(s) for s in statuses
                    ]
                }
            }
            await self._ws_send_ack(ws, op, reqid, result=primitive_statuses)

        elif not write:
            await self._ws_send_error(ws, op, reqid, "Requires writing permission")

        elif op == WSStorageOperation.WRITE:
            offset = payload["offset"]
            await self._storage.write(path, offset, data)
            await self._ws_send_ack(ws, op, reqid)

        elif op == WSStorageOperation.CREATE:
            size = payload["size"]
            await self._storage.create(path, size)
            await self._ws_send_ack(ws, op, reqid)

        elif op == WSStorageOperation.MKDIRS:
            await self._storage.mkdir(path)
            await self._ws_send_ack(ws, op, reqid)

        else:
            await self._ws_send_error(ws, op, reqid, "Unknown operation")

    async def _ws_send(
        self,
        ws: web.WebSocketResponse,
        op: WSStorageOperation,
        payload: dict[str, Any],
        data: bytes = b"",
    ) -> None:
        payload = {"op": op.value, **payload}
        header = cbor.dumps(payload)
        await ws.send_bytes(struct.pack("!I", len(header) + 4) + header + data)

    async def _ws_send_ack(
        self,
        ws: web.WebSocketResponse,
        op: str,
        reqid: int,
        *,
        result: dict[str, Any] = {},
        data: bytes = b"",
    ) -> None:
        payload = {"rop": op, "rid": reqid, "timestamp": int(time.time()), **result}
        await self._ws_send(ws, WSStorageOperation.ACK, payload, data)

    async def _ws_send_error(
        self,
        ws: web.WebSocketResponse,
        op: str,
        reqid: int,
        errmsg: str,
        errno: Optional[int] = None,
    ) -> None:
        payload = {
            "rop": op,
            "rid": reqid,
            "timestamp": int(time.time()),
            "error": errmsg,
        }
        if errno is not None:
            payload["errno"] = errorcode.get(errno, errno)
        await self._ws_send(ws, WSStorageOperation.ERROR, payload)

    async def _handle_open(
        self, request: web.Request, storage_path: PurePath
    ) -> web.StreamResponse:
        try:
            fstat = await self._storage.get_filestatus(storage_path)
        except FileNotFoundError:
            raise web.HTTPNotFound
        try:
            rng = request.http_range
            whole = rng.start is rng.stop is None
            start, stop, _ = rng.indices(fstat.size)
            size = stop - start
            if not size and not whole:
                raise ValueError
        except ValueError:
            response = web.StreamResponse(
                status=web.HTTPRequestRangeNotSatisfiable.status_code,
                headers={"Content-Range": f"bytes */{fstat.size}"},
            )
            await response.prepare(request)
            return response

        response = self._create_response(fstat)
        if whole:
            start = size = 0
        else:
            response.set_status(web.HTTPPartialContent.status_code)
            response.headers["Content-Range"] = f"bytes {start}-{stop-1}/{fstat.size}"
            response.content_length = size
        await response.prepare(request)
        await self._storage.retrieve(response, storage_path, start, size or None)
        await response.write_eof()

        return response

    async def _handle_liststatus(
        self, storage_path: PurePath, tree: ClientAccessSubTreeView
    ) -> web.Response:
        try:
            async with self._storage.iterstatus(storage_path) as statuses:
                filtered_statuses = [
                    fstat async for fstat in self._liststatus_filter(statuses, tree)
                ]
        except FileNotFoundError:
            raise web.HTTPNotFound
        except NotADirectoryError as e:
            raise _http_bad_request("Not a directory", errno=e.errno)
        primitive_statuses = {
            "FileStatuses": {
                "FileStatus": [
                    self._convert_filestatus_to_primitive(s) for s in filtered_statuses
                ]
            }
        }

        return web.json_response(primitive_statuses)

    async def _handle_iterstatus(
        self, request: Request, storage_path: PurePath, tree: ClientAccessSubTreeView
    ) -> web.StreamResponse:
        response = web.StreamResponse()
        response.headers["Content-Type"] = "application/x-ndjson"
        handle_error = partial(handle_error_if_streamed, response)
        try:
            async with self._storage.iterstatus(storage_path) as statuses:
                async for fstat in self._liststatus_filter(statuses, tree):
                    if not response.prepared:
                        await response.prepare(request)
                    stat_dict = {
                        "FileStatus": self._convert_filestatus_to_primitive(fstat)
                    }
                    await response.write(json.dumps(stat_dict).encode() + b"\r\n")
        except asyncio.CancelledError:
            raise
        except FileNotFoundError as e:
            await handle_error("Not found", errno=e.errno, error_class=web.HTTPNotFound)
        except NotADirectoryError as e:
            await handle_error("Not a directory", errno=e.errno)
        except Exception as e:
            msg_str = _unknown_error_message(e, request)
            logging.exception(msg_str)
            await handle_error(msg_str, error_class=web.HTTPInternalServerError)
        if not response.prepared:
            await response.prepare(request)
        await response.write_eof()
        return response

    async def _liststatus_filter(
        self, statuses: AsyncIterable[FileStatus], tree: ClientAccessSubTreeView
    ) -> AsyncIterator[FileStatus]:
        can_read = tree.can_read()
        async for status in statuses:
            sub_tree = tree.children.get(str(status.path))
            if sub_tree:
                action = sub_tree.action
            elif can_read:
                action = tree.action
            else:
                continue
            yield status.with_permission(self._convert_action_to_permission(action))

    def _convert_action_to_permission(self, action: str) -> FileStatusPermission:
        if action == AuthAction.LIST.value:
            return FileStatusPermission.READ
        return FileStatusPermission(action)

    async def _handle_getfilestatus(
        self, storage_path: PurePath, action: str
    ) -> web.StreamResponse:
        try:
            fstat = await self._storage.get_filestatus(storage_path)
        except FileNotFoundError:
            raise web.HTTPNotFound

        fstat = fstat.with_permission(self._convert_action_to_permission(action))
        stat_dict = {"FileStatus": self._convert_filestatus_to_primitive(fstat)}
        return web.json_response(stat_dict)

    async def _handle_getdiskusage(
        self,
    ) -> web.StreamResponse:
        try:
            usage = await self._storage.disk_usage()
        except FileNotFoundError:
            raise web.HTTPNotFound

        usage_dict = self._convert_disk_usage_to_primitive(usage)
        return web.json_response(usage_dict)

    async def _handle_mkdirs(self, storage_path: PurePath) -> web.StreamResponse:
        try:
            await self._storage.mkdir(storage_path)
        except FileExistsError as e:
            raise _http_bad_request("File exists", errno=e.errno)
        except NotADirectoryError as e:
            raise _http_bad_request("Predecessor is not a directory", errno=e.errno)
        raise web.HTTPCreated

    async def _handle_delete(
        self, storage_path: PurePath, request: web.Request
    ) -> web.StreamResponse:
        recursive = _get_bool_param(request, "recursive", True)
        try:
            await self._storage.remove(storage_path, recursive=recursive)
        except IsADirectoryError as e:
            raise _http_bad_request("Target is a directory", errno=e.errno)
        raise web.HTTPNoContent

    async def _handle_iterdelete(
        self, request: Request, storage_path: PurePath
    ) -> web.StreamResponse:
        recursive = _get_bool_param(request, "recursive", True)
        response = web.StreamResponse()
        response.headers["Content-Type"] = "application/x-ndjson"
        handle_error = partial(handle_error_if_streamed, response)

        try:
            async for remove_listing in await self._storage.iterremove(
                storage_path, recursive=recursive
            ):
                if not response.prepared:
                    await response.prepare(request)
                listing_dict = {
                    "path": str(remove_listing.path),
                    "is_dir": remove_listing.is_dir,
                }
                await response.write(json.dumps(listing_dict).encode() + b"\r\n")
        except asyncio.CancelledError:
            raise
        except IsADirectoryError as e:
            await handle_error("Target is a directory", e.errno)
        except OSError as e:
            await handle_error(e.strerror, e.errno)
        except Exception as e:
            msg_str = _unknown_error_message(e, request)
            logging.exception(msg_str)
            await handle_error(msg_str, error_class=web.HTTPInternalServerError)
        await response.write_eof()
        return response

    async def _handle_rename(
        self, old: PurePath, request: web.Request
    ) -> web.StreamResponse:
        if "destination" not in request.query:
            raise _http_bad_request("No destination")
        try:
            new = PurePath(request.query["destination"])
            if new.root == "":
                new = old.parent / new
            new = self._storage.sanitize_path(new)
            await self._check_user_permissions(request, new)
            await self._storage.rename(old, new)
        except FileNotFoundError:
            raise web.HTTPNotFound
        except IsADirectoryError as e:
            raise _http_bad_request("Destination is a directory", errno=e.errno)
        except NotADirectoryError as e:
            raise _http_bad_request("Destination is not a directory", errno=e.errno)
        except OSError as e:
            raise _http_bad_request("Incorrect destination", errno=e.errno)
        raise web.HTTPNoContent

    @classmethod
    def _convert_filestatus_to_primitive(cls, status: FileStatus) -> dict[str, Any]:
        return {
            "path": str(status.path),
            "length": status.size,
            "modificationTime": status.modification_time,
            "permission": status.permission.value,
            "type": str(status.type),
        }

    @classmethod
    def _convert_disk_usage_to_primitive(cls, status: DiskUsageInfo) -> dict[str, Any]:
        return {
            "total": status.total,
            "used": status.used,
            "free": status.free,
        }

    async def _check_user_permissions(
        self, request: web.Request, target_path: PurePath, action: str = ""
    ) -> None:
        if not action:
            if request.method in ("HEAD", "GET"):
                action = AuthAction.READ.value
            else:  # POST, PUT, PATCH, DELETE
                action = AuthAction.WRITE.value
        await self._permission_checker.check_user_permissions(
            request, target_path, action
        )


async def handle_error_if_streamed(
    response: web.StreamResponse,
    str_error: str,
    errno: Optional[int] = None,
    error_class: type[web.HTTPError] = web.HTTPBadRequest,
) -> None:
    if response.prepared:
        error_dict = dict(
            error=str_error,
            errno=errorcode[errno]
            if errno is not None and errno in errorcode
            else errno,
        )
        await response.write(json.dumps(error_dict).encode())
    else:
        raise _http_exception(error_class, str_error, errno=errno)


def _http_exception(
    error_class: type[web.HTTPError],
    message: str,
    errno: Optional[int] = None,
    **kwargs: Any,
) -> web.HTTPError:
    error_payload: dict[str, Any] = {"error": message, **kwargs}
    if errno is not None:
        if errno in errorcode:
            error_payload["errno"] = errorcode[errno]
        else:
            error_payload["errno"] = errno
    data = json.dumps(error_payload)
    return error_class(text=data, content_type="application/json")


def _http_bad_request(message: str, **kwargs: Any) -> web.HTTPError:
    return _http_exception(web.HTTPBadRequest, message, **kwargs)


def _unknown_error_message(exc: Exception, request: web.Request) -> str:
    return (
        f"Unexpected exception {exc.__class__.__name__}: {str(exc)}. "
        f"Path with query: {request.path_qs}."
    )


@web.middleware
async def handle_exceptions(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    try:
        return await handler(request)
    except ValueError as e:
        raise _http_bad_request(str(e))
    except OSError as e:
        raise _http_bad_request(e.strerror or str(e), errno=e.errno)
    except web.HTTPException:
        raise
    except Exception as e:
        msg_str = _unknown_error_message(e, request)
        logging.exception(msg_str)
        raise _http_exception(web.HTTPInternalServerError, msg_str)


def _get_bool_param(request: Request, name: str, default: bool = False) -> bool:
    param = request.query.get(name)
    if param is None:
        return default
    param = param.lower()
    if param in ("1", "true"):
        return True
    if param in ("0", "false"):
        return False
    raise ValueError(f'"{name}" request parameter can be "true"/"1" or "false"/"0"')


def _setup_cors(app: aiohttp.web.Application, config: CORSConfig) -> CorsConfig:
    if not config.allowed_origins:
        return aiohttp_cors.setup(app)

    logger.info(f"Setting up CORS with allowed origins: {config.allowed_origins}")
    default_options = aiohttp_cors.ResourceOptions(
        allow_credentials=True,
        expose_headers="*",
        allow_headers="*",
    )
    cors = aiohttp_cors.setup(
        app, defaults={origin: default_options for origin in config.allowed_origins}
    )
    return cors


package_version = pkg_resources.get_distribution("platform-storage-api").version


async def add_version_to_header(request: Request, response: web.StreamResponse) -> None:
    response.headers["X-Service-Version"] = f"platform-storage-api/{package_version}"


def make_tracing_trace_configs(config: Config) -> list[aiohttp.TraceConfig]:
    trace_configs = []

    if config.zipkin:
        trace_configs.append(make_zipkin_trace_config())

    if config.sentry:
        trace_configs.append(make_sentry_trace_config())

    return trace_configs


async def create_app(config: Config) -> web.Application:
    app = web.Application(
        middlewares=[handle_exceptions],
        handler_args=dict(keepalive_timeout=config.server.keep_alive_timeout_s),
    )
    app["config"] = config

    cors = _setup_cors(app, config.cors)

    async def _init_app(app: web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            logger.info("Initializing Auth Client For Storage API")

            auth_client = await exit_stack.enter_async_context(
                AuthClient(
                    config.auth.server_endpoint_url,
                    config.auth.service_token,
                    make_tracing_trace_configs(config),
                )
            )

            await setup_security(
                app=app, auth_client=auth_client, auth_scheme=AuthScheme.BEARER
            )

            app["api_v1"]["auth_client"] = auth_client

            logger.info(
                f"Auth Client for Storage API Initialized. "
                f"URL={config.auth.server_endpoint_url}"
            )

            fs = await exit_stack.enter_async_context(
                LocalFileSystem(
                    executor_max_workers=config.storage.fs_local_thread_pool_size
                )
            )
            if config.storage.mode == StorageMode.SINGLE:
                path_resolver: StoragePathResolver = SingleStoragePathResolver(
                    config.storage.fs_local_base_path
                )
            else:
                path_resolver = MultipleStoragePathResolver(
                    fs, config.storage.fs_local_base_path
                )
            storage = Storage(path_resolver, fs)
            app["api_v1"]["storage"] = storage

            # TODO (Rafa Zubairov): configured service shall ensure that
            # pre-requisites are up and running
            # TODO here we shall test whether AuthClient properly
            # initialized - perform ping
            # TODO here we shall test whether secured-ping works as well
            # TODO in a spin loop we shall do that

            yield

    app.cleanup_ctx.append(_init_app)

    api_v1_app = web.Application()
    api_v1_handler = ApiHandler()
    probes_routes = api_v1_handler.register(api_v1_app)
    app["api_v1"] = api_v1_app

    storage_app = web.Application()
    storage_handler = StorageHandler(api_v1_app, config)
    storage_handler.register(storage_app, cors)

    api_v1_app.add_subapp("/storage", storage_app)
    app.add_subapp("/api/v1", api_v1_app)

    app.on_response_prepare.append(add_version_to_header)

    if config.zipkin:
        setup_zipkin(app, skip_routes=probes_routes)

    logger.info("Storage API has been initialized, ready to serve.")

    return app


def setup_tracing(config: Config) -> None:
    if config.zipkin:
        setup_zipkin_tracer(
            config.zipkin.app_name,
            config.server.host,
            config.server.port,
            config.zipkin.url,
            config.zipkin.sample_rate,
        )

    if config.sentry:
        setup_sentry(
            config.sentry.dsn,
            app_name=config.sentry.app_name,
            cluster_name=config.sentry.cluster_name,
            sample_rate=config.sentry.sample_rate,
            exclude=[FileNotFoundError],
        )


def main() -> None:
    init_logging()
    config = Config.from_environ()
    logging.info("Loaded config: %r", config)

    setup_tracing(config)

    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(create_app(config))
    web.run_app(app, host=config.server.host, port=config.server.port)
