import errno
import os
import struct
import uuid
from collections.abc import Awaitable, Callable
from time import time as current_time
from typing import Any, Optional
from unittest import mock

import aiohttp
import cbor

from platform_storage_api.api import WSStorageOperation
from platform_storage_api.fs.local import FileStatusType
from tests.integration.conftest import (
    ApiConfig,
    get_filestatus_dict,
    get_liststatus_dict,
)

from .conftest_auth import _User, _UserFactory


def ws_request(
    op: WSStorageOperation, id: int, path: Optional[str] = None, **kwargs: Any
) -> bytes:
    payload = {"op": op, "id": id, **kwargs}
    if path is not None:
        payload["path"] = path
    header = cbor.dumps(payload)
    return struct.pack("!I", len(header) + 4) + header


def parse_ws_response(resp: bytes) -> dict[str, Any]:
    (hsize,) = struct.unpack("!I", resp[:4])
    return cbor.loads(resp[4:hsize])


def get_ws_response_data(resp: bytes) -> bytes:
    (hsize,) = struct.unpack("!I", resp[:4])
    return resp[hsize:]


def assert_ws_response(
    resp: bytes,
    op: WSStorageOperation,
    rop: WSStorageOperation,
    rid: int,
    **kwargs: Any,
) -> None:
    payload = parse_ws_response(resp)
    assert payload["op"] == op
    assert payload["rop"] == rop
    assert payload["rid"] == rid
    curtime = int(current_time())
    assert curtime - 1 <= payload["timestamp"] <= curtime
    for key, value in kwargs.items():
        assert payload[key] == value


def assert_ws_ack(
    resp: bytes, rop: WSStorageOperation, rid: int, **kwargs: Any
) -> None:
    assert_ws_response(resp, WSStorageOperation.ACK, rop, rid, **kwargs)


def assert_ws_error(
    resp: bytes, rop: WSStorageOperation, rid: int, error: str, **kwargs: Any
) -> None:
    assert_ws_response(resp, WSStorageOperation.ERROR, rop, rid, error=error, **kwargs)


class TestStorageWebSocket:
    async def test_create_write(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        base_path = f"/{user.name}/root-{uuid.uuid4()}"
        rel_path = f"path/to/file-{uuid.uuid4()}"

        async with client.ws_connect(
            f"{server_url}{base_path}?op=WEBSOCKET", headers=headers, timeout=10
        ) as ws:
            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 1, rel_path))
            resp = await ws.receive_bytes()
            assert_ws_error(
                resp,
                WSStorageOperation.STAT,
                1,
                os.strerror(errno.ENOENT),
                errno="ENOENT",
            )

            size = 54321
            offset = 12345
            data = b"abcde"

            mtime_min = int(current_time())
            await ws.send_bytes(
                ws_request(WSStorageOperation.CREATE, 100_000, rel_path, size=size)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.CREATE, 100_000)

            await ws.send_bytes(
                ws_request(WSStorageOperation.WRITE, 200_000, rel_path, offset=offset)
                + data
            )
            resp = await ws.receive_bytes()
            mtime_min2 = int(current_time())
            assert_ws_ack(resp, WSStorageOperation.WRITE, 200_000)

            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 200_001, rel_path))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.STAT, 200_001)
            payload = get_filestatus_dict(parse_ws_response(resp))
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": size,
                "modificationTime": mock.ANY,
                "permission": "read",
            }
            assert payload["path"].endswith(f"{base_path}/{rel_path}")
            assert payload["modificationTime"] >= mtime_min
            assert payload["modificationTime"] <= mtime_min2

            await ws.send_bytes(
                ws_request(
                    WSStorageOperation.READ,
                    200_002,
                    rel_path,
                    offset=offset,
                    size=len(data),
                )
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.READ, 200_002)
            assert get_ws_response_data(resp) == data

    async def test_write_create(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        base_path = f"/{user.name}/root-{uuid.uuid4()}"
        rel_path = f"path/to/file-{uuid.uuid4()}"

        async with client.ws_connect(
            f"{server_url}{base_path}?op=WEBSOCKET", headers=headers
        ) as ws:
            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 1, rel_path))
            resp = await ws.receive_bytes()
            assert_ws_error(
                resp,
                WSStorageOperation.STAT,
                1,
                os.strerror(errno.ENOENT),
                errno="ENOENT",
            )

            size = 543_210
            offset = 12345
            data = b"abcde" * 6789
            assert offset + len(data) < size

            mtime_min = int(current_time())
            await ws.send_bytes(
                ws_request(WSStorageOperation.WRITE, 100_000, rel_path, offset=offset)
                + data
            )
            resp = await ws.receive_bytes()
            mtime_min2 = int(current_time())
            assert_ws_ack(resp, WSStorageOperation.WRITE, 100_000)

            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 100_001, rel_path))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.STAT, 100_001)
            payload = get_filestatus_dict(parse_ws_response(resp))
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": offset + len(data),
                "modificationTime": mock.ANY,
                "permission": "read",
            }
            assert payload["path"].endswith(f"{base_path}/{rel_path}")
            assert payload["modificationTime"] >= mtime_min
            assert payload["modificationTime"] <= mtime_min2

            await ws.send_bytes(
                ws_request(WSStorageOperation.CREATE, 200_000, rel_path, size=size)
            )
            resp = await ws.receive_bytes()
            mtime_min3 = int(current_time())
            assert_ws_ack(resp, WSStorageOperation.CREATE, 200_000)

            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 200_001, rel_path))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.STAT, 200_001)
            payload = get_filestatus_dict(parse_ws_response(resp))
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": size,
                "modificationTime": mock.ANY,
                "permission": "read",
            }
            assert payload["path"].endswith(f"{base_path}/{rel_path}")
            assert payload["modificationTime"] >= mtime_min
            assert payload["modificationTime"] <= mtime_min3

            await ws.send_bytes(
                ws_request(
                    WSStorageOperation.READ,
                    200_002,
                    rel_path,
                    offset=offset,
                    size=len(data),
                )
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.READ, 200_002)
            assert get_ws_response_data(resp) == data

    async def test_mkdirs(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:

        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        base_path = f"/{user.name}/root-{uuid.uuid4()}"
        dir_name = f"dir-{uuid.uuid4()}"
        rel_path = f"nested/{dir_name}"

        async with client.ws_connect(
            f"{server_url}{base_path}?op=WEBSOCKET", headers=headers
        ) as ws:
            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 1, rel_path))
            resp = await ws.receive_bytes()
            assert_ws_error(
                resp,
                WSStorageOperation.STAT,
                1,
                os.strerror(errno.ENOENT),
                errno="ENOENT",
            )

            mtime_min = int(current_time())
            await ws.send_bytes(
                ws_request(WSStorageOperation.MKDIRS, 100_000, rel_path)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.MKDIRS, 100_000)

            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 100_001, rel_path))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.STAT, 100_001)
            payload = get_filestatus_dict(parse_ws_response(resp))
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": "read",
            }
            assert payload["path"].endswith(f"{base_path}/{rel_path}")
            assert payload["modificationTime"] >= mtime_min

            await ws.send_bytes(ws_request(WSStorageOperation.LIST, 100_002))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.LIST, 100_002)
            statuses = get_liststatus_dict(parse_ws_response(resp))
            assert statuses == [
                {
                    "path": "nested",
                    "type": str(FileStatusType.DIRECTORY),
                    "length": 0,
                    "modificationTime": mock.ANY,
                    "permission": "read",
                }
            ]
            assert statuses[0]["modificationTime"] >= mtime_min

            await ws.send_bytes(ws_request(WSStorageOperation.LIST, 100_002, "nested"))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.LIST, 100_002)
            statuses = get_liststatus_dict(parse_ws_response(resp))
            assert statuses == [
                {
                    "path": dir_name,
                    "type": str(FileStatusType.DIRECTORY),
                    "length": 0,
                    "modificationTime": mock.ANY,
                    "permission": "read",
                }
            ]
            assert statuses[0]["modificationTime"] >= mtime_min

    async def test_stat_list(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        base_path = f"/{user.name}/root-{uuid.uuid4()}"
        file1_name = f"file-{uuid.uuid4()}"
        dir1_name = f"dir-{uuid.uuid4()}"
        file2_name = f"file-{uuid.uuid4()}"
        dir2_name = f"dir-{uuid.uuid4()}"
        file2_path = f"{dir1_name}/{file2_name}"
        dir2_path = f"{dir1_name}/{dir2_name}"

        async with client.ws_connect(
            f"{server_url}{base_path}?op=WEBSOCKET", headers=headers
        ) as ws:
            mtime_min = int(current_time())
            await ws.send_bytes(
                ws_request(WSStorageOperation.CREATE, 100_000, file1_name, size=123)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.CREATE, 100_000)

            await ws.send_bytes(
                ws_request(WSStorageOperation.MKDIRS, 100_001, dir1_name)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.MKDIRS, 100_001)

            await ws.send_bytes(
                ws_request(WSStorageOperation.CREATE, 100_002, file2_path, size=321)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.CREATE, 100_002)

            await ws.send_bytes(
                ws_request(WSStorageOperation.MKDIRS, 100_003, dir2_path)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.MKDIRS, 100_003)
            mtime_min2 = int(current_time())

        async with client.ws_connect(
            f"{server_url}{base_path}?op=WEBSOCKET", headers=headers
        ) as ws:
            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 200_001))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.STAT, 200_001)
            payload = get_filestatus_dict(parse_ws_response(resp))
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": "read",
            }
            assert payload["path"].endswith(base_path)
            assert payload["modificationTime"] >= mtime_min
            assert payload["modificationTime"] <= mtime_min2

            await ws.send_bytes(
                ws_request(WSStorageOperation.STAT, 200_002, file1_name)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.STAT, 200_002)
            payload = get_filestatus_dict(parse_ws_response(resp))
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": 123,
                "modificationTime": mock.ANY,
                "permission": "read",
            }
            assert payload["path"].endswith(f"{base_path}/{file1_name}")
            assert payload["modificationTime"] >= mtime_min
            assert payload["modificationTime"] <= mtime_min2

            await ws.send_bytes(ws_request(WSStorageOperation.STAT, 200_003, dir1_name))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.STAT, 200_003)
            payload = get_filestatus_dict(parse_ws_response(resp))
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": "read",
            }
            assert payload["path"].endswith(f"{base_path}/{dir1_name}")
            assert payload["modificationTime"] >= mtime_min
            assert payload["modificationTime"] <= mtime_min2

            await ws.send_bytes(ws_request(WSStorageOperation.LIST, 300_000))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.LIST, 300_000)
            statuses = get_liststatus_dict(parse_ws_response(resp))
            statuses = sorted(statuses, key=lambda s: s["path"])
            assert statuses == [
                {
                    "path": dir1_name,
                    "type": str(FileStatusType.DIRECTORY),
                    "length": 0,
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
                {
                    "path": file1_name,
                    "type": str(FileStatusType.FILE),
                    "length": 123,
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
            ]

            await ws.send_bytes(ws_request(WSStorageOperation.LIST, 300_001, dir1_name))
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.LIST, 300_001)
            statuses = get_liststatus_dict(parse_ws_response(resp))
            statuses = sorted(statuses, key=lambda s: s["path"])
            assert statuses == [
                {
                    "path": dir2_name,
                    "type": str(FileStatusType.DIRECTORY),
                    "length": 0,
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
                {
                    "path": file2_name,
                    "type": str(FileStatusType.FILE),
                    "length": 321,
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
            ]

            await ws.send_bytes(
                ws_request(WSStorageOperation.LIST, 300_002, file1_name)
            )
            resp = await ws.receive_bytes()
            assert_ws_error(
                resp,
                WSStorageOperation.LIST,
                300_002,
                os.strerror(errno.ENOTDIR),
                errno="ENOTDIR",
            )

    async def test_shared(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user1 = await regular_user_factory()
        user2 = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user2.token}
        file_name = f"file-{uuid.uuid4()}"
        dir_name = f"dir-{uuid.uuid4()}"

        await granter(
            user2.name,
            [{"uri": f"storage://{cluster_name}/{user1.name}/", "action": "read"}],
            user1,
        )

        async with client.ws_connect(
            f"{server_url}/{user1.name}?op=WEBSOCKET", headers=headers
        ) as ws:
            await ws.send_bytes(
                ws_request(WSStorageOperation.CREATE, 400_001, file_name, size=123)
            )
            resp = await ws.receive_bytes()
            assert_ws_error(
                resp, WSStorageOperation.CREATE, 400_001, "Requires writing permission"
            )

            await ws.send_bytes(
                ws_request(WSStorageOperation.WRITE, 400_002, file_name, offset=0) + b""
            )
            resp = await ws.receive_bytes()
            assert_ws_error(
                resp, WSStorageOperation.WRITE, 400_002, "Requires writing permission"
            )

            await ws.send_bytes(
                ws_request(WSStorageOperation.MKDIRS, 400_003, dir_name)
            )
            resp = await ws.receive_bytes()
            assert_ws_error(
                resp, WSStorageOperation.MKDIRS, 400_003, "Requires writing permission"
            )

        await granter(
            user2.name,
            [{"uri": f"storage://{cluster_name}/{user1.name}/", "action": "write"}],
            user1,
        )

        async with client.ws_connect(
            f"{server_url}/{user1.name}?op=WEBSOCKET", headers=headers
        ) as ws:
            await ws.send_bytes(
                ws_request(WSStorageOperation.CREATE, 500_001, file_name, size=123)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.CREATE, 500_001)

            await ws.send_bytes(
                ws_request(WSStorageOperation.WRITE, 500_002, file_name, offset=0) + b""
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.WRITE, 500_002)

            await ws.send_bytes(
                ws_request(WSStorageOperation.MKDIRS, 500_003, dir_name)
            )
            resp = await ws.receive_bytes()
            assert_ws_ack(resp, WSStorageOperation.MKDIRS, 500_003)
