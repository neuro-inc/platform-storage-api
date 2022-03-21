import calendar
import json
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path, PurePath
from time import time as current_time
from typing import Any
from unittest import mock

import aiohttp
import aiohttp.web
import pytest
import pytest_asyncio

from platform_storage_api.config import Config
from platform_storage_api.fs.local import FileStatusType

from tests.integration.auth import _User, _UserFactory
from tests.integration.conftest import (
    ApiConfig,
    get_filestatus_dict,
    get_liststatus_dict,
    status_iter_response_to_list,
)


def make_url(server_url: str, user: _User, path: str) -> str:
    return f"{server_url}/{user.name}/{path}"


@pytest_asyncio.fixture()
async def alice(regular_user_factory: _UserFactory) -> _User:
    return await regular_user_factory()


@pytest_asyncio.fixture()
async def bob(regular_user_factory: _UserFactory) -> _User:
    return await regular_user_factory()


class TestApi:
    async def test_ping(self, api: ApiConfig, client: aiohttp.ClientSession) -> None:
        async with client.head(api.ping_url) as response:
            assert response.status == 200

        async with client.get(api.ping_url) as response:
            assert response.status == 200

    async def test_ping_includes_version(
        self, api: ApiConfig, client: aiohttp.ClientSession
    ) -> None:
        async with client.get(api.ping_url) as resp:
            assert resp.status == 200
            assert "platform-storage-api" in resp.headers["X-Service-Version"]


class TestStorage:
    @pytest.mark.parametrize("method", ["GET", "PUT", "POST", "DELETE", "HEAD"])
    async def test_options_allowed(
        self, server_url: str, client: aiohttp.ClientSession, method: str
    ) -> None:
        headers = {
            "Origin": "http://localhost:8000",
            "Access-Control-Request-Method": method,
        }

        async with client.options(f"{server_url}/user", headers=headers) as response:
            assert response.status == 200, await response.text()

    async def test_options_forbidden(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
    ) -> None:
        headers = {
            "Origin": "http://otherhost:8000",
            "Access-Control-Request-Method": "GET",
        }

        async with client.options(f"{server_url}/user", headers=headers) as response:
            assert response.status == 403, await response.text()

    async def test_put_head_get(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        payload = b"test"
        mtime_min = int(current_time())

        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201, await response.text()

        async with client.head(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == len(payload)
            last_modified = response.headers["Last-Modified"]
            mtime = calendar.timegm(
                time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            )
            assert mtime >= mtime_min
            assert mtime <= int(current_time())
            assert response.headers["X-File-Type"] == "FILE"
            assert response.headers["X-File-Permission"] == "read"
            assert response.headers["X-File-Length"] == str(len(payload))
            assert response.headers["Accept-Range"] == "bytes"
            assert "Content-Range" not in response.headers

        async with client.get(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == len(payload)
            assert response.headers["Last-Modified"] == last_modified
            assert response.headers["X-File-Type"] == "FILE"
            assert response.headers["X-File-Permission"] == "read"
            assert response.headers["X-File-Length"] == str(len(payload))
            assert response.headers["Accept-Range"] == "bytes"
            assert "Content-Range" not in response.headers
            result_payload = await response.read()
            assert result_payload == payload

    async def test_get_partial(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        payload = b"test content"

        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201

        headers["Range"] = "bytes=5-8"
        async with client.get(url, headers=headers) as response:
            assert response.status == 206
            assert response.content_length == 4
            assert response.headers["X-File-Type"] == "FILE"
            assert response.headers["X-File-Permission"] == "read"
            assert response.headers["X-File-Length"] == "12"
            assert response.headers["Content-Range"] == "bytes 5-8/12"
            result_payload = await response.read()
            assert result_payload == b"cont"

        headers["Range"] = "bytes=5-"
        async with client.get(url, headers=headers) as response:
            assert response.status == 206
            assert response.content_length == 7
            assert response.headers["X-File-Type"] == "FILE"
            assert response.headers["X-File-Permission"] == "read"
            assert response.headers["X-File-Length"] == "12"
            assert response.headers["Content-Range"] == "bytes 5-11/12"
            result_payload = await response.read()
            assert result_payload == b"content"

        headers["Range"] = "bytes=-4"
        async with client.get(url, headers=headers) as response:
            assert response.status == 206
            assert response.content_length == 4
            assert response.headers["X-File-Type"] == "FILE"
            assert response.headers["X-File-Permission"] == "read"
            assert response.headers["X-File-Length"] == "12"
            assert response.headers["Content-Range"] == "bytes 8-11/12"
            result_payload = await response.read()
            assert result_payload == b"tent"

    async def test_get_partial_invalid_range(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        payload = b"test content"

        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201

        headers["Range"] = "bytes=5-4"
        async with client.get(url, headers=headers) as response:
            assert response.status == 416, await response.text()

        headers["Range"] = "bytes=12-"
        async with client.get(url, headers=headers) as response:
            assert response.status == 416, await response.text()

        headers["Range"] = "chars=5-8"
        async with client.get(url, headers=headers) as response:
            assert response.status == 416, await response.text()

    @pytest_asyncio.fixture()
    async def put_test_file(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> AsyncIterator[tuple[str, dict[str, str]]]:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        payload = b"test content"

        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201

        yield (url, headers)

    async def test_patch(
        self,
        client: aiohttp.ClientSession,
        put_test_file: tuple[str, dict[str, str]],
    ) -> None:
        url, headers = put_test_file

        headers2 = {**headers, "Content-Range": "bytes 5-8/12"}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 200

        async with client.get(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == 12
            assert response.headers["X-File-Length"] == "12"
            result_payload = await response.read()
            assert result_payload == b"test spament"

    async def test_patch_unknown_size(
        self,
        client: aiohttp.ClientSession,
        put_test_file: tuple[str, dict[str, str]],
    ) -> None:
        url, headers = put_test_file

        headers2 = {**headers, "Content-Range": "bytes 5-8/*"}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 200

        async with client.get(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == 12
            assert response.headers["X-File-Length"] == "12"
            result_payload = await response.read()
            assert result_payload == b"test spament"

    async def test_patch_past_the_end(
        self,
        client: aiohttp.ClientSession,
        put_test_file: tuple[str, dict[str, str]],
    ) -> None:
        url, headers = put_test_file

        headers2 = {**headers, "Content-Range": "bytes 15-18/20"}
        async with client.patch(url, headers=headers2, data=b"ham") as response:
            assert response.status == 200

        async with client.get(url, headers=headers) as response:
            assert response.status == 200
            result_payload = await response.read()
            assert response.content_length == 18
            assert response.headers["X-File-Length"] == "18"
            assert result_payload == b"test content\0\0\0ham"

    async def test_patch_invalid_headers(
        self,
        client: aiohttp.ClientSession,
        put_test_file: tuple[str, dict[str, str]],
    ) -> None:
        url, headers = put_test_file

        # No Content-Range
        async with client.patch(url, headers=headers, data=b"spam") as response:
            assert response.status == 400, await response.text()

        # Invalid Content-Range
        headers2 = {**headers, "Content-Range": "bytes 5-8"}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 400, await response.text()

        headers["Content-Range"] = "bytes 5-8/12"
        headers2 = {**headers, "Content-Type": "text/plain"}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 400, await response.text()

        headers2 = {**headers, "If-Match": '"xyzzy"'}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 400, await response.text()

        headers2 = {**headers, "If-None-Match": "*"}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 400, await response.text()

        headers2 = {**headers, "If-Unmodified-Since": "Sat, 29 Oct 1994 19:43:31 GMT"}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 400, await response.text()

        headers2 = {**headers, "If-Range": "Wed, 21 Oct 2015 07:28:00 GMT"}
        async with client.patch(url, headers=headers2, data=b"spam") as response:
            assert response.status == 400, await response.text()

    async def test_head_non_existent(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/non-existent"

        async with client.head(url, headers=headers) as response:
            assert response.status == 404

    async def test_get_non_existent(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/non-existent"

        async with client.get(url, headers=headers) as response:
            assert response.status == 404

    async def test_patch_non_existent(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {
            "Authorization": "Bearer " + user.token,
            "Content-Range": "bytes 5-8/12",
        }
        url = f"{server_url}/{user.name}/non-existent"

        async with client.patch(url, headers=headers, data=b"spam") as response:
            assert response.status == 404

    async def test_put_illegal_op(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        params = {"op": "OPEN"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = "Illegal operation: OPEN"
            assert payload["error"] == expected_error

    async def test_get_illegal_op(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        params = {"op": "CREATE"}
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = "Illegal operation: CREATE"
            assert payload["error"] == expected_error

    async def test_iterstatus(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        payload = b"test"
        mtime_min = int(current_time())
        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201

        params = {"op": "LISTSTATUS"}
        headers["Accept"] = "application/x-ndjson"
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            file_status = statuses[0]

            assert file_status == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": len(payload),
                "modificationTime": mock.ANY,
                "permission": "manage",
            }
            assert file_status["path"] == file_name
            assert file_status["modificationTime"] >= mtime_min

    async def test_iterstatus_empty_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to/emptydir"
        dir_url = f"{server_url}/{dir_path}"

        params = {"op": "MKDIRS"}
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        params = {"op": "LISTSTATUS"}
        headers["Accept"] = "application/x-ndjson"
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            assert statuses == []

    async def test_iterstatus_no_op_param_no_equals(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"
        payload = b"test"

        mtime_min = int(current_time())
        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201

        headers["Accept"] = "application/x-ndjson"
        async with client.get(dir_url + "?liststatus", headers=headers) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            file_status = statuses[0]

            assert file_status == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": len(payload),
                "modificationTime": mock.ANY,
                "permission": "manage",
            }
            assert file_status["path"] == file_name
            assert file_status["modificationTime"] >= mtime_min

    async def test_iterstatus_non_existent_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {
            "Authorization": "Bearer " + user.token,
            "Accept": "application/x-ndjson",
        }
        dir_url = f"{server_url}/{user.name}/non-existent"

        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 404

    async def test_iterstatus_file(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 201

        params = {"op": "LISTSTATUS"}
        headers["Accept"] = "application/x-ndjson"
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Not a directory"

    async def test_disk_usage(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        admin_token: str,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}

        params = {"op": "MKDIRS"}
        async with client.put(
            server_url + f"/{user.name}/", headers=headers, params=params
        ) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        params = {"op": "GETDISKUSAGE"}
        async with client.get(
            server_url + f"/{user.name}/", headers=headers, params=params
        ) as response:
            assert response.status == 200
            res = await response.json()

            # Cannot test exact values here
            assert "total" in res
            assert "used" in res
            assert "free" in res

    async def test_liststatus(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        payload = b"test"
        mtime_min = int(current_time())
        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201

        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 200
            statuses = get_liststatus_dict(await response.json())
            file_status = statuses[0]

            assert file_status == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": len(payload),
                "modificationTime": mock.ANY,
                "permission": "manage",
            }
            assert file_status["path"] == file_name
            assert file_status["modificationTime"] >= mtime_min

    async def test_liststatus_empty_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to/emptydir"
        dir_url = f"{server_url}/{dir_path}"

        params = {"op": "MKDIRS"}
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 200
            statuses = get_liststatus_dict(await response.json())
            assert statuses == []

    async def test_liststatus_no_op_param_no_equals(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"
        payload = b"test"

        mtime_min = int(current_time())
        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == 201

        async with client.get(dir_url + "?liststatus", headers=headers) as response:
            statuses = get_liststatus_dict(await response.json())
            file_status = statuses[0]

            assert file_status == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": len(payload),
                "modificationTime": mock.ANY,
                "permission": "manage",
            }
            assert file_status["path"] == file_name
            assert file_status["modificationTime"] >= mtime_min

    async def test_ambiguous_operations_with_op(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/"
        async with client.get(
            dir_url + "?op=liststatus&open", headers=headers
        ) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert "Ambiguous operations" in payload["error"]

    async def test_ambiguous_operations(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/"
        async with client.get(
            dir_url + "?liststatus&open", headers=headers
        ) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert "Ambiguous operations" in payload["error"]

    async def test_unknown_operation(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/"
        async with client.get(dir_url + "?op=unknown", headers=headers) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = "'UNKNOWN' is not a valid StorageOperation"
            assert payload["error"] == expected_error

    async def test_liststatus_non_existent_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/non-existent"

        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 404

    async def test_liststatus_file(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 201

        params = {"op": "LISTSTATUS"}
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Not a directory"
            assert payload["errno"] == "ENOTDIR"

    async def test_mkdirs(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        dir_url = f"{server_url}{path_str}"

        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        params = {"op": "MKDIRS"}
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code

    async def test_mkdirs_existent_dir(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        dir_url = f"{server_url}{path_str}"

        params = {"op": "MKDIRS"}
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    async def test_mkdirs_existent_file(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "File exists"
            assert payload["errno"] == "EEXIST"

    async def test_mkdirs_existent_parent_file(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        dir_url = f"{server_url}{path_str}/dir"
        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Predecessor is not a directory"
            assert payload["errno"] == "ENOTDIR"

    async def test_put_target_is_directory(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Destination is a directory"
            assert payload["errno"] == "EISDIR"

    async def test_head_target_is_directory(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.head(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == 0
            assert response.headers["X-File-Type"] == "DIRECTORY"
            assert response.headers["X-File-Permission"] == "read"
            assert "X-File-Length" not in response.headers
            assert "Accept-Range" not in response.headers

    async def test_get_target_is_directory(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.get(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == 0
            assert response.headers["X-File-Type"] == "DIRECTORY"
            assert response.headers["X-File-Permission"] == "read"
            assert "X-File-Length" not in response.headers
            assert "Accept-Range" not in response.headers
            payload = await response.read()
            assert payload == b""

    async def test_delete_non_existent(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        async with client.delete(url, headers=headers) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    async def test_delete_file(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        payload = b"test"

        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.delete(url, headers=headers) as response:
            assert response.status == aiohttp.web.HTTPNoContent.status_code

    async def test_iterdelete_file(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        payload = b"test"

        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        headers["Accept"] = "application/x-ndjson"
        async with client.delete(url, headers=headers) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = [json.loads(line) async for line in response.content]
            file_status = statuses[0]

            assert file_status == {
                "path": path_str,
                "is_dir": False,
            }

    async def test_iterdelete_dir_with_file(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path_str = f"/{user.name}/new/nested/to_delete"
        file_path_str = f"{dir_path_str}/{uuid.uuid4()}"
        payload = b"test"

        async with client.put(
            f"{server_url}{file_path_str}", headers=headers, data=payload
        ) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        headers["Accept"] = "application/x-ndjson"
        async with client.delete(
            f"{server_url}{dir_path_str}", headers=headers
        ) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = [json.loads(line) async for line in response.content]

            assert statuses[0] == {
                "path": file_path_str,
                "is_dir": False,
            }

            assert statuses[1] == {
                "path": dir_path_str,
                "is_dir": True,
            }

    @pytest.mark.parametrize("use_stream_response", [False, True])
    async def test_cant_delete_folder_without_non_recursive(
        self,
        use_stream_response: bool,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        params_mkdir = {"op": "MKDIRS"}
        params_delete = {"recursive": "false"}
        path_str = f"/{user.name}/new/nested/foobar222/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"

        async with client.put(url, headers=headers, params=params_mkdir) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        if use_stream_response:
            headers["Accept"] = "application/x-ndjson"

        async with client.delete(
            url, headers=headers, params=params_delete
        ) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Target is a directory"
            assert payload["errno"] == "EISDIR"

    async def test_can_delete_folder_with_recursive_set(
        self,
        server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        api: ApiConfig,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        params_mkdir = {"op": "MKDIRS"}
        params_delete = {"recursive": "true"}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"

        async with client.put(url, headers=headers, params=params_mkdir) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.delete(
            url, headers=headers, params=params_delete
        ) as response:
            assert response.status == aiohttp.web.HTTPNoContent.status_code


class TestGetFileStatus:

    payload = b"test"
    len_payload = len(payload)
    file1 = "file1.txt"
    file2 = "file2.txt"
    dir3 = "dir3"
    dir3_file3 = f"{dir3}/file3.txt"
    dir3_dir4 = f"{dir3}/dir4"

    @classmethod
    async def put_file(
        cls, server_url: str, client: aiohttp.ClientSession, user: _User, path: str
    ) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = make_url(server_url, user, path)
        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    async def put_dir(
        cls, server_url: str, client: aiohttp.ClientSession, user: _User, path: str
    ) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = make_url(server_url, user, path)
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    def get_filestatus(
        cls,
        user: _User,
        path: str,
        server_url: str,
        client: aiohttp.ClientSession,
        file_owner: _User,
    ) -> AbstractAsyncContextManager[aiohttp.ClientResponse]:
        headers = {"Authorization": "Bearer " + user.token}
        params = {"op": "GETFILESTATUS"}
        url = make_url(server_url, file_owner, path)
        return client.get(url, headers=headers, params=params)

    @classmethod
    async def init_test_stat(
        self, server_url: str, client: aiohttp.ClientSession, alice: _User
    ) -> int:
        expected_mtime_min = int(current_time())
        # Alice creates a file in her home 'file1.txt'
        await self.put_file(server_url, client, alice, self.file1)
        # and 'file3.txt' in directory 'dir3'
        await self.put_file(server_url, client, alice, self.dir3_file3)
        # and 'dir4' in directory 'dir3'
        await self.put_dir(server_url, client, alice, self.dir3_dir4)
        return expected_mtime_min

    async def test_filestatus_alice_checks_her_own_files(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
    ) -> None:
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice checks statuses of 'file1.txt', 'dir3' and 'dir3/file3.txt'

        async with self.get_filestatus(
            alice, self.file1, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": self.len_payload,
                "modificationTime": mock.ANY,
                "permission": "manage",
            }
            assert payload["path"].endswith(self.file1)  # relative path
            assert payload["modificationTime"] >= mtime_min

        # check that directory was created
        async with self.get_filestatus(
            alice, self.dir3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": "manage",
            }
            assert payload["path"].endswith(self.dir3)  # relative path
            assert payload["modificationTime"] >= mtime_min

        async with self.get_filestatus(
            alice, self.dir3_file3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": self.len_payload,
                "modificationTime": mock.ANY,
                "permission": "manage",
            }
            assert payload["path"].endswith(self.dir3_file3)  # relative path
            assert payload["modificationTime"] >= mtime_min

    async def test_filestatus_check_non_existing_file(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
    ) -> None:
        # Alice creates a file in her home
        await self.put_file(server_url, client, alice, self.file1)

        # Alice gets status of non-existing 'file2.txt' -- NOT FOUND
        async with self.get_filestatus(
            alice, self.file2, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    async def test_filestatus_bob_checks_alices_files(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
    ) -> None:
        await self.init_test_stat(server_url, client, alice)

        # Bob checks status of Alice's 'file1.txt' -- NOT FOUND
        async with self.get_filestatus(
            bob, self.file1, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        # Bob checks status of Alice's 'file2.txt' -- NOT FOUND
        async with self.get_filestatus(
            bob, self.file2, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        # Bob checks status of Alice's 'dir3' -- NOT FOUND
        async with self.get_filestatus(
            bob, self.dir3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        # Bob checks status of Alice's 'dir3/file3.txt' -- NOT FOUND
        async with self.get_filestatus(
            bob, self.dir3_file3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_filestatus_share_file_then_check_it(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
        permission: str,
    ) -> None:
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'file1.txt' with permission P
        await granter(
            bob.name,
            [
                {
                    "uri": f"storage://{cluster_name}/{alice.name}/{self.file1}",
                    "action": permission,
                }
            ],
            alice,
        )

        # then Bob checks status of 'file1.txt' (permission=P)
        async with self.get_filestatus(
            bob, self.file1, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": self.len_payload,
                "modificationTime": mock.ANY,
                "permission": permission,
            }
            assert payload["path"].endswith(self.file1)  # relative path
            assert payload["modificationTime"] >= mtime_min

    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_filestatus_share_dir_then_check_it(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
        permission: str,
    ) -> None:
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3' with permission P
        await granter(
            bob.name,
            [
                {
                    "uri": f"storage://{cluster_name}/{alice.name}/{self.dir3}",
                    "action": permission,
                }
            ],
            alice,
        )

        # then Bob checks status of 'dir3' (permission=P)
        async with self.get_filestatus(
            bob, self.dir3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": permission,
            }
            assert payload["path"].endswith(self.dir3)  # relative path

        # then Bob checks status dir3/file3.txt (OK)
        async with self.get_filestatus(
            bob, self.dir3_file3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.FILE),
                "length": self.len_payload,
                "modificationTime": mock.ANY,
                "permission": permission,
            }
            assert payload["path"].endswith(self.dir3_file3)  # relative path
            assert payload["modificationTime"] >= mtime_min

    @pytest.mark.parametrize(
        "perm_file,perm_parent_dir",
        [("read", "read"), ("write", "read"), ("manage", "read")],
    )
    async def test_filestatus_share_file_then_check_parent_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
        perm_file: str,
        perm_parent_dir: str,
    ) -> None:
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3/file3.txt' with permission P

        await granter(
            bob.name,
            [
                {
                    "uri": f"storage://{cluster_name}/{alice.name}/{self.dir3_file3}",
                    "action": perm_file,
                }
            ],
            alice,
        )

        # then Bob checks status of 'dir3' (permission=P)
        async with self.get_filestatus(
            bob, self.dir3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": perm_parent_dir,
            }
            assert payload["path"].endswith(self.dir3)  # relative path
            assert payload["modificationTime"] >= mtime_min

    @pytest.mark.parametrize(
        "perm_dir,perm_parent_dir",
        [("read", "read"), ("write", "read"), ("manage", "read")],
    )
    async def test_filestatus_share_dir_then_check_parent_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
        perm_dir: str,
        perm_parent_dir: str,
    ) -> None:
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3/dir4' with permission P
        await granter(
            bob.name,
            [
                {
                    "uri": f"storage://{cluster_name}/{alice.name}/{self.dir3_dir4}",
                    "action": perm_dir,
                }
            ],
            alice,
        )

        # then Bob checks status 'dir3/dir4' (permission=P)
        async with self.get_filestatus(
            bob, self.dir3_dir4, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": perm_dir,
            }
            assert payload["path"].endswith(self.dir3_dir4)  # relative path
            assert payload["modificationTime"] >= mtime_min

        # then Bob checks status 'dir3' (permission=list)
        async with self.get_filestatus(
            bob, self.dir3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": perm_parent_dir,
            }
            assert payload["path"].endswith(self.dir3)  # relative path
            assert payload["modificationTime"] >= mtime_min

    @pytest.mark.parametrize(
        "perm_dir,perm_child_dir",
        [("read", "read"), ("write", "write"), ("manage", "manage")],
    )
    async def test_filestatus_share_dir_then_check_child_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
        perm_dir: str,
        perm_child_dir: str,
    ) -> None:
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3' with permission P
        await granter(
            bob.name,
            [
                {
                    "uri": f"storage://{cluster_name}/{alice.name}/{self.dir3}",
                    "action": perm_dir,
                }
            ],
            alice,
        )

        # then Bob checks status 'dir3' (permission=P)
        async with self.get_filestatus(
            bob, self.dir3, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": perm_dir,
            }
            assert payload["path"].endswith(self.dir3)  # relative path
            assert payload["modificationTime"] >= mtime_min

        # then Bob checks status 'dir3/dir4' (permission=P)
        async with self.get_filestatus(
            bob, self.dir3_dir4, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            payload = get_filestatus_dict(await response.json())
            assert payload == {
                "path": mock.ANY,
                "type": str(FileStatusType.DIRECTORY),
                "length": 0,
                "modificationTime": mock.ANY,
                "permission": perm_child_dir,
            }
            assert payload["path"].endswith(self.dir3_dir4)  # relative path
            assert payload["modificationTime"] >= mtime_min


class TestRename:
    payload1 = b"test"
    payload2 = b"mississippi"
    file1 = "file1"
    file2 = "file2"

    @classmethod
    async def put_file(
        cls,
        server_url: str,
        client: aiohttp.ClientSession,
        user: _User,
        path: str,
        payload: bytes,
    ) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = make_url(server_url, user, path)
        async with client.put(url, headers=headers, data=payload) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    async def put_dir(
        cls, server_url: str, client: aiohttp.ClientSession, user: _User, path: str
    ) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = make_url(server_url, user, path)
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    async def get_filestatus(
        cls,
        server_url: str,
        client: aiohttp.ClientSession,
        user: _User,
        owner: _User,
        path: str,
    ) -> aiohttp.ClientResponse:
        headers = {"Authorization": "Bearer " + user.token}
        params = {"op": "GETFILESTATUS"}
        url = make_url(server_url, owner, path)
        return await client.get(url, headers=headers, params=params)

    @classmethod
    async def assert_filestatus_equal(
        cls, response: aiohttp.ClientResponse, expected: aiohttp.ClientResponse
    ) -> None:
        assert response.status == expected.status
        values_root = await response.json()
        expected_values_root = await expected.json()
        expected_values = get_filestatus_dict(expected_values_root)
        values = get_filestatus_dict(values_root)
        for strict_key in ["type", "length", "modificationTime"]:
            assert values[strict_key] == expected_values[strict_key]

    @classmethod
    async def rename(
        cls,
        server_url: str,
        client: aiohttp.ClientSession,
        user: _User,
        owner1: _User,
        path1: str,
        owner2: _User,
        path2: str,
    ) -> aiohttp.ClientResponse:
        headers = {"Authorization": "Bearer " + user.token}
        params = {
            "op": "RENAME",
            "destination": str(PurePath("/") / owner2.name / path2),
        }
        url = make_url(server_url, owner1, path1)
        return await client.post(url, headers=headers, params=params)

    @classmethod
    async def rename_relative(
        cls,
        server_url: str,
        client: aiohttp.ClientSession,
        user: _User,
        owner1: _User,
        path1: str,
        path2: str,
    ) -> aiohttp.ClientResponse:
        headers = {"Authorization": "Bearer " + user.token}
        params = {"op": "RENAME", "destination": path2}
        url = make_url(server_url, owner1, path1)
        return await client.post(url, headers=headers, params=params)

    @classmethod
    async def assert_no_file(
        cls,
        server_url: str,
        client: aiohttp.ClientSession,
        owner: _User,
        user: _User,
        path: str,
    ) -> None:
        response_status = await cls.get_filestatus(
            server_url, client, user, owner, path
        )
        assert response_status.status == aiohttp.web.HTTPNotFound.status_code

    async def test_rename_file_same_dir(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
    ) -> None:
        await self.put_file(server_url, client, alice, self.file1, self.payload1)
        old_status = await self.get_filestatus(
            server_url, client, alice, alice, self.file1
        )
        rename_status = await self.rename(
            server_url, client, alice, alice, self.file1, alice, self.file2
        )
        assert rename_status.status == aiohttp.web.HTTPNoContent.status_code
        new_status = await self.get_filestatus(
            server_url, client, alice, alice, self.file2
        )
        await self.assert_filestatus_equal(new_status, old_status)
        await self.assert_no_file(server_url, client, alice, alice, self.file1)

    async def test_rename_file_same_dir_relative(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
    ) -> None:
        await self.put_file(server_url, client, alice, self.file1, self.payload1)
        old_status = await self.get_filestatus(
            server_url, client, alice, alice, self.file1
        )
        rename_status = await self.rename_relative(
            server_url, client, alice, alice, self.file1, self.file2
        )
        assert rename_status.status == aiohttp.web.HTTPNoContent.status_code
        new_status = await self.get_filestatus(
            server_url, client, alice, alice, self.file2
        )
        await self.assert_filestatus_equal(new_status, old_status)
        await self.assert_no_file(server_url, client, alice, alice, self.file1)

    async def test_rename_file_to_existing_file(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
    ) -> None:
        await self.put_file(server_url, client, alice, self.file1, self.payload1)
        await self.put_file(server_url, client, alice, self.file2, self.payload2)
        status = await self.get_filestatus(server_url, client, alice, alice, self.file1)
        await self.rename(
            server_url, client, alice, alice, self.file1, alice, self.file2
        )
        new_status = await self.get_filestatus(
            server_url, client, alice, alice, self.file2
        )
        await self.assert_filestatus_equal(status, new_status)
        await self.assert_no_file(server_url, client, alice, alice, self.file1)

    async def test_alice_rename_file_to_bobs_folder(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
    ) -> None:
        await self.put_file(server_url, client, alice, self.file1, self.payload1)
        status = await self.get_filestatus(server_url, client, alice, alice, self.file1)
        response = await self.rename(
            server_url, client, alice, alice, self.file1, bob, self.file2
        )
        assert response.status == aiohttp.web.HTTPNotFound.status_code
        new_status = await self.get_filestatus(
            server_url, client, alice, alice, self.file1
        )
        await self.assert_filestatus_equal(status, new_status)

    async def test_alice_rename_file_to_bobs_relative(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
    ) -> None:
        await self.put_file(server_url, client, alice, self.file1, self.payload1)
        status = await self.get_filestatus(server_url, client, alice, alice, self.file1)
        response = await self.rename_relative(
            server_url, client, alice, alice, self.file1, f"../{bob.name}/self.file2"
        )
        assert response.status == aiohttp.web.HTTPNotFound.status_code
        new_status = await self.get_filestatus(
            server_url, client, alice, alice, self.file1
        )
        await self.assert_filestatus_equal(status, new_status)

    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_alice_rename_file_to_bobs_folder_shared(
        self,
        server_url: str,
        api: ApiConfig,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
        permission: str,
    ) -> None:
        await self.put_file(server_url, client, bob, self.file1, self.payload2)
        await granter(
            alice.name,
            [{"uri": f"storage://{cluster_name}/{bob.name}", "action": permission}],
            bob,
        )
        await self.put_file(server_url, client, alice, self.file1, self.payload1)
        status = await self.get_filestatus(server_url, client, alice, alice, self.file1)
        response = await self.rename(
            server_url, client, alice, alice, self.file1, bob, self.file2
        )
        if permission == "read":
            assert response.status == aiohttp.web.HTTPNotFound.status_code
            new_status = await self.get_filestatus(
                server_url, client, alice, alice, self.file1
            )
            await self.assert_filestatus_equal(status, new_status)
            await self.assert_no_file(server_url, client, alice, bob, self.file2)
        else:
            assert response.status == aiohttp.web.HTTPNoContent.status_code
            new_status = await self.get_filestatus(
                server_url, client, alice, bob, self.file2
            )
            await self.assert_filestatus_equal(status, new_status)
            await self.assert_no_file(server_url, client, alice, alice, self.file1)

    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_alice_rename_bobs_files_shared(
        self,
        server_url: str,
        api: ApiConfig,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
        client: aiohttp.ClientSession,
        alice: _User,
        bob: _User,
        permission: str,
    ) -> None:
        await self.put_file(server_url, client, bob, self.file2, self.payload2)
        await granter(
            alice.name,
            [{"uri": f"storage://{cluster_name}/{bob.name}", "action": permission}],
            bob,
        )
        await self.put_file(server_url, client, alice, self.file1, self.payload1)
        status = await self.get_filestatus(server_url, client, alice, bob, self.file2)
        response = await self.rename(
            server_url, client, alice, bob, self.file2, alice, self.file1
        )
        if permission == "read":
            assert response.status == aiohttp.web.HTTPNotFound.status_code
            new_status = await self.get_filestatus(
                server_url, client, alice, bob, self.file2
            )
            await self.assert_filestatus_equal(status, new_status)
            await self.assert_no_file(server_url, client, alice, alice, self.file2)
        else:
            assert response.status == aiohttp.web.HTTPNoContent.status_code
            new_status = await self.get_filestatus(
                server_url, client, alice, alice, self.file1
            )
            await self.assert_filestatus_equal(status, new_status)
            await self.assert_no_file(server_url, client, alice, bob, self.file2)


class TestMultiStorage:
    async def test_put_main_storage(
        self,
        multi_storage_config: Config,
        multi_storage_server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{multi_storage_server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 201

        assert Path(
            multi_storage_config.storage.fs_local_base_path,
            "main",
            dir_path,
            file_name,
        ).exists()

    async def test_put_extra_storage(
        self,
        multi_storage_config: Config,
        multi_storage_server_url: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{multi_storage_server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        Path(
            multi_storage_config.storage.fs_local_base_path,
            "extra",
            user.name,
        ).mkdir(parents=True)

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 201

        assert Path(
            multi_storage_config.storage.fs_local_base_path,
            "extra",
            dir_path,
            file_name,
        ).exists()


class TestMaintenanceCheckerIntegration:
    async def test_put_main_storage_not_ready(
        self,
        on_maintenance_cluster_api: ApiConfig,
        on_maintenance_cluster_name: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user = await regular_user_factory(
            override_cluster_name=on_maintenance_cluster_name
        )
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{on_maintenance_cluster_api.storage_base_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 503
            data = await response.json()
            assert "maintenance" in data["error"]

    async def test_put_org_storage_not_ready(
        self,
        on_maintenance_org_cluster_api: ApiConfig,
        on_maintenance_org_cluster_name: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        raise ValueError("foo")
        user = await regular_user_factory(
            override_cluster_name=on_maintenance_org_cluster_name
        )
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"org/{user.name}/path/to"
        dir_url = f"{on_maintenance_org_cluster_api.storage_base_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 503, await response.text()
            data = await response.json()
            assert "maintenance" in data["error"]

    async def test_put_main_ok_if_org_storage_not_ready(
        self,
        on_maintenance_org_cluster_api: ApiConfig,
        on_maintenance_org_cluster_name: str,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        raise ValueError("foo")
        user = await regular_user_factory(
            override_cluster_name=on_maintenance_org_cluster_name
        )
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{on_maintenance_org_cluster_api.storage_base_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        async with client.put(url, headers=headers, data=b"test") as response:
            assert response.status == 201
