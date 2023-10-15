import asyncio
from collections.abc import Awaitable, Callable
from io import BytesIO
from time import time as current_time
from typing import Any
from unittest import mock

import aiohttp
import yarl

from platform_storage_api.fs.local import FileStatusType

from .auth import _User, _UserFactory
from tests.integration.conftest import ApiConfig, status_iter_response_to_list


class TestStorageListAndResourceSharing:
    def file_status_sort(self, file_status: dict[str, Any]) -> Any:
        return file_status["path"]

    async def test_ls_other_user_data_no_permission(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        user1 = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user1.token}
        dir_url = f"{server_url}/{user1.name}/path/to"
        url = dir_url + "/file"
        payload = b"test"
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        user2 = await regular_user_factory()
        headers = {
            "Authorization": "Bearer " + user2.token,
            "Accept": "application/x-ndjson",
        }
        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 404

    async def test_ls_other_user_data_no_permission_issue(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
    ) -> None:
        # user1 uploads a file
        user1 = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user1.token}
        dir_url = f"{server_url}/{user1.name}/path/to"
        url = dir_url + "/file"
        payload = b"test"
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        # user2 uploads a file
        user2 = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user2.token}
        dir_url = f"{server_url}/{user2.name}/path/to"
        url = dir_url + "/file"
        payload = b"test"
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        # user2 lists users
        headers = {
            "Authorization": "Bearer " + user2.token,
            "Accept": "application/x-ndjson",
        }
        dir_url = f"{server_url}/{user2.name}/../"
        params = {"op": "LISTSTATUS"}
        async with client.get(
            yarl.URL(dir_url, encoded=True), headers=headers, params=params
        ) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            resp_text = await response.text("utf-8")
            assert user1.name not in resp_text
            assert user2.name in resp_text

    async def test_ls_other_user_data_shared_with_files(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {
            "Authorization": "Bearer " + user2.token,
            "Accept": "application/x-ndjson",
        }

        # create file /path/to/file by user1
        dir_url = f"{server_url}/{user1.name}/path/to"
        url = dir_url + "/file"
        payload = b"test"
        min_mtime_first = int(current_time())
        async with client.put(url, headers=headers1, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        async with client.put(
            dir_url + "/second", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        # list by user2
        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 404

        await granter(
            user2.name,
            [{"uri": f"storage://{cluster_name}/{user1.name}/path/", "action": "read"}],
            user1,
        )
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            statuses = sorted(statuses, key=self.file_status_sort)
            assert statuses == [
                {
                    "path": "file",
                    "length": len(payload),
                    "type": str(FileStatusType.FILE),
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
                {
                    "path": "second",
                    "length": 0,
                    "type": str(FileStatusType.DIRECTORY),
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
            ]
            for status in statuses:
                assert status["modificationTime"] >= min_mtime_first

    async def test_ls_other_user_data_exclude_files(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {
            "Authorization": "Bearer " + user2.token,
            "Accept": "application/x-ndjson",
        }

        # create file /path/to/file by user1
        dir_url = f"{server_url}/{user1.name}/path/to/"
        url = dir_url + "/file"
        payload = b"test"
        min_mtime_first = int(current_time())
        async with client.put(url, headers=headers1, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        async with client.put(
            dir_url + "/first/second", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        # list by user2
        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 404

        root_uri = f"storage://{cluster_name}/{user1.name}"
        await granter(
            user2.name, [{"uri": root_uri + "/path/to/first", "action": "read"}], user1
        )
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            statuses = sorted(statuses, key=self.file_status_sort)
            assert statuses == [
                {
                    "path": "first",
                    "length": 0,
                    "type": str(FileStatusType.DIRECTORY),
                    "modificationTime": mock.ANY,
                    "permission": "read",
                }
            ]
            for status in statuses:
                assert status["modificationTime"] >= min_mtime_first

    async def test_liststatus_other_user_data_two_subdirs(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {
            "Authorization": "Bearer " + user2.token,
            "Accept": "application/x-ndjson",
        }

        # create file /path/to/file by user1
        dir_url = f"{server_url}/{user1.name}/path/to"
        url = dir_url + "/file"
        payload = b"test"
        async with client.put(url, headers=headers1, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        min_mtime_second = int(current_time())
        async with client.put(
            dir_url + "/first/second", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        min_mtime_third = int(current_time())
        async with client.put(
            dir_url + "/first/third", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        async with client.put(
            dir_url + "/first/fourth", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        # list by user2
        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 404

        root_uri = f"storage://{cluster_name}/{user1.name}"
        await granter(
            user2.name,
            [{"uri": root_uri + "/path/to/first/second", "action": "read"}],
            user1,
        )
        await granter(
            user2.name,
            [{"uri": root_uri + "/path/to/first/third", "action": "read"}],
            user1,
        )
        async with client.get(
            dir_url + "/first", headers=headers2, params=params
        ) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            statuses = sorted(statuses, key=self.file_status_sort)
            assert statuses == [
                {
                    "path": "second",
                    "length": 0,
                    "type": str(FileStatusType.DIRECTORY),
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
                {
                    "path": "third",
                    "length": 0,
                    "type": str(FileStatusType.DIRECTORY),
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
            ]
            assert statuses[0]["modificationTime"] >= min_mtime_second
            assert statuses[1]["modificationTime"] >= min_mtime_third

    async def test_liststatus_permissions(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: _UserFactory,
        granter: Callable[[str, Any, _User], Awaitable[None]],
        cluster_name: str,
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {
            "Authorization": "Bearer " + user2.token,
            "Accept": "application/x-ndjson",
        }

        # create file /path/to/file by user1
        dir_url = f"{server_url}/{user1.name}/path/to"
        url = dir_url + "/file"
        payload = b"test"
        async with client.put(url, headers=headers1, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        await asyncio.sleep(1)
        min_mtime_third = int(current_time())
        await asyncio.sleep(1)
        async with client.put(
            dir_url + "/first/second/third", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        await asyncio.sleep(1)
        min_mtime_fourth = int(current_time())
        await asyncio.sleep(1)
        async with client.put(
            dir_url + "/first/second/fourth", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        await asyncio.sleep(1)
        async with client.put(
            dir_url + "/first/fifth", headers=headers1, params=params
        ) as response:
            assert response.status == 201

        # list by user2
        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 404

        root_uri = f"storage://{cluster_name}/{user1.name}"
        await granter(
            user2.name, [{"uri": root_uri + "/path/to/file", "action": "read"}], user1
        )
        await granter(
            user2.name,
            [{"uri": root_uri + "/path/to/first/second", "action": "write"}],
            user1,
        )
        await granter(
            user2.name,
            [{"uri": root_uri + "/path/to/first/second/third", "action": "manage"}],
            user1,
        )

        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            statuses = sorted(statuses, key=self.file_status_sort)
            assert statuses == [
                {
                    "path": "file",
                    "length": 4,
                    "type": "FILE",
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
                {
                    "path": "first",
                    "length": 0,
                    "type": "DIRECTORY",
                    "modificationTime": mock.ANY,
                    "permission": "read",
                },
            ]
            assert statuses[0]["modificationTime"] <= min_mtime_third
            assert statuses[1]["modificationTime"] >= min_mtime_fourth

        async with client.get(
            dir_url + "/first", headers=headers2, params=params
        ) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            statuses = sorted(statuses, key=self.file_status_sort)
            assert statuses == [
                {
                    "path": "second",
                    "length": 0,
                    "type": "DIRECTORY",
                    "modificationTime": mock.ANY,
                    "permission": "write",
                }
            ]
            assert statuses[0]["modificationTime"] >= min_mtime_fourth

        async with client.get(
            dir_url + "/first/second", headers=headers2, params=params
        ) as response:
            assert response.status == 200
            assert response.headers["Content-Type"] == "application/x-ndjson"
            statuses = await status_iter_response_to_list(response.content)
            statuses = sorted(statuses, key=self.file_status_sort)
            assert statuses == [
                {
                    "path": "fourth",
                    "length": 0,
                    "type": "DIRECTORY",
                    "modificationTime": mock.ANY,
                    "permission": "write",
                },
                {
                    "path": "third",
                    "length": 0,
                    "type": "DIRECTORY",
                    "modificationTime": mock.ANY,
                    "permission": "manage",
                },
            ]
            assert statuses[0]["modificationTime"] >= min_mtime_fourth
            assert statuses[1]["modificationTime"] >= min_mtime_third
            assert statuses[1]["modificationTime"] <= min_mtime_fourth
