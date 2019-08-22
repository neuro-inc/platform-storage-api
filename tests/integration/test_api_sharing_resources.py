import asyncio
from io import BytesIO
from time import time as current_time
from typing import Any, Awaitable, Callable, Dict
from unittest import mock

import aiohttp
import pytest
import yarl
from neuro_auth_client import User

from platform_storage_api.fs.local import FileStatusType
from tests.integration.conftest import ApiConfig, get_liststatus_dict


class TestStorageListAndResourceSharing:
    def file_status_sort(self, file_status: Dict[str, Any]) -> Any:
        return file_status["path"]

    @pytest.mark.asyncio
    async def test_ls_other_user_data_no_permission(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], User],
    ) -> None:
        user1 = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user1.token}
        dir_url = f"{server_url}/{user1.name}/path/to"
        url = dir_url + "/file"
        payload = b"test"
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        user2 = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user2.token}
        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 404

    @pytest.mark.asyncio
    async def test_ls_other_user_data_no_permission_issue(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], User],
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
        headers = {"Authorization": "Bearer " + user2.token}
        dir_url = f"{server_url}/{user2.name}/../"
        params = {"op": "LISTSTATUS"}
        async with client.get(
            yarl.URL(dir_url, encoded=True), headers=headers, params=params
        ) as response:
            assert response.status == 200
            resp_text = await response.text()
            assert user1.name not in resp_text
            assert user2.name in resp_text

    @pytest.mark.asyncio
    async def test_ls_other_user_data_shared_with_files(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], User],
        granter: Callable[[str, Any, User], Awaitable[None]],
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {"Authorization": "Bearer " + user2.token}

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
            [{"uri": f"storage://{user1.name}/path/", "action": "read"}],
            user1,
        )
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 200
            statuses = get_liststatus_dict(await response.json())
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

    @pytest.mark.asyncio
    async def test_ls_other_user_data_exclude_files(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], User],
        granter: Callable[[str, Any, User], Awaitable[None]],
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {"Authorization": "Bearer " + user2.token}

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

        await granter(
            user2.name,
            [{"uri": f"storage://{user1.name}/path/to/first", "action": "read"}],
            user1,
        )
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 200

            statuses = get_liststatus_dict(await response.json())
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

    @pytest.mark.asyncio
    async def test_liststatus_other_user_data_two_subdirs(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], User],
        granter: Callable[[str, Any, User], Awaitable[None]],
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {"Authorization": "Bearer " + user2.token}

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

        await granter(
            user2.name,
            [{"uri": f"storage://{user1.name}/path/to/first/second", "action": "read"}],
            user1,
        )
        await granter(
            user2.name,
            [{"uri": f"storage://{user1.name}/path/to/first/third", "action": "read"}],
            user1,
        )
        async with client.get(
            dir_url + "/first", headers=headers2, params=params
        ) as response:
            assert response.status == 200
            statuses = get_liststatus_dict(await response.json())
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

    @pytest.mark.asyncio
    async def test_liststatus_permissions(
        self,
        server_url: str,
        api: ApiConfig,
        client: aiohttp.ClientSession,
        regular_user_factory: Callable[[], User],
        granter: Callable[[str, Any, User], Awaitable[None]],
    ) -> None:
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {"Authorization": "Bearer " + user2.token}

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

        await granter(
            user2.name,
            [{"uri": f"storage://{user1.name}/path/to/file", "action": "read"}],
            user1,
        )
        await granter(
            user2.name,
            [
                {
                    "uri": f"storage://{user1.name}/path/to/first/second",
                    "action": "write",
                }
            ],
            user1,
        )
        await granter(
            user2.name,
            [
                {
                    "uri": f"storage://{user1.name}/path/to/first/second/third",
                    "action": "manage",
                }
            ],
            user1,
        )

        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 200
            statuses = get_liststatus_dict(await response.json())
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
            statuses = get_liststatus_dict(await response.json())
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
            statuses = get_liststatus_dict(await response.json())
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
