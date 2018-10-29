from io import BytesIO

from time import time as current_time
import pytest

from platform_storage_api.fs.local import FileStatusType
from .conftest import assert_filestatus, assert_filestatus_list
from ..conftest import get_liststatus_dict


class TestStorageListAndResourceSharing:
    def file_status_sort(self, file_status):
        return file_status["path"]

    @pytest.mark.asyncio
    async def test_ls_other_user_data_no_permission(
        self, server_url, api, client, regular_user_factory, granter
    ):
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
    async def test_ls_other_user_data_shared_with_files(
        self, server_url, api, client, regular_user_factory, granter
    ):
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {"Authorization": "Bearer " + user2.token}

        # create file /path/to/file by user1
        dir_url = f'{server_url}/{user1.name}/path/to'
        url = dir_url + '/file'
        payload = b'test'
        min_mtime_first = int(current_time())
        async with client.put(url, headers=headers1, data=BytesIO(payload)) \
                as response:
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
            assert_filestatus_list(statuses, self.file_status_sort, [
                {'path': 'file',
                 'length': len(payload),
                 'type': FileStatusType.FILE,
                 'modification_time_min': min_mtime_first,
                 },
                {'path': 'second',
                 'length': 0,
                 'type': FileStatusType.DIRECTORY,
                 'modification_time_min': min_mtime_first,
                }
            ])

    @pytest.mark.asyncio
    async def test_ls_other_user_data_exclude_files(
        self, server_url, api, client, regular_user_factory, granter
    ):
        user1 = await regular_user_factory()
        headers1 = {"Authorization": "Bearer " + user1.token}

        user2 = await regular_user_factory()
        headers2 = {"Authorization": "Bearer " + user2.token}

        # create file /path/to/file by user1
        dir_url = f'{server_url}/{user1.name}/path/to/'
        url = dir_url + '/file'
        payload = b'test'
        min_mtime_first = int(current_time())
        async with client.put(url, headers=headers1, data=BytesIO(payload)) \
                as response:
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
            statuses = await response.text()

        await granter(
            user2.name,
            [{"uri": f"storage://{user1.name}/path/to/first", "action": "read"}],
            user1,
        )
        async with client.get(dir_url, headers=headers2, params=params) as response:
            assert response.status == 200

            statuses = get_liststatus_dict(await response.json())
            assert_filestatus_list(statuses, self.file_status_sort, [
                {'path': 'first',
                 'length': 0,
                 'type': FileStatusType.DIRECTORY,
                 'modification_time_min': min_mtime_first,
                 }
            ])

    @pytest.mark.asyncio
    async def test_liststatus_other_user_data_two_subdirs(
        self, server_url, api, client, regular_user_factory, granter
    ):
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

        params = {'op': 'MKDIRS'}
        min_mtime_second = int(current_time())
        async with client.put(dir_url + "/first/second", headers=headers1,
                              params=params) \
                as response:
            assert response.status == 201

        min_mtime_third = int(current_time())
        async with client.put(dir_url + "/first/third", headers=headers1,
                              params=params) \
                as response:
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
            assert_filestatus_list(statuses, self.file_status_sort, [
                {'path': 'second',
                 'length': 0,
                 'type': FileStatusType.DIRECTORY,
                 'modification_time_min': min_mtime_second,
                 },
                {'path': 'third',
                 'length': 0,
                 'type': FileStatusType.DIRECTORY,
                 'modification_time_min': min_mtime_third,
                },
            ])
