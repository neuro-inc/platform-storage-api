import time
import uuid
from io import BytesIO
from pathlib import PurePath
from time import time as current_time
from unittest import mock

import aiohttp
import aiohttp.web
import pytest

from platform_storage_api.fs.local import FileStatusType
from tests.integration.conftest import get_filestatus_dict, get_liststatus_dict


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api, client):
        async with client.head(api.ping_url) as response:
            assert response.status == 200

        async with client.get(api.ping_url) as response:
            assert response.status == 200


class TestStorage:
    @pytest.mark.asyncio
    async def test_put_head_get(self, server_url, client, regular_user_factory, api):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        payload = b"test"
        mtime_min = int(current_time())

        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        async with client.head(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == len(payload)
            last_modified = response.headers["Last-Modified"]
            mtime = time.mktime(
                time.strptime(last_modified, "%a, %d %b %Y %H:%M:%S %Z")
            )
            assert mtime >= mtime_min
            assert mtime <= int(current_time())
            assert response.headers["X-File-Type"] == "FILE"
            assert response.headers["X-File-Permission"] == "read"
            assert response.headers["X-File-Length"] == str(len(payload))

        async with client.get(url, headers=headers) as response:
            assert response.status == 200
            assert response.content_length == len(payload)
            assert response.headers["Last-Modified"] == last_modified
            assert response.headers["X-File-Type"] == "FILE"
            assert response.headers["X-File-Permission"] == "read"
            assert response.headers["X-File-Length"] == str(len(payload))
            result_payload = await response.read()
            assert result_payload == payload

    @pytest.mark.asyncio
    async def test_head_non_existent(
        self, server_url, client, regular_user_factory, api
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/non-existent"

        async with client.head(url, headers=headers) as response:
            assert response.status == 404

    @pytest.mark.asyncio
    async def test_get_non_existent(
        self, server_url, client, regular_user_factory, api
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/non-existent"

        async with client.get(url, headers=headers) as response:
            assert response.status == 404

    @pytest.mark.asyncio
    async def test_put_illegal_op(self, server_url, api, client, regular_user_factory):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        params = {"op": "OPEN"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = "Illegal operation: OPEN"
            assert payload["error"] == expected_error

    @pytest.mark.asyncio
    async def test_get_illegal_op(self, server_url, api, client, regular_user_factory):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        params = {"op": "CREATE"}
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = "Illegal operation: CREATE"
            assert payload["error"] == expected_error

    @pytest.mark.asyncio
    async def test_liststatus(self, server_url, api, client, regular_user_factory):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"

        payload = b"test"
        mtime_min = int(current_time())
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
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

    @pytest.mark.asyncio
    async def test_liststatus_no_op_param_no_equals(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_path = f"{user.name}/path/to"
        dir_url = f"{server_url}/{dir_path}"
        file_name = "file.txt"
        url = f"{dir_url}/{file_name}"
        payload = b"test"

        mtime_min = int(current_time())
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
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

    @pytest.mark.asyncio
    async def test_ambiguous_operations_with_op(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/"
        async with client.get(
            dir_url + "?op=liststatus&open", headers=headers
        ) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert "Ambiguous operations" in payload["error"]

    @pytest.mark.asyncio
    async def test_ambiguous_operations(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/"
        async with client.get(
            dir_url + "?liststatus&open", headers=headers
        ) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert "Ambiguous operations" in payload["error"]

    @pytest.mark.asyncio
    async def test_unknown_operation(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/"
        async with client.get(dir_url + "?op=unknown", headers=headers) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = "'UNKNOWN' is not a valid StorageOperation"
            assert payload["error"] == expected_error

    @pytest.mark.asyncio
    async def test_liststatus_non_existent_dir(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        dir_url = f"{server_url}/{user.name}/non-existent"

        params = {"op": "LISTSTATUS"}
        async with client.get(dir_url, headers=headers, params=params) as response:
            assert response.status == 404

    @pytest.mark.asyncio
    async def test_liststatus_file(self, server_url, api, client, regular_user_factory):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        payload = b"test"

        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {"op": "LISTSTATUS"}
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Not a directory"

    @pytest.mark.asyncio
    async def test_mkdirs(self, server_url, api, client, regular_user_factory):
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

    @pytest.mark.asyncio
    async def test_mkdirs_existent_dir(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        dir_url = f"{server_url}{path_str}"

        params = {"op": "MKDIRS"}
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @pytest.mark.asyncio
    async def test_mkdirs_existent_file(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        payload = b"test"
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "File exists"

    @pytest.mark.asyncio
    async def test_mkdirs_existent_parent_file(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        dir_url = f"{server_url}{path_str}/dir"
        payload = b"test"
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {"op": "MKDIRS"}
        async with client.put(dir_url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Predescessor is not a directory"

    @pytest.mark.asyncio
    async def test_put_target_is_directory(
        self, server_url, client, regular_user_factory, api
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        url = f"{server_url}/{user.name}/path/to/file"
        payload = b"test"
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload["error"] == "Destination is a directory"

    @pytest.mark.asyncio
    async def test_head_target_is_directory(
        self, server_url, client, regular_user_factory, api
    ):
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

    @pytest.mark.asyncio
    async def test_get_target_is_directory(
        self, server_url, client, regular_user_factory, api
    ):
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
            payload = await response.read()
            assert payload == b""

    @pytest.mark.asyncio
    async def test_delete_non_existent(
        self, server_url, api, client, regular_user_factory
    ):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        async with client.delete(url, headers=headers) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    @pytest.mark.asyncio
    async def test_delete_file(self, server_url, api, client, regular_user_factory):
        user = await regular_user_factory()
        headers = {"Authorization": "Bearer " + user.token}
        path_str = f"/{user.name}/new/nested/{uuid.uuid4()}"
        url = f"{server_url}{path_str}"
        payload = b"test"

        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.delete(url, headers=headers) as response:
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
    def url(cls, server_url, user, path):
        return f"{server_url}/{user.name}/{path}"

    @pytest.fixture()
    async def alice(self, regular_user_factory):
        return await regular_user_factory()

    @pytest.fixture()
    async def bob(self, regular_user_factory):
        return await regular_user_factory()

    @classmethod
    async def put_file(cls, server_url, client, user, path) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = cls.url(server_url, user, path)
        async with client.put(url, headers=headers, data=BytesIO(b"test")) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    async def put_dir(cls, server_url, client, user, path) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = cls.url(server_url, user, path)
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    def get_filestatus(
        cls, user, path, server_url, client, file_owner
    ) -> aiohttp.web.Response:
        headers = {"Authorization": "Bearer " + user.token}
        params = {"op": "GETFILESTATUS"}
        url = cls.url(server_url, file_owner, path)
        return client.get(url, headers=headers, params=params)

    @classmethod
    async def init_test_stat(self, server_url, client, alice):
        expected_mtime_min = int(current_time())
        # Alice creates a file in her home 'file1.txt'
        await self.put_file(server_url, client, alice, self.file1)
        # and 'file3.txt' in directory 'dir3'
        await self.put_file(server_url, client, alice, self.dir3_file3)
        # and 'dir4' in directory 'dir3'
        await self.put_dir(server_url, client, alice, self.dir3_dir4)
        return expected_mtime_min

    @pytest.mark.asyncio
    async def test_filestatus_alice_checks_her_own_files(
        self, server_url, api, client, alice, bob
    ):
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

    @pytest.mark.asyncio
    async def test_filestatus_check_non_existing_file(
        self, server_url, api, client, alice, bob
    ):
        # Alice creates a file in her home
        await self.put_file(server_url, client, alice, self.file1)

        # Alice gets status of non-existing 'file2.txt' -- NOT FOUND
        async with self.get_filestatus(
            alice, self.file2, server_url, client, file_owner=alice
        ) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    @pytest.mark.asyncio
    async def test_filestatus_bob_checks_alices_files(
        self, server_url, api, client, alice, bob
    ):
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_filestatus_share_file_then_check_it(
        self, server_url, api, client, alice, bob, granter, permission
    ):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'file1.txt' with permission P
        await granter(
            bob.name,
            [{"uri": f"storage://{alice.name}/{self.file1}", "action": permission}],
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_filestatus_share_dir_then_check_it(
        self, server_url, api, client, alice, bob, granter, permission
    ):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3' with permission P
        await granter(
            bob.name,
            [{"uri": f"storage://{alice.name}/{self.dir3}", "action": permission}],
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "perm_file,perm_parent_dir",
        [("read", "read"), ("write", "read"), ("manage", "read")],
    )
    async def test_filestatus_share_file_then_check_parent_dir(
        self, server_url, api, client, alice, bob, granter, perm_file, perm_parent_dir
    ):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3/file3.txt' with permission P

        await granter(
            bob.name,
            [{"uri": f"storage://{alice.name}/{self.dir3_file3}", "action": perm_file}],
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "perm_dir,perm_parent_dir",
        [("read", "read"), ("write", "read"), ("manage", "read")],
    )
    async def test_filestatus_share_dir_then_check_parent_dir(
        self, server_url, api, client, alice, bob, granter, perm_dir, perm_parent_dir
    ):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3/dir4' with permission P
        await granter(
            bob.name,
            [{"uri": f"storage://{alice.name}/{self.dir3_dir4}", "action": perm_dir}],
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "perm_dir,perm_child_dir",
        [("read", "read"), ("write", "write"), ("manage", "manage")],
    )
    async def test_filestatus_share_dir_then_check_child_dir(
        self, server_url, api, client, alice, bob, granter, perm_dir, perm_child_dir
    ):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob file 'dir3' with permission P
        await granter(
            bob.name,
            [{"uri": f"storage://{alice.name}/{self.dir3}", "action": perm_dir}],
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
    def url(cls, server_url, user, path):
        return f"{server_url}/{user.name}/{path}"

    @pytest.fixture()
    async def alice(self, regular_user_factory):
        return await regular_user_factory()

    @pytest.fixture()
    async def bob(self, regular_user_factory):
        return await regular_user_factory()

    @classmethod
    async def put_file(cls, server_url, client, user, path, payload) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = cls.url(server_url, user, path)
        async with client.put(url, headers=headers, data=BytesIO(payload)) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    async def put_dir(cls, server_url, client, user, path) -> None:
        headers = {"Authorization": "Bearer " + user.token}
        url = cls.url(server_url, user, path)
        params = {"op": "MKDIRS"}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    async def get_filestatus(
        cls, server_url, client, user, owner, path
    ) -> aiohttp.web.Response:
        headers = {"Authorization": "Bearer " + user.token}
        params = {"op": "GETFILESTATUS"}
        url = cls.url(server_url, owner, path)
        return await client.get(url, headers=headers, params=params)

    @classmethod
    async def assert_filestatus_equal(
        cls, response: aiohttp.web.Response, expected: aiohttp.web.Response
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
        cls, server_url, client, user, owner1, path1, owner2, path2
    ) -> aiohttp.web.Response:
        headers = {"Authorization": "Bearer " + user.token}
        params = {
            "op": "RENAME",
            "destination": str(PurePath("/") / owner2.name / path2),
        }
        url = cls.url(server_url, owner1, path1)
        return await client.post(url, headers=headers, params=params)

    @classmethod
    async def rename_relative(
        cls, server_url, client, user, owner1, path1, path2
    ) -> aiohttp.web.Response:
        headers = {"Authorization": "Bearer " + user.token}
        params = {"op": "RENAME", "destination": path2}
        url = cls.url(server_url, owner1, path1)
        return await client.post(url, headers=headers, params=params)

    @classmethod
    async def assert_no_file(cls, server_url, client, owner, user, path):
        response_status = await cls.get_filestatus(
            server_url, client, user, owner, path
        )
        assert response_status.status == aiohttp.web.HTTPNotFound.status_code

    @pytest.mark.asyncio
    async def test_rename_file_same_dir(self, server_url, api, client, alice):
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

    @pytest.mark.asyncio
    async def test_rename_file_same_dir_relative(self, server_url, api, client, alice):
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

    @pytest.mark.asyncio
    async def test_rename_file_to_existing_file(self, server_url, api, client, alice):
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

    @pytest.mark.asyncio
    async def test_alice_rename_file_to_bobs_folder(
        self, server_url, api, client, alice, bob
    ):
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

    @pytest.mark.asyncio
    async def test_alice_rename_file_to_bobs_relative(
        self, server_url, api, client, alice, bob
    ):
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_alice_rename_file_to_bobs_folder_shared(
        self, server_url, api, granter, client, alice, bob, permission
    ):
        await self.put_file(server_url, client, bob, self.file1, self.payload2)
        await granter(
            alice.name, [{"uri": f"storage://{bob.name}", "action": permission}], bob
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize("permission", ["read", "write", "manage"])
    async def test_alice_rename_bobs_files_shared(
        self, server_url, api, granter, client, alice, bob, permission
    ):
        await self.put_file(server_url, client, bob, self.file2, self.payload2)
        await granter(
            alice.name, [{"uri": f"storage://{bob.name}", "action": permission}], bob
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
