import uuid
from io import BytesIO
from time import time as current_time

import aiohttp
import aiohttp.web
import pytest

from platform_storage_api.fs.local import FileStatusType


class TestApi:
    @pytest.mark.asyncio
    async def test_ping(self, api, client):
        async with client.get(api.ping_url) as response:
            assert response.status == 200


class TestStorage:
    @pytest.mark.asyncio
    async def test_put_get(self, server_url,
                           client, regular_user_factory, api):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        url = f'{server_url}/{user.name}/path/to/file'
        payload = b'test'

        async with client.put(url, headers=headers, data=BytesIO(payload)) \
                as response:
            assert response.status == 201

        async with client.get(url, headers=headers) as response:
            assert response.status == 200
            result_payload = await response.read()
            assert result_payload == payload

    @pytest.mark.asyncio
    async def test_put_illegal_op(self, server_url, api, client,
                                  regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        url = f'{server_url}/{user.name}/path/to/file'
        params = {'op': 'OPEN'}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = 'Illegal operation: OPEN'
            assert payload['error'] == expected_error

    @pytest.mark.asyncio
    async def test_get_illegal_op(self, server_url, api, client,
                                  regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        url = f'{server_url}/{user.name}/path/to/file'
        params = {'op': 'CREATE'}
        async with client.get(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = 'Illegal operation: CREATE'
            assert payload['error'] == expected_error

    @pytest.mark.asyncio
    async def test_liststatus(self, server_url, api, client,
                              regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        dir_url = f'{server_url}/{user.name}/path/to'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, headers=headers, data=BytesIO(payload)) \
                as response:
            assert response.status == 201

        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers, params=params) \
                as response:
            statuses = await response.json()
            assert statuses == [{
                'path': 'file',
                'size': len(payload),
                'type': 'FILE',
            }]

    @pytest.mark.asyncio
    async def test_liststatus_no_op_param_no_equals(self, server_url, api,
                                                    client,
                                                    regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        dir_url = f'{server_url}/{user.name}/path/to'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, headers=headers, data=BytesIO(payload)) \
                as response:
            assert response.status == 201

        async with client.get(dir_url + '?liststatus', headers=headers) \
                as response:
            statuses = await response.json()
            assert statuses == [{
                'path': 'file',
                'size': len(payload),
                'type': 'FILE',
            }]

    @pytest.mark.asyncio
    async def test_ambiguous_operations_with_op(self, server_url, api,
                                                client, regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        dir_url = f'{server_url}/{user.name}/'
        async with client.get(dir_url + '?op=liststatus&open',
                              headers=headers) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert 'Ambiguous operations' in payload['error']

    @pytest.mark.asyncio
    async def test_ambiguous_operations(self, server_url, api,
                                        client, regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        dir_url = f'{server_url}/{user.name}/'
        async with client.get(dir_url + '?liststatus&open',
                              headers=headers) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert 'Ambiguous operations' in payload['error']

    @pytest.mark.asyncio
    async def test_unknown_operation(self, server_url, api, client,
                                     regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        dir_url = f'{server_url}/{user.name}/'
        async with client.get(dir_url + '?op=unknown',
                              headers=headers) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = '\'UNKNOWN\' is not a valid StorageOperation'
            assert payload['error'] == expected_error

    @pytest.mark.asyncio
    async def test_liststatus_non_existent_dir(self, server_url, api, client,
                                               regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        dir_url = f'{server_url}/{user.name}/non-existent'

        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers,
                              params=params) as response:
            assert response.status == 404

    @pytest.mark.asyncio
    async def test_mkdirs(self, server_url, api, client, regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        path_str = f'/{user.name}/new/nested/{uuid.uuid4()}'
        dir_url = f'{server_url}{path_str}'

        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers,
                              params=params) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        params = {'op': 'MKDIRS'}
        async with client.put(dir_url, headers=headers,
                              params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers,
                              params=params) as response:
            assert response.status == aiohttp.web.HTTPOk.status_code

    @pytest.mark.asyncio
    async def test_mkdirs_existent_dir(self, server_url, api, client,
                                       regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        path_str = f'/{user.name}/new/nested/{uuid.uuid4()}'
        dir_url = f'{server_url}{path_str}'

        params = {'op': 'MKDIRS'}
        async with client.put(dir_url, headers=headers,
                              params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code
        async with client.put(dir_url, headers=headers,
                              params=params) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @pytest.mark.asyncio
    async def test_mkdirs_existent_file(self, server_url, api, client,
                                        regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        path_str = f'/{user.name}/new/nested/{uuid.uuid4()}'
        url = f'{server_url}{path_str}'
        payload = b'test'
        async with client.put(url, headers=headers,
                              data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {'op': 'MKDIRS'}
        async with client.put(url, headers=headers, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert payload['error'] == 'File exists'

    @pytest.mark.asyncio
    async def test_delete_non_existent(self, server_url, api, client,
                                       regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        path_str = f'/{user.name}/new/nested/{uuid.uuid4()}'
        url = f'{server_url}{path_str}'
        async with client.delete(url, headers=headers) as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    @pytest.mark.asyncio
    async def test_delete_file(self, server_url, api, client,
                               regular_user_factory):
        user = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user.token}
        path_str = f'/{user.name}/new/nested/{uuid.uuid4()}'
        url = f'{server_url}{path_str}'
        payload = b'test'

        async with client.put(url, headers=headers,
                              data=BytesIO(payload)) as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

        async with client.delete(url, headers=headers) as response:
            assert response.status == aiohttp.web.HTTPNoContent.status_code


class TestFileStatus:

    payload = b'test'
    len_payload = len(payload)
    file1_path = 'file1.txt'
    file2_path = 'file2.txt'
    dir3_path = 'dir3'
    file3_path = f'{dir3_path}/file3.txt'

    @classmethod
    def url(cls, server_url, user, path):
        return f'{server_url}/{user.name}/{path}'

    @pytest.fixture()
    async def alice(self, regular_user_factory):
        return await regular_user_factory()

    @pytest.fixture()
    async def bob(self, regular_user_factory):
        return await regular_user_factory()

    @classmethod
    async def put_file(cls, server_url, client, user, path) -> None:
        headers = {'Authorization': 'Bearer ' + user.token}
        url = cls.url(server_url, user, path)
        async with client.put(url, headers=headers,
                              data=BytesIO(b'test')) \
                as response:
            assert response.status == aiohttp.web.HTTPCreated.status_code

    @classmethod
    def get_filestatus(cls, user, path, server_url, client, file_owner) \
            -> aiohttp.web.Response:
        headers = {'Authorization': 'Bearer ' + user.token}
        params = {'op': 'FILESTATUS'}
        url = cls.url(server_url, file_owner, path)
        return client.get(url, headers=headers, params=params)

    @classmethod
    async def assert_filestatus(cls,
                                response: aiohttp.web.Response,
                                **expected) -> None:
        values_root = await response.json()
        values = values_root['FileStatus']
        for strict_key in ['type', 'length', 'permission']:
            assert values[strict_key] == expected[strict_key]
        assert values['modificationTime'] >= expected['modificationTime']
        assert len(values) == 4  # no more extra keys

    @classmethod
    async def init_test_stat(self, server_url, client, alice):
        expected_mtime_min = int(current_time())
        # Alice creates a file in her home "file1.txt"
        await self.put_file(server_url, client, alice, self.file1_path)
        # and "file3.txt in directory dir3"
        await self.put_file(server_url, client, alice, self.file3_path)
        return expected_mtime_min

    @pytest.mark.asyncio
    async def test_filestatus_alice_checks_her_own_files(self, server_url,
                                                         api, client,
                                                         alice, bob):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice checks statuses of "file1.txt", "dir3" and "dir3/file3.txt"

        async with self.get_filestatus(alice, self.file1_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            await self.assert_filestatus(response,
                                    type=FileStatusType.FILE,
                                    modificationTime=mtime_min,
                                    length=self.len_payload,
                                    permission='manage')

        # check that directory was created
        async with self.get_filestatus(alice, self.dir3_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            await self.assert_filestatus(response,
                                    type=FileStatusType.DIRECTORY,
                                    modificationTime=mtime_min,
                                    length=0,
                                    permission='manage')

        async with self.get_filestatus(alice, self.file3_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            await self.assert_filestatus(response,
                                    type=FileStatusType.FILE,
                                    modificationTime=mtime_min,
                                    length=self.len_payload,
                                    permission='manage')

    @pytest.mark.asyncio
    async def test_filestatus_alice_checks_non_existing_file(self,
                                                             server_url,
                                                             api, client,
                                                             alice, bob):
        # Alice creates a file in her home
        await self.put_file(server_url, client, alice, self.file1_path)

        # Alice gets status of non-existing "file2.txt" -- NOT FOUND
        async with self.get_filestatus(alice, self.file2_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    @pytest.mark.asyncio
    async def test_filestatus_bob_checks_alices_files(self, server_url,
                                                      api, client,
                                                      alice, bob):
        await self.init_test_stat(server_url, client, alice)

        # Bob checks status of Alice's "file1.txt" -- NOT FOUND
        async with self.get_filestatus(bob, self.file1_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        # Bob checks status of Alice's "file2.txt" -- NOT FOUND
        async with self.get_filestatus(bob, self.file2_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        # Bob checks status of Alice's "dir3" -- NOT FOUND
        async with self.get_filestatus(bob, self.dir3_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

        # Bob checks status of Alice's "dir3/file3.txt" -- NOT FOUND
        async with self.get_filestatus(bob, self.file3_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPNotFound.status_code

    @pytest.mark.asyncio
    @pytest.mark.parametrize('permission', ['read', 'write', 'manage'])
    async def test_filestatus_alice_shares_file(self, server_url, api, client,
                                                alice, bob, granter,
                                                permission):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob in READ mode file1.txt
        await granter(bob.name, [
            {'uri': f'storage://{alice.name}/{self.file1_path}',
             'action': permission}], alice)

        async with self.get_filestatus(bob, self.file1_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            await self.assert_filestatus(response,
                                    type=FileStatusType.FILE,
                                    modificationTime=mtime_min,
                                    length=self.len_payload,
                                    permission=permission)

    @pytest.mark.asyncio
    @pytest.mark.parametrize('permission', ['read', 'write', 'manage'])
    async def test_filestatus_alice_shares_dir(self, server_url, api, client,
                                               alice, bob, granter,
                                               permission):
        mtime_min = await self.init_test_stat(server_url, client, alice)

        # Alice shares with Bob in READ mode dir3
        await granter(bob.name, [
            {'uri': f'storage://{alice.name}/{self.dir3_path}',
             'action': permission}], alice)

        # then Bob checks status dir3 (OK)
        async with self.get_filestatus(bob, self.dir3_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            await self.assert_filestatus(response,
                                    type=FileStatusType.DIRECTORY,
                                    modificationTime=mtime_min,
                                    length=0,
                                    permission=permission)

        # then Bob checks status dir3/file3.txt (OK)
        async with self.get_filestatus(bob, self.file3_path, server_url,
                                       client, file_owner=alice) \
                as response:
            assert response.status == aiohttp.web.HTTPOk.status_code
            await self.assert_filestatus(response,
                                    type=FileStatusType.FILE,
                                    modificationTime=mtime_min,
                                    length=self.len_payload,
                                    permission=permission)


