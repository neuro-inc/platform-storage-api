import os
import uuid
from io import BytesIO
from pathlib import PurePath
from typing import NamedTuple, Optional

import aiohttp
import pytest
from attr import dataclass
from jose import jwt
from neuro_auth_client import AuthClient, User
from yarl import URL

from platform_storage_api.api import create_app
from platform_storage_api.config import (AuthConfig, Config,
                                         EnvironConfigFactory, ServerConfig,
                                         StorageConfig)
from platform_storage_api.storage import Storage


class ApiConfig(NamedTuple):
    host: str
    port: int

    @property
    def endpoint(self):
        return f'http://{self.host}:{self.port}/api/v1'

    @property
    def storage_base_url(self):
        return self.endpoint + '/storage'

    @property
    def ping_url(self):
        return self.endpoint + '/ping'


@pytest.fixture(scope='session')
def in_docker():
    return os.path.isfile('/.dockerenv')


@pytest.fixture
def token_factory():
    def _factory(name: str):
        payload = {'identity': name}
        return jwt.encode(payload, 'secret', algorithm='HS256')

    return _factory


@pytest.fixture
def admin_token(token_factory):
    return token_factory('admin')


@dataclass
class _User:
    name: str
    token: str


@pytest.fixture
def server_url(in_docker, api):
    if in_docker:
        return 'http://storage:5000/api/v1/storage'
    else:
        return api.storage_base_url


@pytest.fixture
def config(in_docker, admin_token):
    if in_docker:
        return EnvironConfigFactory().create()

    server_config = ServerConfig()
    path = PurePath('/tmp/np_storage')
    storage_config = StorageConfig(fs_local_base_path=path)
    auth = AuthConfig(
        server_endpoint_url=URL('http://localhost:5003'),
        service_token=admin_token
    )
    return Config(server=server_config,
                  storage=storage_config,
                  auth=auth)


@pytest.fixture
async def auth_client(config, admin_token):
    async with AuthClient(
            url=config.auth.server_endpoint_url, token=admin_token
    ) as client:
        yield client


@pytest.fixture
async def regular_user_factory(auth_client, token_factory):
    async def _factory(name: Optional[str] = None) -> User:
        if not name:
            name = str(uuid.uuid4())
        user = User(name=name)
        await auth_client.add_user(user)
        return _User(  # type: ignore
            name=user.name, token=token_factory(user.name)
        )

    return _factory


@pytest.fixture
async def storage(local_fs, config):
    return Storage(fs=local_fs, base_path=config.storage.fs_local_base_path)


@pytest.fixture
async def api(config, storage, in_docker):
    app = await create_app(config, storage)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host='0.0.0.0', port=8080)
    site = aiohttp.web.TCPSite(runner, api_config.host, api_config.port)
    await site.start()
    yield api_config
    await runner.cleanup()


@pytest.fixture
async def client():
    async with aiohttp.ClientSession() as session:
        yield session


@pytest.fixture
async def granter(auth_client, admin_token):
    async def f(whom, what, sourcer):
        headers = auth_client._generate_headers(sourcer.token)
        async with auth_client._request(
                'POST',
                f'/api/v1/users/{whom}/permissions',
                headers=headers,
                json=what,
        ) as p:
            assert p.status == 201

    return f


class TestStorageListAndResourceSharing:

    def file_status_sort(self, file_status):
        return file_status['path']

    @pytest.mark.asyncio
    async def test_ls_other_user_data_no_permission(self,
                                                    server_url,
                                                    api, client,
                                                    regular_user_factory,
                                                    granter):
        user1 = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user1.token}
        dir_url = f'{server_url}/{user1.name}/path/to'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, headers=headers, data=BytesIO(payload)) \
                as response:
            assert response.status == 201

        user2 = await regular_user_factory()
        headers = {'Authorization': 'Bearer ' + user2.token}
        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers, params=params) \
                as response:
            assert response.status == 404

    @pytest.mark.asyncio
    async def test_ls_other_user_data_shared_with_files(self,
                                                        server_url,
                                                        api, client,
                                                        regular_user_factory,
                                                        granter):
        user1 = await regular_user_factory()
        headers1 = {'Authorization': 'Bearer ' + user1.token}

        user2 = await regular_user_factory()
        headers2 = {'Authorization': 'Bearer ' + user2.token}

        # create file /path/to/file by user1
        dir_url = f'{server_url}/{user1.name}/path/to'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, headers=headers1, data=BytesIO(payload)) \
                as response:
            assert response.status == 201

        params = {'op': 'MKDIRS'}
        async with client.put(dir_url + "/second",
                              headers=headers1,
                              params=params) \
                as response:
            assert response.status == 201

        # list by user2
        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers2, params=params) \
                as response:
            assert response.status == 404

        await granter(user2.name,
                      [{'uri': f'storage://{user1.name}/path/',
                        'action': 'read'}],
                      user1)
        async with client.get(dir_url, headers=headers2, params=params) \
                as response:
            assert response.status == 200
            statuses = await response.json()
            assert statuses == [{
                'path': 'file',
                'size': len(payload),
                'type': 'FILE',
            }, {
                'path': 'second',
                'size': 0,
                'type': 'DIRECTORY',
            }]

    @pytest.mark.asyncio
    async def test_ls_other_user_data_exclude_files(self,
                                                    server_url,
                                                    api, client,
                                                    regular_user_factory,
                                                    granter):
        user1 = await regular_user_factory()
        headers1 = {'Authorization': 'Bearer ' + user1.token}

        user2 = await regular_user_factory()
        headers2 = {'Authorization': 'Bearer ' + user2.token}

        # create file /path/to/file by user1
        dir_url = f'{server_url}/{user1.name}/path/to/'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, headers=headers1, data=BytesIO(payload)) \
                as response:
            assert response.status == 201

        params = {'op': 'MKDIRS'}
        async with client.put(dir_url + "/first/second", headers=headers1,
                              params=params) \
                as response:
            assert response.status == 201

        # list by user2
        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers2, params=params) \
                as response:
            assert response.status == 404
            statuses = await response.text()

        await granter(user2.name, [
            {'uri': f'storage://{user1.name}/path/to/first',
             'action': 'read'}], user1)
        async with client.get(dir_url, headers=headers2, params=params) \
                as response:
            assert response.status == 200
            statuses = await response.json()
            assert statuses == [{
                'path': 'first',
                'size': 0,
                'type': 'DIRECTORY',
            }]

    @pytest.mark.asyncio
    async def test_liststatus_other_user_data_two_subdirs(self,
                                                          server_url,
                                                          api, client,
                                                          regular_user_factory,
                                                          granter):
        user1 = await regular_user_factory()
        headers1 = {'Authorization': 'Bearer ' + user1.token}

        user2 = await regular_user_factory()
        headers2 = {'Authorization': 'Bearer ' + user2.token}

        # create file /path/to/file by user1
        dir_url = f'{server_url}/{user1.name}/path/to/'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, headers=headers1, data=BytesIO(payload)) \
                as response:
            assert response.status == 201

        params = {'op': 'MKDIRS'}
        async with client.put(dir_url + "/first/second", headers=headers1,
                              params=params) \
                as response:
            assert response.status == 201

        async with client.put(dir_url + "/first/third", headers=headers1,
                              params=params) \
                as response:
            assert response.status == 201

        async with client.put(dir_url + "/first/fourth", headers=headers1,
                              params=params) \
                as response:
            assert response.status == 201

        # list by user2
        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, headers=headers2, params=params) \
                as response:
            assert response.status == 404
            statuses = await response.text()

        await granter(user2.name, [
            {'uri': f'storage://{user1.name}/path/to/first/second',
             'action': 'read'}], user1)
        await granter(user2.name, [
            {'uri': f'storage://{user1.name}/path/to/first/third',
             'action': 'read'}], user1)
        async with client.get(dir_url + '/first',
                              headers=headers2,
                              params=params) \
                as response:
            assert response.status == 200
            statuses = await response.json()
            statuses.sort(key=self.file_status_sort)

            assert statuses == [{
                'path': 'second',
                'size': 0,
                'type': 'DIRECTORY',
            }, {
                'path': 'third',
                'size': 0,
                'type': 'DIRECTORY',
            }]
