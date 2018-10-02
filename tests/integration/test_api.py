import os
import uuid
from io import BytesIO
from pathlib import PurePath
from typing import NamedTuple, Optional

import aiohttp
import aiohttp.web
import pytest
from dataclasses import dataclass
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
