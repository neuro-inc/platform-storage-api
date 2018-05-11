from io import BytesIO
from typing import NamedTuple

import aiohttp
import aiohttp.web
import pytest

from platform_storage_api.api import create_app
from platform_storage_api.config import Config, ServerConfig, StorageConfig
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


@pytest.fixture
def config():
    server_config = ServerConfig()
    storage_config = StorageConfig(fs_local_base_path='/tmp/np_storage')
    return Config(server=server_config, storage=storage_config)


@pytest.fixture
async def storage(local_fs, config):
    return Storage(fs=local_fs, base_path=config.storage.fs_local_base_path)


@pytest.fixture
async def api(config, storage):
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
    async def test_put_get(self, api, client):
        url = api.storage_base_url + '/path/to/file'
        payload = b'test'
        async with client.put(url, data=BytesIO(payload)) as response:
            assert response.status == 201

        async with client.get(url) as response:
            assert response.status == 200
            result_payload = await response.read()
            assert result_payload == payload

    @pytest.mark.asyncio
    async def test_get_illegal_op(self, api, client):
        url = api.storage_base_url + '/path/to/file'
        params = {'op': 'CREATE'}
        async with client.get(url, params=params) as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = 'Illegal operation: CREATE'
            assert payload['error'] == expected_error

    @pytest.mark.asyncio
    async def test_liststatus(self, api, client):
        dir_url = api.storage_base_url + '/path/to'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, data=BytesIO(payload)) as response:
            assert response.status == 201

        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, params=params) as response:
            statuses = await response.json()
            assert statuses == [{
                'path': 'file',
                'size': len(payload),
                'type': 'FILE',
            }]

    @pytest.mark.asyncio
    async def test_liststatus_no_op_param_no_equals(self, api, client):
        dir_url = api.storage_base_url + '/path/to'
        url = dir_url + '/file'
        payload = b'test'
        async with client.put(url, data=BytesIO(payload)) as response:
            assert response.status == 201

        async with client.get(dir_url + '?liststatus') as response:
            statuses = await response.json()
            assert statuses == [{
                'path': 'file',
                'size': len(payload),
                'type': 'FILE',
            }]

    @pytest.mark.asyncio
    async def test_ambiguous_operations_with_op(self, api, client):
        dir_url = api.storage_base_url + '/'
        async with client.get(dir_url + '?op=liststatus&open') as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert 'Ambiguous operations' in payload['error']

    @pytest.mark.asyncio
    async def test_ambiguous_operations(self, api, client):
        dir_url = api.storage_base_url + '/'
        async with client.get(dir_url + '?liststatus&open') as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            assert 'Ambiguous operations' in payload['error']

    @pytest.mark.asyncio
    async def test_unknown_operation(self, api, client):
        dir_url = api.storage_base_url + '/'
        async with client.get(dir_url + '?op=unknown') as response:
            assert response.status == aiohttp.web.HTTPBadRequest.status_code
            payload = await response.json()
            expected_error = '\'UNKNOWN\' is not a valid StorageOperation'
            assert payload['error'] == expected_error

    @pytest.mark.asyncio
    async def test_liststatus_non_existent_dir(self, api, client):
        dir_url = api.storage_base_url + '/non-existent'

        params = {'op': 'LISTSTATUS'}
        async with client.get(dir_url, params=params) as response:
            assert response.status == 404
