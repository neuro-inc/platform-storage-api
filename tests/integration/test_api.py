from io import BytesIO
from typing import NamedTuple

import aiohttp
import aiohttp.web
import pytest

from platform_storage_api.api import create_app


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
async def api(local_fs):
    app = await create_app(local_fs)
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
