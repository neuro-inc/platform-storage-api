import aiohttp
import pytest


class TestStorage:
    @pytest.mark.asyncio
    async def test_put_multiple_get(self, server_url,
                                    client, regular_user_factory, api):
        user = await regular_user_factory()
        url = f'{server_url}/{user.name}/path/to/files'
        payload1 = b'test1'
        payload2 = b'test2'

        with aiohttp.MultipartWriter('mixed') as mpwriter:
            # file 1
            value = 'attachment; filename="file1.txt"'
            headers = {'Content-Disposition': value}
            mpwriter.append(payload1, headers)
            # file 2
            value = 'attachment; filename="file2.txt"'
            headers = {'Content-Disposition': value}
            mpwriter.append(payload2, headers)

        headers = {'Authorization': 'Bearer ' + user.token}
        async with client.put(url, headers=headers, data=mpwriter) \
                as response:
            assert response.status == 201

        async with client.get(url + '/file1.txt', headers=headers) as response:
            assert response.status == 200
            result_payload = await response.read()
            assert result_payload == payload1

        async with client.get(url + '/file2.txt', headers=headers) as response:
            assert response.status == 200
            result_payload = await response.read()
            assert result_payload == payload2
