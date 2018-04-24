from io import BytesIO

import pytest

from platform_storage_api.storage import Storage


class AsyncBytesIO(BytesIO):
    async def read(self, *args, **kwargs):
        return super().read(*args, **kwargs)

    async def write(self, *args, **kwargs):
        return super().write(*args, **kwargs)


class TestStorage:
    @pytest.mark.asyncio
    async def test_store(self, local_fs, local_tmp_dir_path):
        base_path = local_tmp_dir_path
        storage = Storage(fs=local_fs, base_path=base_path)

        expected_payload = b'test'
        outstream = AsyncBytesIO(expected_payload)
        path = '/path/to/file'
        await storage.store(outstream, path)

        real_dir_path = local_tmp_dir_path / 'path/to'
        real_file_path = real_dir_path / 'file'
        files = await local_fs.listdir(real_dir_path)
        assert files == [real_file_path]

        async with local_fs.open(real_file_path, 'rb') as f:
            payload = await f.read()
            assert payload == expected_payload

    @pytest.mark.asyncio
    async def test_retrieve(self, local_fs, local_tmp_dir_path):
        base_path = local_tmp_dir_path
        storage = Storage(fs=local_fs, base_path=base_path)

        expected_payload = b'test'

        real_file_path = local_tmp_dir_path / 'file'
        async with local_fs.open(real_file_path, 'wb') as f:
            await f.write(expected_payload)

        instream = AsyncBytesIO()
        await storage.retrieve(instream, '/file')
        instream.seek(0)
        payload = await instream.read()
        assert payload == expected_payload
