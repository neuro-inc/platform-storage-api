from io import BytesIO

import pytest

from platform_storage_api.storage import Storage


class TestStorage:
    @pytest.mark.asyncio
    async def test_store(self, local_fs, local_tmp_dir_path):
        base_path = local_tmp_dir_path
        storage = Storage(fs=local_fs, base_path=base_path)

        class AsyncBytesIO(BytesIO):
            async def read(self, *args, **kwargs):
                return super().read(*args, **kwargs)

        expected_payload = b'test'
        stream = AsyncBytesIO(expected_payload)
        path = '/path/to/file'
        await storage.store(stream, path)

        real_dir_path = local_tmp_dir_path / 'path/to'
        real_file_path = real_dir_path / 'file'
        files = await local_fs.listdir(real_dir_path)
        assert files == [real_file_path]

        async with local_fs.open(real_file_path, 'rb') as f:
            payload = await f.read()
            assert payload == expected_payload
