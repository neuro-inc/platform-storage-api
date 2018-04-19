import os
import tempfile

import pytest

from platform_storage_api.fs.local import StorageType, FileSystem


class TestLocalFileSystem:
    @pytest.fixture
    def tmp_dir_path(self):
        # although blocking, this is fine for tests
        with tempfile.TemporaryDirectory() as d:
            yield d

    @pytest.fixture
    def tmp_file(self, tmp_dir_path):
        # although blocking, this is fine for tests
        with tempfile.NamedTemporaryFile(dir=tmp_dir_path) as f:
            f.flush()
            yield f

    @pytest.fixture
    async def fs(self):
        async with FileSystem.create(StorageType.LOCAL) as fs:
            yield fs

    @pytest.mark.asyncio
    async def test_open_empty_file_for_reading(self, fs, tmp_file):
        async with fs.open(tmp_file.name) as f:
            payload = await f.read()
            assert not payload

    @pytest.mark.asyncio
    async def test_open_for_writing(self, fs, tmp_file):
        expected_payload = b'test'

        async with fs.open(tmp_file.name, 'wb') as f:
            await f.write(expected_payload)
            await f.flush()

        async with fs.open(tmp_file.name, 'rb') as f:
            payload = await f.read()
            assert payload == expected_payload
