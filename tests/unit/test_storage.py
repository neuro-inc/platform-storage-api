import os
from io import BytesIO
from pathlib import PurePath

import pytest

from platform_storage_api.fs.local import FileStatusType, LocalFileSystem
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

        expected_payload = b"test"
        outstream = AsyncBytesIO(expected_payload)
        path = "/path/to/file"
        await storage.store(outstream, path)

        real_dir_path = local_tmp_dir_path / "path/to"
        real_file_path = real_dir_path / "file"
        files = await local_fs.listdir(real_dir_path)
        assert files == [real_file_path]

        async with local_fs.open(real_file_path, "rb") as f:
            payload = await f.read()
            assert payload == expected_payload

    @pytest.mark.asyncio
    async def test_retrieve(self, local_fs, local_tmp_dir_path):
        base_path = local_tmp_dir_path
        storage = Storage(fs=local_fs, base_path=base_path)

        expected_payload = b"test"

        real_file_path = local_tmp_dir_path / "file"
        async with local_fs.open(real_file_path, "wb") as f:
            await f.write(expected_payload)

        instream = AsyncBytesIO()
        await storage.retrieve(instream, "/file")
        instream.seek(0)
        payload = await instream.read()
        assert payload == expected_payload

    @pytest.mark.asyncio
    async def test_filestatus_file(
        self, local_fs: LocalFileSystem, local_tmp_dir_path: PurePath
    ):
        base_path = local_tmp_dir_path
        storage = Storage(fs=local_fs, base_path=base_path)

        file_name = "file.txt"
        real_file_path = local_tmp_dir_path / file_name
        async with local_fs.open(real_file_path, "wb") as f:
            await f.write(b"test")

        storage_stat = await storage.get_filestatus(f"/{file_name}")

        real_stat = os.stat(str(real_file_path))

        assert storage_stat.type == FileStatusType.FILE
        assert storage_stat.path == real_file_path
        assert storage_stat.length == real_stat.st_size
        assert storage_stat.modification_time == int(real_stat.st_mtime)

    @pytest.mark.asyncio
    async def test_filestatus_dir(
        self, local_fs: LocalFileSystem, local_tmp_dir_path: PurePath
    ):
        base_path = local_tmp_dir_path
        storage = Storage(fs=local_fs, base_path=base_path)

        dir_name = "dir/"
        real_dir_path = local_tmp_dir_path / dir_name
        await local_fs.mkdir(real_dir_path)

        storage_stat = await storage.get_filestatus(f"/{dir_name}")

        real_stat = os.stat(str(real_dir_path))

        assert storage_stat.type == FileStatusType.DIRECTORY
        assert storage_stat.path == real_dir_path
        assert storage_stat.length == 0
        assert storage_stat.modification_time == int(real_stat.st_mtime)
