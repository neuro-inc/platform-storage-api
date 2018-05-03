import tempfile
from pathlib import Path
import uuid

import pytest

from platform_storage_api.fs.local import (
    StorageType, FileStatus, FileStatusType, FileSystem, LocalFileSystem,
    copy_streams)


class TestFileSystem:
    @pytest.mark.asyncio
    async def test_create_local(self):
        fs = FileSystem.create(StorageType.LOCAL)
        try:
            assert isinstance(fs, LocalFileSystem)
        finally:
            await fs.close()

    def test_create_s3(self):
        with pytest.raises(ValueError, match='Unsupported storage type: s3'):
            FileSystem.create(StorageType.S3)


class TestLocalFileSystem:
    @pytest.fixture
    def tmp_dir_path(self):
        # although blocking, this is fine for tests
        with tempfile.TemporaryDirectory() as d:
            yield Path(d)

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
        async with fs.open(Path(tmp_file.name)) as f:
            payload = await f.read()
            assert not payload

    @pytest.mark.asyncio
    async def test_open_for_writing(self, fs, tmp_file):
        expected_payload = b'test'

        async with fs.open(Path(tmp_file.name), 'wb') as f:
            await f.write(expected_payload)
            await f.flush()

        async with fs.open(Path(tmp_file.name), 'rb') as f:
            payload = await f.read()
            assert payload == expected_payload

    @pytest.mark.asyncio
    async def test_copy_streams(self, fs, tmp_dir_path):
        expected_payload = b'test'
        chunk_size = 1

        out_filename = tmp_dir_path / str(uuid.uuid4())
        in_filename = tmp_dir_path / str(uuid.uuid4())

        async with fs.open(out_filename, mode='wb') as f:
            await f.write(b'test')
            await f.flush()

        async with fs.open(out_filename, mode='rb') as out_f:
            async with fs.open(in_filename, mode='wb') as in_f:
                await copy_streams(out_f, in_f, chunk_size=chunk_size)

        async with fs.open(in_filename, mode='rb') as f:
            payload = await f.read()
            assert payload == expected_payload

    @pytest.mark.asyncio
    async def test_listdir(self, fs, tmp_dir_path, tmp_file):
        files = await fs.listdir(tmp_dir_path)
        assert files == [Path(tmp_file.name)]

    @pytest.mark.asyncio
    async def test_listdir_empty(self, fs, tmp_dir_path):
        files = await fs.listdir(tmp_dir_path)
        assert not files

    @pytest.mark.asyncio
    async def test_mkdir(self, fs, tmp_dir_path):
        dir_name = 'new'
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

    @pytest.mark.asyncio
    async def test_mkdir_existing(self, fs, tmp_dir_path):
        dir_name = 'new'
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

        # should not fail
        await fs.mkdir(path)

    @pytest.mark.asyncio
    async def test_liststatus_single_empty_file(
            self, fs, tmp_dir_path, tmp_file):
        expected_path = Path(Path(tmp_file.name).name)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(expected_path, size=0, type=FileStatusType.FILE)]

    @pytest.mark.asyncio
    async def test_liststatus_single_file(
            self, fs, tmp_dir_path, tmp_file):
        expected_path = Path(Path(tmp_file.name).name)
        expected_payload = b'test'
        expected_size = len(expected_payload)
        async with fs.open(Path(tmp_file.name), 'wb') as f:
            await f.write(expected_payload)
            await f.flush()

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                expected_path, size=expected_size,
                type=FileStatusType.FILE)]

    @pytest.mark.asyncio
    async def test_liststatus_single_dir(self, fs, tmp_dir_path):
        expected_path = Path('nested')
        path = tmp_dir_path / expected_path
        await fs.mkdir(path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(expected_path, size=0, type=FileStatusType.DIRECTORY)]
