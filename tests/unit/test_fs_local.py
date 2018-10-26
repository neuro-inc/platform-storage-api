import tempfile
import uuid
from pathlib import Path
from time import time as current_time

import pytest

from platform_storage_api.fs.local import (FileStatus, FileStatusType,
                                           FileSystem, LocalFileSystem,
                                           StorageType, copy_streams)


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

    @pytest.mark.asyncio
    async def test_liststatus_non_existent_dir(self, fs, tmp_dir_path):
        path = tmp_dir_path / 'nested'

        with pytest.raises(FileNotFoundError):
            await fs.liststatus(path)

    @pytest.mark.asyncio
    async def test_liststatus_empty_dir(self, fs, tmp_dir_path):
        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.asyncio
    async def test_rm_non_existent(self, fs, tmp_dir_path):
        path = tmp_dir_path / 'nested'
        with pytest.raises(FileNotFoundError):
            await fs.remove(path)

    @pytest.mark.asyncio
    async def test_rm_empty_dir(self, fs, tmp_dir_path):
        expected_path = Path('nested')
        path = tmp_dir_path / expected_path
        await fs.mkdir(path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(expected_path, size=0, type=FileStatusType.DIRECTORY)]

        await fs.remove(path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.asyncio
    async def test_rm_dir(self, fs, tmp_dir_path):
        expected_path = Path('nested')
        dir_path = tmp_dir_path / expected_path
        file_path = dir_path / 'file'
        await fs.mkdir(dir_path)

        async with fs.open(file_path, mode='wb') as f:
            await f.write(b'test')
            await f.flush()

        statuses = await fs.liststatus(dir_path)
        assert statuses == [
            FileStatus(Path('file'), size=4, type=FileStatusType.FILE)]

        await fs.remove(dir_path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.asyncio
    async def test_rm_file(self, fs, tmp_dir_path):
        expected_path = Path('nested')
        path = tmp_dir_path / expected_path

        async with fs.open(path, mode='wb') as f:
            await f.write(b'test')
            await f.flush()

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(expected_path, size=4, type=FileStatusType.FILE)]

        await fs.remove(path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []


    @classmethod
    def assert_filestatus(cls, actual: FileStatus, **expected):
        for key in ['path', 'size', 'type']:
            assert actual.__getattribute__(key) == expected[key]
        mkey = 'modification_time'
        assert actual.__getattribute__(mkey) >= expected[mkey]

    @pytest.mark.asyncio
    async def test_get_filestatus_file(self, fs, tmp_dir_path):
        expected_mtime_min = int(current_time())
        file_relative = Path('nested')
        expected_file_path = tmp_dir_path / file_relative

        payload = b'test'
        async with fs.open(expected_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        status = await fs.get_filestatus(expected_file_path)
        self.assert_filestatus(status, path=expected_file_path,
                                       size=len(payload),
                                       type=FileStatusType.FILE,
                                       modification_time=expected_mtime_min)

        await fs.remove(expected_file_path)

    @pytest.mark.asyncio
    async def test_get_filestatus_file(self, fs, tmp_dir_path):
        expected_mtime_min = int(current_time())
        file_relative = Path('nested')
        expected_file_path = tmp_dir_path / file_relative

        payload = b'test'
        async with fs.open(expected_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        status = await fs.get_filestatus(tmp_dir_path)
        self.assert_filestatus(status, path=tmp_dir_path,
                                       size=0,
                                       type=FileStatusType.DIRECTORY,
                                       modification_time=expected_mtime_min)

        await fs.remove(expected_file_path)
