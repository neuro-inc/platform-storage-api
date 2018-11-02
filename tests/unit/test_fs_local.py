import os
import tempfile
import uuid
from pathlib import Path, PurePath

import pytest

from platform_storage_api.fs.local import (
    FileStatus,
    FileStatusType,
    FileSystem,
    LocalFileSystem,
    StorageType,
    copy_streams,
)


class TestFileSystem:
    @pytest.mark.asyncio
    async def test_create_local(self):
        fs = FileSystem.create(StorageType.LOCAL)
        try:
            assert isinstance(fs, LocalFileSystem)
        finally:
            await fs.close()

    def test_create_s3(self):
        with pytest.raises(ValueError, match="Unsupported storage type: s3"):
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
        expected_payload = b"test"

        async with fs.open(Path(tmp_file.name), "wb") as f:
            await f.write(expected_payload)
            await f.flush()

        async with fs.open(Path(tmp_file.name), "rb") as f:
            payload = await f.read()
            assert payload == expected_payload

    @pytest.mark.asyncio
    async def test_copy_streams(self, fs, tmp_dir_path):
        expected_payload = b"test"
        chunk_size = 1

        out_filename = tmp_dir_path / str(uuid.uuid4())
        in_filename = tmp_dir_path / str(uuid.uuid4())

        async with fs.open(out_filename, mode="wb") as f:
            await f.write(b"test")
            await f.flush()

        async with fs.open(out_filename, mode="rb") as out_f:
            async with fs.open(in_filename, mode="wb") as in_f:
                await copy_streams(out_f, in_f, chunk_size=chunk_size)

        async with fs.open(in_filename, mode="rb") as f:
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
        dir_name = "new"
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

    @pytest.mark.asyncio
    async def test_mkdir_existing(self, fs, tmp_dir_path):
        dir_name = "new"
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

        # should not fail
        await fs.mkdir(path)

    @pytest.mark.asyncio
    async def test_liststatus_single_empty_file(self, fs, tmp_dir_path, tmp_file):
        expected_path = Path(Path(tmp_file.name).name)
        stat = os.stat(tmp_file.name)
        expected_mtime = int(stat.st_mtime)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                expected_path,
                size=0,
                type=FileStatusType.FILE,
                modification_time=expected_mtime,
            )
        ]

    @pytest.mark.asyncio
    async def test_liststatus_single_file(self, fs, tmp_dir_path, tmp_file):
        expected_path = Path(Path(tmp_file.name).name)
        expected_payload = b"test"
        expected_size = len(expected_payload)
        async with fs.open(Path(tmp_file.name), "wb") as f:
            await f.write(expected_payload)
            await f.flush()
        stat = os.stat(tmp_file.name)
        expected_mtime = int(stat.st_mtime)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                expected_path,
                size=expected_size,
                type=FileStatusType.FILE,
                modification_time=expected_mtime,
            )
        ]

    @pytest.mark.asyncio
    async def test_liststatus_single_dir(self, fs, tmp_dir_path):
        expected_path = Path("nested")
        path = tmp_dir_path / expected_path
        await fs.mkdir(path)
        stat = os.stat(path)
        expected_mtime = int(stat.st_mtime)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                expected_path,
                size=0,
                type=FileStatusType.DIRECTORY,
                modification_time=expected_mtime,
            )
        ]

    @pytest.mark.asyncio
    async def test_liststatus_non_existent_dir(self, fs, tmp_dir_path):
        path = tmp_dir_path / "nested"

        with pytest.raises(FileNotFoundError):
            await fs.liststatus(path)

    @pytest.mark.asyncio
    async def test_liststatus_empty_dir(self, fs, tmp_dir_path):
        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.asyncio
    async def test_rm_non_existent(self, fs, tmp_dir_path):
        path = tmp_dir_path / "nested"
        with pytest.raises(FileNotFoundError):
            await fs.remove(path)

    @pytest.mark.asyncio
    async def test_rm_empty_dir(self, fs, tmp_dir_path):
        expected_path = Path("nested")
        path = tmp_dir_path / expected_path
        await fs.mkdir(path)
        stat = os.stat(path)
        expected_mtime = int(stat.st_mtime)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                expected_path,
                size=0,
                type=FileStatusType.DIRECTORY,
                modification_time=expected_mtime,
            )
        ]

        await fs.remove(path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.asyncio
    async def test_rm_dir(self, fs, tmp_dir_path):
        expected_path = Path("nested")
        dir_path = tmp_dir_path / expected_path
        file_path = dir_path / "file"
        await fs.mkdir(dir_path)
        async with fs.open(file_path, mode="wb") as f:
            await f.write(b"test")
            await f.flush()

        stat = os.stat(file_path)
        expected_mtime = int(stat.st_mtime)

        statuses = await fs.liststatus(dir_path)
        assert statuses == [
            FileStatus(
                Path("file"),
                size=4,
                type=FileStatusType.FILE,
                modification_time=expected_mtime,
            )
        ]

        await fs.remove(dir_path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.asyncio
    async def test_rm_file(self, fs, tmp_dir_path):
        expected_path = Path("nested")
        path = tmp_dir_path / expected_path

        async with fs.open(path, mode="wb") as f:
            await f.write(b"test")
            await f.flush()
        stat = os.stat(path)
        expected_mtime = int(stat.st_mtime)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                expected_path,
                size=4,
                type=FileStatusType.FILE,
                modification_time=expected_mtime,
            )
        ]

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
        file_relative = Path("nested")
        expected_file_path = tmp_dir_path / file_relative

        payload = b"test"
        async with fs.open(expected_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()
        stat = os.stat(expected_file_path)
        expected_mtime = int(stat.st_mtime)

        status = await fs.get_filestatus(expected_file_path)
        assert status == FileStatus(
            path=expected_file_path,
            size=len(payload),
            type=FileStatusType.FILE,
            modification_time=expected_mtime,
        )

        await fs.remove(expected_file_path)

    @pytest.mark.asyncio
    async def test_get_filestatus_dir(self, fs, tmp_dir_path):
        file_relative = Path("nested")
        expected_file_path = tmp_dir_path / file_relative

        payload = b"test"
        async with fs.open(expected_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()
        stat = os.stat(expected_file_path)
        expected_mtime = int(stat.st_mtime)

        status = await fs.get_filestatus(tmp_dir_path)
        assert status == FileStatus(
            path=tmp_dir_path,
            size=0,
            type=FileStatusType.DIRECTORY,
            modification_time=expected_mtime,
        )

        await fs.remove(expected_file_path)

    # helper methods for working with sets of statuses
    @classmethod
    def statuses_get(cls, statuses, path):
        return next(st for st in statuses if st.path == path)

    @classmethod
    def statuses_add(cls, statuses, status):
        return statuses | {status}

    @classmethod
    def statuses_drop(cls, statuses, path):
        return set(filter(lambda st: st.path != path, statuses))

    @classmethod
    def statuses_rename(cls, statuses, old_path, new_path):
        def rename(status):
            if status.path == old_path:
                return FileStatus(path=new_path,
                                  type=status.type,
                                  size=status.size,
                                  modification_time=status.modification_time,
                                  permission=status.permission)
            else:
                return status

        return set(map(rename, statuses))

    @pytest.mark.asyncio
    async def test_rename_file_same_dir(self, fs, tmp_dir_path):
        old_name = PurePath('old')
        new_name = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_name
        new_path = tmp_dir_path / new_name

        async with fs.open(old_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        expected_statuses = self.statuses_rename(old_statuses,
                                                 old_name, new_name)
        assert statuses == expected_statuses

        async with fs.open(new_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

        await fs.remove(new_path)

    @pytest.mark.asyncio
    async def test_rename_file_different_dir(self, fs, tmp_dir_path):
        old_name = PurePath('old')
        subdir = PurePath('nested')
        new_name = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_name
        subdir_path = tmp_dir_path / subdir
        new_path = subdir_path / new_name

        async with fs.open(old_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        await fs.mkdir(subdir_path)

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        renaming_status = self.statuses_get(old_statuses, old_name)

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses | {renaming_status} == old_statuses

        statuses = set(await fs.liststatus(subdir_path))
        assert statuses == self.statuses_rename({renaming_status}, old_name, new_name)

        async with fs.open(new_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_nonexistent_dir(self, fs, tmp_dir_path):
        old_name = PurePath('old')
        subdir = PurePath('nested')
        new_name = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_name
        subdir_path = tmp_dir_path / subdir
        new_path = subdir_path / new_name

        async with fs.open(old_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        with pytest.raises(FileNotFoundError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        async with fs.open(old_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_to_dir(self, fs, tmp_dir_path):
        old_name = PurePath('old')
        subdir = PurePath('nested')
        payload = b'test'
        old_path = tmp_dir_path / old_name
        subdir_path = tmp_dir_path / subdir

        async with fs.open(old_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        await fs.mkdir(subdir_path)

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        with pytest.raises(IsADirectoryError):
            await fs.rename(old_path, subdir_path)

        statuses = set(await fs.liststatus(tmp_dir_path))

        assert statuses == old_statuses

        statuses = await fs.liststatus(subdir_path)
        assert statuses == []

        async with fs.open(old_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_no_file(self, fs, tmp_dir_path):
        old_name = PurePath('old')
        new_name = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_name
        new_path = tmp_dir_path / new_name

        async with fs.open(new_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        with pytest.raises(FileNotFoundError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        async with fs.open(new_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_self(self, fs, tmp_dir_path):
        name = PurePath('file')
        payload = b'test'
        path = tmp_dir_path / name

        async with fs.open(path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        await fs.rename(path, path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        async with fs.open(path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_to_existing_file(self, fs, tmp_dir_path):
        old_name = PurePath('old')
        new_name = PurePath('new')
        old_payload = b'test'
        new_payload = b'mississippi'
        old_path = tmp_dir_path / old_name
        new_path = tmp_dir_path / new_name

        async with fs.open(old_path, mode='wb') as f:
            await f.write(old_payload)
            await f.flush()

        async with fs.open(new_path, mode='wb') as f:
            await f.write(new_payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        expected_statuses = self.statuses_drop(old_statuses, new_name)
        expected_statuses = self.statuses_rename(expected_statuses,
                                                 old_name, new_name)
        assert statuses == expected_statuses

        async with fs.open(new_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == old_payload

    @pytest.mark.asyncio
    async def test_rename_dir_same_dir(self, fs, tmp_dir_path):
        file_name = PurePath('file')
        old_dir = PurePath('old')
        new_dir = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_dir
        old_file_path = old_path / file_name
        new_file_path = new_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == self.statuses_rename(old_statuses,
                                                old_dir, new_dir)

        statuses = set(await fs.liststatus(new_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(new_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_different_dir(self, fs, tmp_dir_path):
        file_name = PurePath('file')
        old_dir = PurePath('old')
        new_dir = PurePath('new')
        nested_dir = PurePath('nested')
        payload = b'test'
        old_path = tmp_dir_path / old_dir
        nested_path = tmp_dir_path / nested_dir
        new_path = nested_path / new_dir
        old_file_path = old_path / file_name
        new_file_path = new_path / file_name

        await fs.mkdir(old_path)
        await fs.mkdir(nested_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))
        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == self.statuses_drop(old_statuses, old_dir)

        statuses = set(await fs.liststatus(nested_path))
        expected_statuses = self.statuses_drop(old_statuses, nested_dir)
        expected_statuses = self.statuses_rename(expected_statuses,
                                                 old_dir, new_dir)
        assert statuses == expected_statuses

        statuses = set(await fs.liststatus(new_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(new_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_nonexistent_dir(self, fs, tmp_dir_path):
        file_name = PurePath('file')
        old_dir = PurePath('old')
        new_dir = PurePath('new')
        nested_dir = PurePath('nested')
        payload = b'test'
        old_path = tmp_dir_path / old_dir
        nested_path = tmp_dir_path / nested_dir
        new_path = nested_path / new_dir
        old_file_path = old_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        with pytest.raises(FileNotFoundError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        statuses = set(await fs.liststatus(old_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(old_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_file(self, fs, tmp_dir_path):
        old_dir = PurePath('old')
        new_file = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_file

        await fs.mkdir(old_path)

        async with fs.open(new_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        with pytest.raises(NotADirectoryError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        statuses = set(await fs.liststatus(old_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(new_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_empty_dir(self, fs, tmp_dir_path):
        file_name = PurePath('file')
        old_dir = PurePath('old')
        new_dir = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_dir
        old_file_path = old_path / file_name
        new_file_path = new_path / file_name

        await fs.mkdir(old_path)
        await fs.mkdir(new_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        expected_statuses = self.statuses_drop(old_statuses, new_dir)
        expected_statuses = self.statuses_rename(expected_statuses,
                                                 old_dir, new_dir)
        assert statuses == expected_statuses

        statuses = set(await fs.liststatus(new_path))
        statuses = old_statuses_old_dir

        async with fs.open(new_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_nonempty_dir(self, fs, tmp_dir_path):
        old_file_name = PurePath('old_file')
        new_file_name = PurePath('new_file')
        old_dir = PurePath('old')
        new_dir = PurePath('new')
        old_payload = b'test'
        new_payload = b'mississippi'
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_dir
        old_file_path = old_path / old_file_name
        new_file_path = new_path / new_file_name

        await fs.mkdir(old_path)
        await fs.mkdir(new_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(old_payload)
            await f.flush()

        async with fs.open(new_file_path, mode='wb') as f:
            await f.write(new_payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))
        old_statuses_new_dir = set(await fs.liststatus(new_path))

        with pytest.raises(OSError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        statuses = set(await fs.liststatus(old_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(old_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == old_payload

        statuses = set(await fs.liststatus(new_path))
        assert statuses == old_statuses_new_dir

        async with fs.open(new_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == new_payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_ancestor_dir(self, fs, tmp_dir_path):
        old_file_name = PurePath('old_file')
        new_file_name = PurePath('new_file')
        old_dir = PurePath('old')
        old_payload = b'test'
        new_payload = b'mississippi'
        old_path = tmp_dir_path / old_dir
        old_file_path = old_path / old_file_name
        new_file_path = tmp_dir_path / new_file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(old_payload)
            await f.flush()

        async with fs.open(new_file_path, mode='wb') as f:
            await f.write(new_payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        with pytest.raises(OSError):
            await fs.rename(old_path, tmp_dir_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        statuses = set(await fs.liststatus(old_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(old_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == old_payload

        async with fs.open(new_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == new_payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_descended_dir(self, fs, tmp_dir_path):
        file_name = PurePath('file')
        old_dir = PurePath('old')
        new_dir = PurePath('new')
        payload = b'test'
        old_path = tmp_dir_path / old_dir
        new_path = old_path / new_dir
        old_file_path = old_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        with pytest.raises(OSError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        statuses = set(await fs.liststatus(old_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(old_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_dot(self, fs, tmp_dir_path):
        file_name = PurePath('file')
        old_dir = PurePath('old')
        payload = b'test'
        old_path = tmp_dir_path / old_dir
        old_file_path = old_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode='wb') as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        with pytest.raises(OSError):
            await fs.rename(old_path, tmp_dir_path / '.')

        with pytest.raises(OSError):
            await fs.rename(old_path, tmp_dir_path / '..')

        with pytest.raises(OSError):
            await fs.rename(tmp_dir_path / '.', old_path)

        with pytest.raises(OSError):
            await fs.rename(tmp_dir_path / '..', old_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        statuses = set(await fs.liststatus(old_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(old_file_path, mode='rb') as f:
            real_payload = await f.read()
            assert real_payload == payload
