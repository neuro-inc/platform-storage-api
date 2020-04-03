import os
import tempfile
import uuid
from pathlib import Path, PurePath
from typing import AsyncIterator, Iterable, Iterator, Set
from unittest import mock

import pytest

from platform_storage_api.fs.local import (
    FileStatus,
    FileStatusType,
    FileSystem,
    LocalFileSystem,
    StorageType,
    copy_streams,
)
from platform_storage_api.trace import CURRENT_TRACER


class TestFileSystem:
    @pytest.mark.asyncio
    async def test_create_local(self) -> None:
        fs = FileSystem.create(StorageType.LOCAL)
        try:
            assert isinstance(fs, LocalFileSystem)
        finally:
            await fs.close()

    def test_create_s3(self) -> None:
        with pytest.raises(ValueError, match="Unsupported storage type: s3"):
            FileSystem.create(StorageType.S3)


class TestLocalFileSystem:
    @pytest.fixture(autouse=True)
    def setup_tracer(self) -> None:
        CURRENT_TRACER.set(mock.MagicMock())

    @pytest.fixture
    def tmp_dir_path(self) -> Iterator[Path]:
        # although blocking, this is fine for tests
        with tempfile.TemporaryDirectory() as d:
            yield Path(d)

    @pytest.fixture
    def tmp_file_path(self, tmp_dir_path: Path) -> Iterator[Path]:
        # although blocking, this is fine for tests
        with tempfile.NamedTemporaryFile(dir=tmp_dir_path) as f:
            f.flush()
            yield Path(f.name)

    @pytest.fixture
    async def fs(self) -> AsyncIterator[FileSystem]:
        async with FileSystem.create(StorageType.LOCAL) as fs:
            yield fs

    @pytest.mark.asyncio
    async def test_open_empty_file_for_reading(
        self, fs: FileSystem, tmp_file_path: Path
    ) -> None:
        async with fs.open(tmp_file_path) as f:
            payload = await f.read()
            assert not payload

    @pytest.mark.asyncio
    async def test_open_for_writing(self, fs: FileSystem, tmp_file_path: Path) -> None:
        expected_payload = b"test"

        async with fs.open(tmp_file_path, "wb") as f:
            await f.write(expected_payload)
            await f.flush()

        async with fs.open(tmp_file_path, "rb") as f:
            payload = await f.read()
            assert payload == expected_payload

    @pytest.mark.asyncio
    async def test_copy_streams(self, fs: FileSystem, tmp_dir_path: Path) -> None:
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
    async def test_listdir(
        self, fs: FileSystem, tmp_dir_path: Path, tmp_file_path: Path
    ) -> None:
        files = await fs.listdir(tmp_dir_path)
        assert files == [tmp_file_path]

    @pytest.mark.asyncio
    async def test_listdir_empty(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        files = await fs.listdir(tmp_dir_path)
        assert not files

    @pytest.mark.asyncio
    async def test_mkdir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        dir_name = "new"
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

    @pytest.mark.asyncio
    async def test_mkdir_existing(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        dir_name = "new"
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

        # should not fail
        await fs.mkdir(path)

    @pytest.mark.asyncio
    async def test_liststatus_single_empty_file(
        self, fs: FileSystem, tmp_dir_path: Path, tmp_file_path: Path
    ) -> None:
        expected_path = Path(tmp_file_path.name)
        stat = os.stat(tmp_file_path)
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
    async def test_liststatus_single_file(
        self, fs: FileSystem, tmp_dir_path: Path, tmp_file_path: Path
    ) -> None:
        expected_path = Path(tmp_file_path.name)
        expected_payload = b"test"
        expected_size = len(expected_payload)
        async with fs.open(tmp_file_path, "wb") as f:
            await f.write(expected_payload)
            await f.flush()
        stat = os.stat(tmp_file_path)
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
    async def test_liststatus_single_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
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
    async def test_liststatus_many_files(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        expected = []
        for i in range(5000):
            name = f"file-{i}"
            expected.append(PurePath(name))
            async with fs.open(tmp_dir_path / name, "wb"):
                pass
        statuses = await fs.liststatus(tmp_dir_path)
        actual = [status.path for status in statuses]
        assert sorted(actual) == sorted(expected)

    @pytest.mark.asyncio
    async def test_iterstatus_many_files(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        expected = []
        for i in range(5000):
            name = f"file-{i}"
            expected.append(PurePath(name))
            async with fs.open(tmp_dir_path / name, "wb"):
                pass
        async with fs.iterstatus(tmp_dir_path) as it:
            statuses = [status async for status in it]
        actual = [status.path for status in statuses]
        assert sorted(actual) == sorted(expected)

    @pytest.mark.asyncio
    async def test_liststatus_non_existent_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"

        with pytest.raises(FileNotFoundError):
            await fs.liststatus(path)

    @pytest.mark.asyncio
    async def test_liststatus_empty_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.asyncio
    async def test_iterstatus_non_existent_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"

        cm = fs.iterstatus(path)
        with pytest.raises(FileNotFoundError):
            async with cm:
                pass

    @pytest.mark.asyncio
    async def test_iterstatus_broken_directory_link(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"
        os.symlink("nonexisting", path, target_is_directory=True)

        cm = fs.iterstatus(path)
        with pytest.raises(FileNotFoundError):
            async with cm:
                pass

    @pytest.mark.asyncio
    async def test_iterstatus_broken_entry_link(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"
        os.symlink("nonexisting", path)

        async with fs.iterstatus(tmp_dir_path) as it:
            with pytest.raises(FileNotFoundError):
                await it.__anext__()

    @pytest.mark.asyncio
    async def test_rm_non_existent(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        path = tmp_dir_path / "nested"
        with pytest.raises(FileNotFoundError):
            await fs.remove(path)

    @pytest.mark.asyncio
    async def test_rm_empty_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
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
    async def test_rm_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
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
    async def test_rm_file(self, fs: FileSystem, tmp_dir_path: Path) -> None:
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

    @pytest.mark.asyncio
    async def test_rm_symlink_to_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        dir_path = tmp_dir_path / "dir"
        await fs.mkdir(dir_path)
        link_path = tmp_dir_path / "link"
        link_path.symlink_to("dir", target_is_directory=True)

        stat = os.stat(dir_path)
        expected_mtime = int(stat.st_mtime)

        statuses = await fs.liststatus(tmp_dir_path)
        assert sorted(statuses, key=lambda s: s.path) == [
            FileStatus(
                Path("dir"),
                size=0,
                type=FileStatusType.DIRECTORY,
                modification_time=expected_mtime,
            ),
            FileStatus(
                Path("link"),
                size=0,
                type=FileStatusType.DIRECTORY,
                modification_time=expected_mtime,
            ),
        ]

        await fs.remove(link_path)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                Path("dir"),
                size=0,
                type=FileStatusType.DIRECTORY,
                modification_time=expected_mtime,
            )
        ]

    @pytest.mark.asyncio
    async def test_get_filestatus_file(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
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
    async def test_get_filestatus_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
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

    @pytest.mark.asyncio
    async def test_exists_file(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        file_relative = Path("nested")
        expected_file_path = tmp_dir_path / file_relative

        payload = b"test"
        async with fs.open(expected_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        assert await fs.exists(expected_file_path)
        await fs.remove(expected_file_path)

    @pytest.mark.asyncio
    async def test_exists_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        file_relative = Path("nested")
        expected_file_path = tmp_dir_path / file_relative

        payload = b"test"
        async with fs.open(expected_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        assert await fs.exists(tmp_dir_path)
        await fs.remove(expected_file_path)

    @pytest.mark.asyncio
    async def test_exists_unknown(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        assert not await fs.exists(Path("unknown-name"))

    # helper methods for working with sets of statuses
    @classmethod
    def statuses_get(cls, statuses: Iterable[FileStatus], path: PurePath) -> FileStatus:
        return next(st for st in statuses if st.path == path)

    @classmethod
    def statuses_add(
        cls, statuses: Set[FileStatus], status: FileStatus
    ) -> Set[FileStatus]:
        return statuses | {status}

    @classmethod
    def statuses_drop(
        cls, statuses: Iterable[FileStatus], path: PurePath
    ) -> Set[FileStatus]:
        return set(filter(lambda st: st.path != path, statuses))

    @classmethod
    def statuses_rename(
        cls, statuses: Iterable[FileStatus], old_path: PurePath, new_path: PurePath
    ) -> Set[FileStatus]:
        def rename(status: FileStatus) -> FileStatus:
            if status.path == old_path:
                return FileStatus(
                    path=new_path,
                    type=status.type,
                    size=status.size,
                    modification_time=status.modification_time,
                    permission=status.permission,
                )
            else:
                return status

        return set(map(rename, statuses))

    @pytest.mark.asyncio
    async def test_rename_file_same_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        old_name = PurePath("old")
        new_name = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_name
        new_path = tmp_dir_path / new_name

        async with fs.open(old_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        expected_statuses = self.statuses_rename(old_statuses, old_name, new_name)
        assert statuses == expected_statuses

        async with fs.open(new_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

        await fs.remove(new_path)

    @pytest.mark.asyncio
    async def test_rename_file_different_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        old_name = PurePath("old")
        subdir = PurePath("nested")
        new_name = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_name
        subdir_path = tmp_dir_path / subdir
        new_path = subdir_path / new_name

        async with fs.open(old_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        await fs.mkdir(subdir_path)

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        renaming_status = self.statuses_get(old_statuses, old_name)
        old_subdir_status = self.statuses_get(old_statuses, subdir)

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        new_subdir_status = self.statuses_get(statuses, subdir)
        assert new_subdir_status.modification_time is not None
        assert old_subdir_status.modification_time is not None
        assert (
            new_subdir_status.modification_time >= old_subdir_status.modification_time
        )
        assert statuses | {renaming_status} | {old_subdir_status} == old_statuses | {
            new_subdir_status
        }

        statuses = set(await fs.liststatus(subdir_path))
        assert statuses == self.statuses_rename({renaming_status}, old_name, new_name)

        async with fs.open(new_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_nonexistent_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        old_name = PurePath("old")
        subdir = PurePath("nested")
        new_name = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_name
        subdir_path = tmp_dir_path / subdir
        new_path = subdir_path / new_name

        async with fs.open(old_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        with pytest.raises(FileNotFoundError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        async with fs.open(old_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_to_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        old_name = PurePath("old")
        subdir = PurePath("nested")
        payload = b"test"
        old_path = tmp_dir_path / old_name
        subdir_path = tmp_dir_path / subdir

        async with fs.open(old_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        await fs.mkdir(subdir_path)

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        with pytest.raises(IsADirectoryError):
            await fs.rename(old_path, subdir_path)

        statuses = set(await fs.liststatus(tmp_dir_path))

        assert statuses == old_statuses

        statuses_list = await fs.liststatus(subdir_path)
        assert statuses_list == []

        async with fs.open(old_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_no_file(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        old_name = PurePath("old")
        new_name = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_name
        new_path = tmp_dir_path / new_name

        async with fs.open(new_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        with pytest.raises(FileNotFoundError):
            await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        async with fs.open(new_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_self(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        name = PurePath("file")
        payload = b"test"
        path = tmp_dir_path / name

        async with fs.open(path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        await fs.rename(path, path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        async with fs.open(path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_file_to_existing_file(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        old_name = PurePath("old")
        new_name = PurePath("new")
        old_payload = b"test"
        new_payload = b"mississippi"
        old_path = tmp_dir_path / old_name
        new_path = tmp_dir_path / new_name

        async with fs.open(old_path, mode="wb") as f:
            await f.write(old_payload)
            await f.flush()

        async with fs.open(new_path, mode="wb") as f:
            await f.write(new_payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        expected_statuses = self.statuses_drop(old_statuses, new_name)
        expected_statuses = self.statuses_rename(expected_statuses, old_name, new_name)
        assert statuses == expected_statuses

        async with fs.open(new_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == old_payload

    @pytest.mark.asyncio
    async def test_rename_dir_same_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        file_name = PurePath("file")
        old_dir = PurePath("old")
        new_dir = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_dir
        old_file_path = old_path / file_name
        new_file_path = new_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == self.statuses_rename(old_statuses, old_dir, new_dir)

        statuses = set(await fs.liststatus(new_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(new_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_different_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        file_name = PurePath("file")
        old_dir = PurePath("old")
        new_dir = PurePath("new")
        nested_dir = PurePath("nested")
        payload = b"test"
        old_path = tmp_dir_path / old_dir
        nested_path = tmp_dir_path / nested_dir
        new_path = nested_path / new_dir
        old_file_path = old_path / file_name
        new_file_path = new_path / file_name

        await fs.mkdir(old_path)
        await fs.mkdir(nested_path)

        async with fs.open(old_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))
        renaming_status = self.statuses_get(old_statuses, old_dir)
        old_subdir_status = self.statuses_get(old_statuses, nested_dir)
        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        new_subdir_status = self.statuses_get(statuses, nested_dir)
        assert new_subdir_status.modification_time is not None
        assert old_subdir_status.modification_time is not None
        assert (
            new_subdir_status.modification_time >= old_subdir_status.modification_time
        )
        assert statuses | {renaming_status} | {old_subdir_status} == old_statuses | {
            new_subdir_status
        }

        statuses = set(await fs.liststatus(nested_path))
        expected_statuses = self.statuses_drop(old_statuses, nested_dir)
        expected_statuses = self.statuses_rename(expected_statuses, old_dir, new_dir)
        assert statuses == expected_statuses

        statuses = set(await fs.liststatus(new_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(new_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_nonexistent_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        file_name = PurePath("file")
        old_dir = PurePath("old")
        new_dir = PurePath("new")
        nested_dir = PurePath("nested")
        payload = b"test"
        old_path = tmp_dir_path / old_dir
        nested_path = tmp_dir_path / nested_dir
        new_path = nested_path / new_dir
        old_file_path = old_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode="wb") as f:
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

        async with fs.open(old_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_file(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        old_dir = PurePath("old")
        new_file = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_file

        await fs.mkdir(old_path)

        async with fs.open(new_path, mode="wb") as f:
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

        async with fs.open(new_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_empty_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        file_name = PurePath("file")
        old_dir = PurePath("old")
        new_dir = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_dir
        old_file_path = old_path / file_name
        new_file_path = new_path / file_name

        await fs.mkdir(old_path)
        await fs.mkdir(new_path)

        async with fs.open(old_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        await fs.rename(old_path, new_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        expected_statuses = self.statuses_drop(old_statuses, new_dir)
        expected_statuses = self.statuses_rename(expected_statuses, old_dir, new_dir)
        assert statuses == expected_statuses

        statuses = set(await fs.liststatus(new_path))
        statuses = old_statuses_old_dir

        async with fs.open(new_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_nonempty_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        old_file_name = PurePath("old_file")
        new_file_name = PurePath("new_file")
        old_dir = PurePath("old")
        new_dir = PurePath("new")
        old_payload = b"test"
        new_payload = b"mississippi"
        old_path = tmp_dir_path / old_dir
        new_path = tmp_dir_path / new_dir
        old_file_path = old_path / old_file_name
        new_file_path = new_path / new_file_name

        await fs.mkdir(old_path)
        await fs.mkdir(new_path)

        async with fs.open(old_file_path, mode="wb") as f:
            await f.write(old_payload)
            await f.flush()

        async with fs.open(new_file_path, mode="wb") as f:
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

        async with fs.open(old_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == old_payload

        statuses = set(await fs.liststatus(new_path))
        assert statuses == old_statuses_new_dir

        async with fs.open(new_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == new_payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_ancestor_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        old_file_name = PurePath("old_file")
        new_file_name = PurePath("new_file")
        old_dir = PurePath("old")
        old_payload = b"test"
        new_payload = b"mississippi"
        old_path = tmp_dir_path / old_dir
        old_file_path = old_path / old_file_name
        new_file_path = tmp_dir_path / new_file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode="wb") as f:
            await f.write(old_payload)
            await f.flush()

        async with fs.open(new_file_path, mode="wb") as f:
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

        async with fs.open(old_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == old_payload

        async with fs.open(new_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == new_payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_descended_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        file_name = PurePath("file")
        old_dir = PurePath("old")
        new_dir = PurePath("new")
        payload = b"test"
        old_path = tmp_dir_path / old_dir
        new_path = old_path / new_dir
        old_file_path = old_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode="wb") as f:
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

        async with fs.open(old_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload

    @pytest.mark.asyncio
    async def test_rename_dir_to_dot(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        file_name = PurePath("file")
        old_dir = PurePath("old")
        payload = b"test"
        old_path = tmp_dir_path / old_dir
        old_file_path = old_path / file_name

        await fs.mkdir(old_path)

        async with fs.open(old_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        old_statuses = set(await fs.liststatus(tmp_dir_path))
        old_statuses_old_dir = set(await fs.liststatus(old_path))

        with pytest.raises(OSError):
            await fs.rename(old_path, tmp_dir_path / ".")

        with pytest.raises(OSError):
            await fs.rename(old_path, tmp_dir_path / "..")

        with pytest.raises(OSError):
            await fs.rename(tmp_dir_path / ".", old_path)

        with pytest.raises(OSError):
            await fs.rename(tmp_dir_path / "..", old_path)

        statuses = set(await fs.liststatus(tmp_dir_path))
        assert statuses == old_statuses

        statuses = set(await fs.liststatus(old_path))
        assert statuses == old_statuses_old_dir

        async with fs.open(old_file_path, mode="rb") as f:
            real_payload = await f.read()
            assert real_payload == payload
