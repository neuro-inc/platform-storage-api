import os
import shutil
import tempfile
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Iterable, Iterator
from pathlib import Path, PurePath
from typing import Any
from unittest import mock

import pytest
import pytest_asyncio

from platform_storage_api.fs.local import (
    FileStatus,
    FileStatusType,
    FileSystem,
    FileUsage,
    LocalFileSystem,
    StorageType,
    copy_streams,
    parse_du_size_output,
)


@pytest.mark.parametrize(
    ["size_str", "size"],
    [
        ("3", 3),
        ("3K", 3 * 1024),
        ("3M", 3 * 1024**2),
        ("3G", 3 * 1024**3),
        ("3T", 3 * 1024**4),
        ("3P", 3 * 1024**5),
        ("3E", 3 * 1024**6),
        ("3Z", 3 * 1024**7),
        ("3Y", 3 * 1024**8),
        ("3KB", 3 * 1000),
        ("3KiB", 3 * 1024),
        ("3.1KB", 3100),
    ],
)
def test_parse_du_size_output(size_str: str, size: int) -> None:
    assert parse_du_size_output(size_str) == size


class TestFileSystem:
    async def test_create_local(self) -> None:
        fs = FileSystem.create(StorageType.LOCAL)
        try:
            assert isinstance(fs, LocalFileSystem)
        finally:
            await fs.close()

    def test_create_s3(self) -> None:
        with pytest.raises(ValueError, match="Unsupported storage type: s3"):
            FileSystem.create(StorageType.S3)


async def remove_normal(fs: FileSystem, path: PurePath, recursive: bool) -> None:
    await fs.remove(path, recursive=recursive)


async def remove_iter(fs: FileSystem, path: PurePath, recursive: bool) -> None:
    async for _ in fs.iterremove(path, recursive=recursive):
        pass


RemoveMethod = Callable[[FileSystem, PurePath, bool], Coroutine[Any, Any, None]]


class TestLocalFileSystem:
    @pytest.fixture
    def tmp_dir_path(self) -> Iterator[Path]:
        # although blocking, this is fine for tests
        with tempfile.TemporaryDirectory() as d:
            yield Path(os.path.realpath(d))

    @pytest.fixture
    def tmp_file_path(self, tmp_dir_path: Path) -> Iterator[Path]:
        # although blocking, this is fine for tests
        with tempfile.NamedTemporaryFile(dir=tmp_dir_path) as f:
            f.flush()
            yield Path(f.name)

    @pytest.fixture
    def path_with_symlink(self, tmp_dir_path: Path) -> Iterator[Path]:
        (tmp_dir_path / "dir").mkdir()
        (tmp_dir_path / "dir/subdir").mkdir()
        os.symlink("dir", tmp_dir_path / "link")
        yield tmp_dir_path / "link/subdir"

    @pytest.fixture
    def symlink_to_dir(self, tmp_dir_path: Path) -> Iterator[Path]:
        (tmp_dir_path / "dir").mkdir()
        path = tmp_dir_path / "dirlink"
        os.symlink("dir", path, target_is_directory=True)
        assert path.is_dir()
        yield path

    @pytest.fixture
    def symlink_to_file(self, tmp_dir_path: Path) -> Iterator[Path]:
        (tmp_dir_path / "file").write_bytes(b"test")
        path = tmp_dir_path / "filelink"
        os.symlink("file", path)
        assert path.is_file()
        yield path

    @pytest.fixture
    def free_symlink(self, tmp_dir_path: Path) -> Iterator[Path]:
        path = tmp_dir_path / "nonexistentlink"
        os.symlink("nonexistent", path)
        yield path

    @pytest_asyncio.fixture
    async def fs(self) -> AsyncIterator[FileSystem]:
        async with FileSystem.create(StorageType.LOCAL) as fs:
            yield fs

    async def test_open_empty_file_for_reading(
        self, fs: FileSystem, tmp_file_path: Path
    ) -> None:
        async with fs.open(tmp_file_path) as f:
            payload = await f.read()
            assert not payload

    async def test_open_for_writing(self, fs: FileSystem, tmp_file_path: Path) -> None:
        expected_payload = b"test"

        async with fs.open(tmp_file_path, "wb") as f:
            await f.write(expected_payload)
            await f.flush()

        async with fs.open(tmp_file_path, "rb") as f:
            payload = await f.read()
            assert payload == expected_payload

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

    async def test_open_symlink(self, fs: FileSystem, symlink_to_file: Path) -> None:
        for mode in "rb", "rb+", "wb", "xb":
            with pytest.raises(FileNotFoundError):
                async with fs.open(symlink_to_file, mode):
                    pass

    async def test_open_free_symlink(self, fs: FileSystem, free_symlink: Path) -> None:
        for mode in "rb", "rb+", "wb", "xb":
            with pytest.raises(FileNotFoundError):
                async with fs.open(free_symlink, mode):
                    pass
            assert not free_symlink.exists()

    async def test_open_path_with_symlink(
        self, fs: FileSystem, path_with_symlink: Path
    ) -> None:
        path = path_with_symlink / "file"
        path.write_bytes(b"")

        for mode in "rb", "rb+", "wb", "xb":
            with pytest.raises(FileNotFoundError):
                async with fs.open(path, mode):
                    pass

    async def test_listdir(
        self, fs: FileSystem, tmp_dir_path: Path, tmp_file_path: Path
    ) -> None:
        files = await fs.listdir(tmp_dir_path)
        assert files == [tmp_file_path]

    async def test_listdir_empty_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        files = await fs.listdir(tmp_dir_path)
        assert not files

    async def test_listdir_symlink(self, fs: FileSystem, symlink_to_dir: Path) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.listdir(symlink_to_dir)

    async def test_listdir_path_with_symlink(
        self, fs: FileSystem, path_with_symlink: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.listdir(path_with_symlink)

    async def test_mkdir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        dir_name = "new"
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

    async def test_mkdir_existing_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        dir_name = "new"
        path = tmp_dir_path / dir_name
        await fs.mkdir(path)
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

        # should not fail
        await fs.mkdir(path)

    async def test_mkdir_existing_file(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        dir_name = "new"
        path = tmp_dir_path / dir_name
        async with fs.open(path, "wb"):
            pass
        files = await fs.listdir(tmp_dir_path)
        assert files == [path]

        with pytest.raises(FileExistsError):
            await fs.mkdir(path)

    async def test_mkdir_symlink(self, fs: FileSystem, symlink_to_dir: Path) -> None:
        with pytest.raises(FileExistsError):
            await fs.mkdir(symlink_to_dir)

    async def test_mkdir_free_symlink(self, fs: FileSystem, free_symlink: Path) -> None:
        with pytest.raises(FileExistsError):
            await fs.mkdir(free_symlink)

    async def test_mkdir_path_with_symlink(
        self, fs: FileSystem, path_with_symlink: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.mkdir(path_with_symlink / "new")

    async def test_liststatus_with_single_empty_file(
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

    async def test_liststatus_with_single_file(
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

    async def test_liststatus_with_single_dir(
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

    async def test_liststatus_with_many_files(
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

    async def test_iterstatus_with_many_files(
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

    async def test_liststatus_non_existent_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"

        with pytest.raises(FileNotFoundError):
            await fs.liststatus(path)

    async def test_liststatus_empty_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    async def test_liststatus_symlink(
        self, fs: FileSystem, symlink_to_dir: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.liststatus(symlink_to_dir)

    async def test_liststatus_free_symlink(
        self, fs: FileSystem, free_symlink: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.liststatus(free_symlink)

    async def test_liststatus_path_with_symlink(
        self, fs: FileSystem, path_with_symlink: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.liststatus(path_with_symlink)

    async def test_iterstatus_non_existent_dir(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"

        async with fs.iterstatus(path) as it:
            with pytest.raises(FileNotFoundError):
                await it.__anext__()

    async def test_iterstatus_symlink(
        self, fs: FileSystem, symlink_to_dir: Path
    ) -> None:
        async with fs.iterstatus(symlink_to_dir) as it:
            with pytest.raises(FileNotFoundError):
                await it.__anext__()

    async def test_iterstatus_free_symlink(
        self, fs: FileSystem, free_symlink: Path
    ) -> None:
        async with fs.iterstatus(free_symlink) as it:
            with pytest.raises(FileNotFoundError):
                await it.__anext__()

    async def test_iterstatus_broken_directory_link(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"
        os.symlink("nonexisting", path, target_is_directory=True)

        async with fs.iterstatus(path) as it:
            with pytest.raises(FileNotFoundError):
                await it.__anext__()

    async def test_iterstatus_with_broken_entry_link(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        path = tmp_dir_path / "nested"
        os.symlink("nonexisting", path)

        async with fs.iterstatus(tmp_dir_path) as it:
            statuses = [status async for status in it]
        actual = [status.path for status in statuses]
        assert actual == [PurePath("nested")]
        assert statuses[0] == FileStatus(
            PurePath("nested"),
            type=FileStatusType.SYMLINK,
            size=1,
            modification_time=mock.ANY,
            target="nonexisting",
        )

    @pytest.mark.parametrize("recursive", [True, False])
    @pytest.mark.parametrize("remove_method", [remove_normal, remove_iter])
    async def test_rm_non_existent(
        self,
        fs: FileSystem,
        tmp_dir_path: Path,
        remove_method: RemoveMethod,
        recursive: bool,
    ) -> None:
        path = tmp_dir_path / "nested"
        with pytest.raises(FileNotFoundError):
            await remove_method(fs, path, recursive)

    @pytest.mark.parametrize("recursive", [True, False])
    @pytest.mark.parametrize("remove_method", [remove_normal, remove_iter])
    async def test_rm_path_with_symlink(
        self,
        fs: FileSystem,
        path_with_symlink: Path,
        remove_method: RemoveMethod,
        recursive: bool,
    ) -> None:
        path = path_with_symlink / "file"
        path.write_bytes(b"")

        with pytest.raises(FileNotFoundError):
            await remove_method(fs, path, recursive)

    @pytest.mark.parametrize("remove_method", [remove_normal, remove_iter])
    async def test_rm_empty_dir(
        self, fs: FileSystem, tmp_dir_path: Path, remove_method: RemoveMethod
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

        await remove_method(fs, path, True)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.parametrize("remove_method", [remove_normal, remove_iter])
    async def test_rm_dir(
        self, fs: FileSystem, tmp_dir_path: Path, remove_method: RemoveMethod
    ) -> None:
        expected_path = Path("nested")
        dir_path = tmp_dir_path / expected_path
        await fs.mkdir(dir_path)

        subdir_path = dir_path / "subdir"
        subdir_path.mkdir()

        file_path = dir_path / "file"
        async with fs.open(file_path, mode="wb") as f:
            await f.write(b"test")
            await f.flush()

        statuses = await fs.liststatus(dir_path)
        assert len(statuses) == 2

        await remove_method(fs, dir_path, True)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.parametrize("remove_method", [remove_normal, remove_iter])
    async def test_rm_dir_non_recursive_fails(
        self, fs: FileSystem, tmp_dir_path: Path, remove_method: RemoveMethod
    ) -> None:
        dir_path = tmp_dir_path / "nested"
        await fs.mkdir(dir_path)

        with pytest.raises(IsADirectoryError):
            await remove_method(fs, dir_path, False)

    @pytest.mark.parametrize("remove_method", [remove_normal, remove_iter])
    @pytest.mark.parametrize("recursive", [True, False])
    async def test_rm_file(
        self,
        fs: FileSystem,
        tmp_dir_path: Path,
        recursive: bool,
        remove_method: RemoveMethod,
    ) -> None:
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

        await remove_method(fs, path, recursive)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == []

    @pytest.mark.parametrize("recursive", [True, False])
    @pytest.mark.parametrize("remove_method", [remove_normal, remove_iter])
    async def test_rm_symlink_to_dir(
        self,
        fs: FileSystem,
        tmp_dir_path: Path,
        remove_method: RemoveMethod,
        recursive: bool,
    ) -> None:
        dir_path = tmp_dir_path / "dir"
        await fs.mkdir(dir_path)
        link_path = tmp_dir_path / "link"
        link_path.symlink_to("dir", target_is_directory=True)

        stat = os.stat(dir_path)
        expected_mtime = int(stat.st_mtime)
        lstat = os.lstat(link_path)
        expected_link_mtime = int(lstat.st_mtime)

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
                size=1,
                type=FileStatusType.SYMLINK,
                modification_time=expected_link_mtime,
                target="dir",
            ),
        ]

        await remove_method(fs, link_path, recursive)

        statuses = await fs.liststatus(tmp_dir_path)
        assert statuses == [
            FileStatus(
                Path("dir"),
                size=0,
                type=FileStatusType.DIRECTORY,
                modification_time=expected_mtime,
            )
        ]

    async def test_iterremove_with_many_files(
        self, fs: FileSystem, tmp_dir_path: Path
    ) -> None:
        expected = []

        async def make_files(path: PurePath, count: int) -> None:
            for i in range(count):
                name = f"file-{i}"
                filepath = path / name
                expected.append((filepath, False))
                async with fs.open(filepath, "wb"):
                    pass

        to_remove_dir = tmp_dir_path / "to_remove"
        to_remove_dir.mkdir()
        expected.append((to_remove_dir, True))

        for subdir_segments in (("foo",), ("bar",), ("foo", "baz")):
            subdir = to_remove_dir.joinpath(*subdir_segments)
            subdir.mkdir()
            expected.append((subdir, True))
            await make_files(subdir, 100)

        actual = [
            (remove_listing.path, remove_listing.is_dir)
            async for remove_listing in fs.iterremove(to_remove_dir, recursive=True)
        ]
        assert sorted(actual) == sorted(expected)

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

    async def test_get_filestatus_symlink_to_file(
        self, fs: FileSystem, symlink_to_file: Path
    ) -> None:
        stat = os.lstat(symlink_to_file)
        expected_mtime = int(stat.st_mtime)

        status = await fs.get_filestatus(symlink_to_file)
        assert status == FileStatus(
            path=symlink_to_file,
            size=1,
            type=FileStatusType.SYMLINK,
            modification_time=expected_mtime,
            target="file",
        )

    async def test_get_filestatus_symlink_to_dir(
        self, fs: FileSystem, symlink_to_dir: Path
    ) -> None:
        stat = os.lstat(symlink_to_dir)
        expected_mtime = int(stat.st_mtime)

        status = await fs.get_filestatus(symlink_to_dir)
        assert status == FileStatus(
            path=symlink_to_dir,
            size=1,
            type=FileStatusType.SYMLINK,
            modification_time=expected_mtime,
            target="dir",
        )

    async def test_get_filestatus_free_symlink(
        self, fs: FileSystem, free_symlink: Path
    ) -> None:
        stat = os.lstat(free_symlink)
        expected_mtime = int(stat.st_mtime)

        status = await fs.get_filestatus(free_symlink)
        assert status == FileStatus(
            path=free_symlink,
            size=1,
            type=FileStatusType.SYMLINK,
            modification_time=expected_mtime,
            target="nonexistent",
        )

    async def test_get_filestatus_path_with_symlink(
        self, fs: FileSystem, path_with_symlink: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.get_filestatus(path_with_symlink)

    async def test_exists_file(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        file_relative = Path("nested")
        expected_file_path = tmp_dir_path / file_relative

        payload = b"test"
        async with fs.open(expected_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        assert await fs.exists(expected_file_path)
        await fs.remove(expected_file_path)

    async def test_exists_dir(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        file_relative = Path("nested")
        expected_file_path = tmp_dir_path / file_relative

        payload = b"test"
        async with fs.open(expected_file_path, mode="wb") as f:
            await f.write(payload)
            await f.flush()

        assert await fs.exists(tmp_dir_path)
        await fs.remove(expected_file_path)

    async def test_exists_unknown(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        assert not await fs.exists(Path("unknown-name"))

    # helper methods for working with sets of statuses
    @classmethod
    def statuses_get(cls, statuses: Iterable[FileStatus], path: PurePath) -> FileStatus:
        return next(st for st in statuses if st.path == path)

    @classmethod
    def statuses_add(
        cls, statuses: set[FileStatus], status: FileStatus
    ) -> set[FileStatus]:
        return statuses | {status}

    @classmethod
    def statuses_drop(
        cls, statuses: Iterable[FileStatus], path: PurePath
    ) -> set[FileStatus]:
        return set(filter(lambda st: st.path != path, statuses))

    @classmethod
    def statuses_rename(
        cls, statuses: Iterable[FileStatus], old_path: PurePath, new_path: PurePath
    ) -> set[FileStatus]:
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

    async def test_rename_symlink_to_file(
        self, fs: FileSystem, tmp_dir_path: Path, symlink_to_file: Path
    ) -> None:
        old_path = symlink_to_file
        new_path = tmp_dir_path / "new"

        await fs.rename(old_path, new_path)
        assert not old_path.exists()
        assert new_path.is_symlink()
        assert new_path.read_bytes() == b"test"
        assert set(tmp_dir_path.iterdir()) == {new_path, new_path.resolve()}

    async def test_rename_file_to_symlink_to_file(
        self, fs: FileSystem, tmp_dir_path: Path, symlink_to_file: Path
    ) -> None:
        old_path = tmp_dir_path / "old"
        old_path.write_bytes(b"test")
        new_path = symlink_to_file
        orig_path = symlink_to_file.resolve()

        await fs.rename(old_path, new_path)
        assert not old_path.exists()
        assert not new_path.is_symlink()
        assert new_path.read_bytes() == b"test"
        assert set(tmp_dir_path.iterdir()) == {new_path, orig_path}

    async def test_rename_file_to_symlink_to_dir(
        self, fs: FileSystem, tmp_dir_path: Path, symlink_to_dir: Path
    ) -> None:
        old_path = tmp_dir_path / "old"
        old_path.write_bytes(b"test")
        new_path = symlink_to_dir
        orig_path = symlink_to_dir.resolve()

        await fs.rename(old_path, new_path)
        assert not old_path.exists()
        assert not new_path.is_symlink()
        assert new_path.read_bytes() == b"test"
        assert set(tmp_dir_path.iterdir()) == {new_path, orig_path}

    async def test_rename_file_to_free_symlink(
        self, fs: FileSystem, tmp_dir_path: Path, free_symlink: Path
    ) -> None:
        old_path = tmp_dir_path / "old"
        old_path.write_bytes(b"test")
        new_path = free_symlink

        await fs.rename(old_path, new_path)
        assert not old_path.exists()
        assert not new_path.is_symlink()
        assert new_path.read_bytes() == b"test"
        assert list(tmp_dir_path.iterdir()) == [new_path]

    async def test_rename_file_src_path_with_symlink(
        self, fs: FileSystem, tmp_dir_path: Path, path_with_symlink: Path
    ) -> None:
        old_path = path_with_symlink / "old"
        new_path = tmp_dir_path / "new"
        old_path.write_bytes(b"test")

        with pytest.raises(FileNotFoundError):
            await fs.rename(old_path, new_path)

    async def test_rename_file_dst_path_with_symlink(
        self, fs: FileSystem, tmp_dir_path: Path, path_with_symlink: Path
    ) -> None:
        old_path = tmp_dir_path / "old"
        new_path = path_with_symlink / "new"
        old_path.write_bytes(b"test")

        with pytest.raises(FileNotFoundError):
            await fs.rename(old_path, new_path)

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

    async def test_disk_usage(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        res = await fs.disk_usage(tmp_dir_path)
        total, used, free = shutil.disk_usage(tmp_dir_path)
        assert res.total == total
        assert res.used == used
        assert res.free == free

    async def test_disk_usage_symlink_to_file(
        self, fs: FileSystem, symlink_to_file: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.disk_usage(symlink_to_file)

    async def test_disk_usage_free_symlink(
        self, fs: FileSystem, free_symlink: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.disk_usage(free_symlink)

    async def test_disk_usage_path_with_symlink(
        self, fs: FileSystem, path_with_symlink: Path
    ) -> None:
        with pytest.raises(FileNotFoundError):
            await fs.disk_usage(path_with_symlink)

    async def test_disk_usage_by_file(self, fs: FileSystem, tmp_dir_path: Path) -> None:
        file1 = tmp_dir_path / "test1"
        file1.write_text("test1")

        file2 = tmp_dir_path / "test2"
        file2.write_text("test2")

        result = await fs.disk_usage_by_file(file1, file2)

        assert result == [
            FileUsage(path=file1, size=mock.ANY),
            FileUsage(path=file2, size=mock.ANY),
        ]
        assert result[0].size
        assert result[1].size
