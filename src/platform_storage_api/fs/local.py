import abc
import asyncio
import contextlib
import enum
import errno
import logging
import os
import shutil
import stat as statmodule
from collections.abc import AsyncIterator, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, replace
from itertools import islice
from pathlib import Path, PurePath
from types import TracebackType
from typing import Any, Optional, TypeVar, Union

import aiofiles

SCANDIR_CHUNK_SIZE = 100

logger = logging.getLogger()


class FileSystemException(Exception):
    pass


# TODO (A Danshyn 04/23/18): likely should be revisited
class StorageType(str, enum.Enum):
    LOCAL = "local"
    S3 = "s3"


class FileStatusType(str, enum.Enum):
    DIRECTORY = "DIRECTORY"
    FILE = "FILE"
    SYMLINK = "SYMLINK"

    def __str__(self) -> str:
        return self.value


class FileStatusPermission(str, enum.Enum):
    READ = "read"
    WRITE = "write"
    MANAGE = "manage"


@dataclass(frozen=True)
class FileStatus:
    path: PurePath
    type: FileStatusType
    size: int
    modification_time: Optional[int] = None
    permission: FileStatusPermission = FileStatusPermission.READ
    target: Optional[str] = None

    @classmethod
    def create_file_status(
        cls, path: PurePath, size: int, modification_time: Optional[int] = None
    ) -> "FileStatus":
        return cls(
            path=path,
            type=FileStatusType.FILE,
            size=size,
            modification_time=modification_time,
        )

    @classmethod
    def create_dir_status(
        cls, path: PurePath, modification_time: Optional[int] = None
    ) -> "FileStatus":
        return cls(
            path=path,
            type=FileStatusType.DIRECTORY,
            size=0,
            modification_time=modification_time,
        )

    @classmethod
    def create_link_status(
        cls,
        path: PurePath,
        modification_time: Optional[int] = None,
        target: Optional[str] = None,
    ) -> "FileStatus":
        return cls(
            path=path,
            type=FileStatusType.SYMLINK,
            size=1,
            modification_time=modification_time,
            target=target,
        )

    def with_permission(self, permission: FileStatusPermission) -> "FileStatus":
        return replace(self, permission=permission)


@dataclass(frozen=True)
class RemoveListing:
    path: PurePath
    is_dir: bool


@dataclass(frozen=True)
class DiskUsage:
    total: int
    used: int
    free: int


@dataclass(frozen=True)
class FileUsage:
    path: PurePath
    size: int


class FileSystem(AbstractAsyncContextManager):  # type: ignore
    @classmethod
    def create(cls, type_: StorageType, *args: Any, **kwargs: Any) -> "FileSystem":
        if type_ == StorageType.LOCAL:
            return LocalFileSystem(**kwargs)
        raise ValueError(f"Unsupported storage type: {type_}")

    async def __aenter__(self) -> "FileSystem":
        await self.init()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Optional[bool]:
        await self.close()
        return None

    @abc.abstractmethod
    async def init(self) -> None:
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    # Actual return type is an async version of io.FileIO
    @abc.abstractmethod
    def open(self, path: PurePath, mode: str = "r") -> Any:
        pass

    @abc.abstractmethod
    async def listdir(self, path: PurePath) -> list[PurePath]:
        pass

    @abc.abstractmethod
    async def mkdir(self, path: PurePath) -> None:
        pass

    @abc.abstractmethod
    def iterstatus(
        self, path: PurePath
    ) -> AbstractAsyncContextManager[AsyncIterator[FileStatus]]:
        pass

    async def liststatus(self, path: PurePath) -> list[FileStatus]:
        # TODO (A Danshyn 05/03/18): the listing size is disregarded for now
        async with self.iterstatus(path) as dir_iter:
            return [status async for status in dir_iter]

    @abc.abstractmethod
    async def get_filestatus(self, path: PurePath) -> FileStatus:
        pass

    @abc.abstractmethod
    async def exists(self, path: PurePath) -> bool:
        pass

    @abc.abstractmethod
    async def remove(self, path: PurePath, *, recursive: bool = False) -> None:
        pass

    @abc.abstractmethod
    def iterremove(
        self, path: PurePath, *, recursive: bool = False
    ) -> AsyncIterator[RemoveListing]:
        pass

    @abc.abstractmethod
    async def rename(self, old: PurePath, new: PurePath) -> None:
        pass

    @abc.abstractmethod
    async def disk_usage(self, path: PurePath) -> DiskUsage:
        pass

    @abc.abstractmethod
    async def disk_usage_by_file(self, *paths: PurePath) -> list[FileUsage]:
        pass


class LocalFileSystem(FileSystem):
    def __init__(
        self,
        *,
        executor_max_workers: Optional[int] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
        **kwargs: Any,
    ) -> None:
        self._executor_max_workers = executor_max_workers
        self._executor: Optional[ThreadPoolExecutor] = None

        self._loop = loop or asyncio.get_event_loop()

    async def init(self) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=self._executor_max_workers,
            thread_name_prefix="LocalFileSystemThread",
        )
        logger.info(
            "Initialized LocalFileSystem with a thread pool of size %s",
            self._executor_max_workers,
        )

    async def close(self) -> None:
        if self._executor:
            self._executor.shutdown()

    def _open_safe_fd(
        self,
        name: str,
        dirfd: Optional[int],
        orig_st: os.stat_result,
        flags: int = os.O_RDONLY,
    ) -> int:
        fd = os.open(name, flags, dir_fd=dirfd)
        try:
            if not os.path.samestat(orig_st, os.stat(fd)):
                raise FileNotFoundError(errno.ENOENT, "No such file or directory")
        except:  # noqa: E722
            os.close(fd)
            raise
        return fd

    @contextlib.contextmanager
    def _resolve_dir_fd(
        self, path: PurePath, dirfd: Optional[int] = None
    ) -> Iterator[int]:
        try:
            for name in path.parts:
                assert name not in ("..", ".", "")
                orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                if statmodule.S_ISLNK(orig_st.st_mode):
                    raise FileNotFoundError(
                        errno.ENOENT, "No such file or directory", str(path)
                    )
                oldfd = dirfd
                dirfd = self._open_safe_fd(name, dirfd, orig_st)
                if oldfd is not None:
                    os.close(oldfd)
            assert dirfd is not None
            yield dirfd
        finally:
            if dirfd is not None:
                os.close(dirfd)

    def _listdir(self, path: PurePath) -> list[PurePath]:
        with self._resolve_dir_fd(path) as dirfd:
            with os.scandir(dirfd) as scandir_it:
                entries = list(scandir_it)
            return [path / entry.name for entry in entries]

    async def listdir(self, path: PurePath) -> list[PurePath]:
        return await self._loop.run_in_executor(self._executor, self._listdir, path)

    # Actual return type is an async version of io.FileIO
    @contextlib.asynccontextmanager
    async def open(self, path: PurePath, mode: str = "r") -> Any:
        def opener(filepath: str, flags: int) -> int:
            # Possible flags values:
            # "rb": os.O_RDONLY
            # "rb+": os.O_RDWR
            # "wb": os.O_CREAT | os.O_TRUNC | os.O_WRONLY
            # "xb": os.O_EXCL | os.O_CREAT | os.O_WRONLY
            with self._resolve_dir_fd(path.parent) as dirfd:
                name = path.name
                assert name not in ("..", ".", "")
                try:
                    orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                except FileNotFoundError:
                    if flags & os.O_CREAT == 0:
                        raise
                    flags &= ~os.O_TRUNC
                    flags |= os.O_EXCL
                    fd = os.open(name, flags, dir_fd=dirfd)
                else:
                    if statmodule.S_ISLNK(orig_st.st_mode):
                        raise FileNotFoundError(
                            errno.ENOENT, "No such file or directory", str(path)
                        )
                    fd = self._open_safe_fd(name, dirfd, orig_st, flags=flags)
                return fd

        async with aiofiles.open(
            path,
            mode=mode,
            opener=opener,
            executor=self._executor,
        ) as f:
            yield f

    def _mkdir(self, path: PurePath) -> None:
        # TODO (A Danshyn 04/23/18): consider setting mode
        dirfd = None
        try:
            for name in path.parts[:-1]:
                assert name not in ("..", ".", "")
                try:
                    orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                except FileNotFoundError:
                    os.mkdir(name, dir_fd=dirfd)
                    orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                oldfd = dirfd
                dirfd = self._open_safe_fd(name, dirfd, orig_st)
                if oldfd is not None:
                    os.close(oldfd)

            name = path.name
            assert name not in ("..", ".", "")
            try:
                os.mkdir(name, dir_fd=dirfd)
            except OSError:
                orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                if not statmodule.S_ISDIR(orig_st.st_mode):
                    raise
        finally:
            if dirfd is not None:
                os.close(dirfd)

    async def mkdir(self, path: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._mkdir, path)

    @classmethod
    def _create_filestatus(
        cls, path: PurePath, stat: os.stat_result, is_dir: bool
    ) -> "FileStatus":
        mod_time = int(stat.st_mtime)  # converting float to int
        if is_dir:
            return FileStatus.create_dir_status(path, modification_time=mod_time)
        else:
            size = stat.st_size
            return FileStatus.create_file_status(path, size, modification_time=mod_time)

    def _create_linkstatus(
        cls,
        path: PurePath,
        stat: os.stat_result,
        target: Optional[str] = None,
    ) -> "FileStatus":
        mod_time = int(stat.st_mtime)  # converting float to int
        return FileStatus.create_link_status(
            path, modification_time=mod_time, target=target
        )

    async def _iterate_in_chunks(
        self, it: Iterator[Any], chunk_size: int
    ) -> AsyncIterator[list[Any]]:
        done = False
        while not done:
            chunk: list[Any] = await self._loop.run_in_executor(
                self._executor, list, islice(it, 0, chunk_size)
            )
            if not chunk:
                break
            done = len(chunk) < chunk_size
            yield chunk

    def _scandir_iter(self, path: PurePath) -> Iterator[FileStatus]:
        with self._resolve_dir_fd(path) as dirfd, os.scandir(dirfd) as scandir_it:
            for entry in scandir_it:
                if entry.is_symlink():
                    target = os.readlink(entry.name, dir_fd=dirfd)
                    yield self._create_linkstatus(
                        PurePath(entry.name),
                        entry.stat(follow_symlinks=False),
                        target=target,
                    )
                else:
                    yield self._create_filestatus(
                        PurePath(entry.name),
                        entry.stat(follow_symlinks=False),
                        entry.is_dir(follow_symlinks=False),
                    )

    async def _iterstatus_iter(
        self, dir_iter: Iterator[FileStatus]
    ) -> AsyncIterator[FileStatus]:
        async for chunk in self._iterate_in_chunks(dir_iter, SCANDIR_CHUNK_SIZE):
            for status in chunk:
                yield status

    @asynccontextmanager
    async def iterstatus(
        self, path: PurePath
    ) -> AsyncIterator[AsyncIterator[FileStatus]]:
        dir_iter = self._scandir_iter(path)
        try:
            yield self._iterstatus_iter(dir_iter)
        finally:
            await self._loop.run_in_executor(
                self._executor,
                dir_iter.close,  # type: ignore
            )

    def _get_file_or_dir_status(self, path: PurePath) -> FileStatus:
        with self._resolve_dir_fd(path.parent) as dirfd:
            name = path.name
            assert name not in ("..", ".", "")
            orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
            if statmodule.S_ISLNK(orig_st.st_mode):
                target = os.readlink(name, dir_fd=dirfd)
                return self._create_linkstatus(path, orig_st, target)
            else:
                return self._create_filestatus(
                    path, orig_st, statmodule.S_ISDIR(orig_st.st_mode)
                )

    async def get_filestatus(self, path: PurePath) -> FileStatus:
        return await self._loop.run_in_executor(
            self._executor, self._get_file_or_dir_status, path
        )

    async def exists(self, path: PurePath) -> bool:
        return await self._loop.run_in_executor(self._executor, Path(path).exists)

    def _remove_as_dir(self, path: PurePath) -> bool:
        real_path = Path(path)
        return not real_path.is_symlink() and real_path.is_dir()

    def _log_remove_error(self, e: OSError, failed_path: PurePath) -> None:
        if e.filename:
            failed_path = e.filename
        path_access_ok = os.access(failed_path, os.W_OK)
        try:
            path_mode = f"{os.stat(failed_path).st_mode:03o}"
        except OSError:
            path_mode = "?"

        parent_path = os.path.dirname(failed_path)
        parent_path_access_ok = os.access(parent_path, os.W_OK)
        try:
            parent_path_mode = f"{os.stat(parent_path).st_mode:03o}"
        except OSError:
            parent_path_mode = "?"

        logger.warning(
            "OSError for path = %s, path_mode = %s, access = %s, "
            "parent_path_mode = %s, parent_access = %s, "
            "error_message = %s, errno = %s",
            failed_path,
            path_mode,
            path_access_ok,
            parent_path_mode,
            parent_path_access_ok,
            str(e),
            errno.errorcode.get(e.errno, e.errno),
        )

    def _iterremovechildren(
        self, topfd: int, path: PurePath
    ) -> Iterator[RemoveListing]:
        with os.scandir(topfd) as scandir_it:
            entries = list(scandir_it)

        for entry in entries:
            name = entry.name
            entry_path = path / name
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError:
                is_dir = False
            else:
                if is_dir:
                    orig_st = entry.stat(follow_symlinks=False)
                    is_dir = statmodule.S_ISDIR(orig_st.st_mode)
            if is_dir:
                dirfd = self._open_safe_fd(name, topfd, orig_st)
                try:
                    yield from self._iterremovechildren(dirfd, entry_path)
                    os.rmdir(name, dir_fd=topfd)
                    yield RemoveListing(entry_path, is_dir=True)
                finally:
                    os.close(dirfd)
            else:
                os.unlink(name, dir_fd=topfd)
                yield RemoveListing(entry_path, is_dir=False)

    def _iterremove(
        self, path: PurePath, *, recursive: bool = False
    ) -> Iterator[RemoveListing]:
        with self._resolve_dir_fd(path.parent) as dirfd:
            name = path.name
            assert name not in ("..", ".", "")
            orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
            if not statmodule.S_ISDIR(orig_st.st_mode):
                os.unlink(name, dir_fd=dirfd)
                yield RemoveListing(path, is_dir=False)
                return

            if not recursive:
                raise IsADirectoryError(
                    errno.EISDIR, "Is a directory, use recursive remove", str(path)
                )
            fd = self._open_safe_fd(name, dirfd, orig_st)
            try:
                yield from self._iterremovechildren(fd, path)
                os.rmdir(name, dir_fd=dirfd)
                yield RemoveListing(path, is_dir=True)
            except OSError as e:
                self._log_remove_error(e, path)
                raise e
            finally:
                os.close(fd)

    async def iterremove(
        self, path: PurePath, *, recursive: bool = False
    ) -> AsyncIterator[RemoveListing]:
        async for remove_entry in sync_iterator_to_async(
            self._loop, self._executor, self._iterremove(path, recursive=recursive)
        ):
            yield remove_entry

    def _rmtree_safe_fd(self, topfd: int) -> None:
        with os.scandir(topfd) as scandir_it:
            entries = list(scandir_it)
        for entry in entries:
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError:
                is_dir = False
            else:
                if is_dir:
                    orig_st = entry.stat(follow_symlinks=False)
                    is_dir = statmodule.S_ISDIR(orig_st.st_mode)
            if is_dir:
                dirfd = self._open_safe_fd(entry.name, topfd, orig_st)
                try:
                    self._rmtree_safe_fd(dirfd)
                    os.rmdir(entry.name, dir_fd=topfd)
                finally:
                    os.close(dirfd)
            else:
                os.unlink(entry.name, dir_fd=topfd)

    def _remove(self, path: PurePath, recursive: bool) -> None:
        with self._resolve_dir_fd(path.parent) as dirfd:
            name = path.name
            assert name not in ("..", ".", "")
            orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
            if not statmodule.S_ISDIR(orig_st.st_mode):
                os.unlink(name, dir_fd=dirfd)
                return

            if not recursive:
                raise IsADirectoryError(
                    errno.EISDIR, "Is a directory, use recursive remove", str(path)
                )
            fd = self._open_safe_fd(name, dirfd, orig_st)
            try:
                self._rmtree_safe_fd(fd)
                os.rmdir(name, dir_fd=dirfd)
            except OSError as e:
                self._log_remove_error(e, path)
                raise e
            finally:
                os.close(fd)

    async def remove(self, path: PurePath, *, recursive: bool = False) -> None:
        await self._loop.run_in_executor(self._executor, self._remove, path, recursive)

    def _rename(self, old: PurePath, new: PurePath) -> None:
        with (
            self._resolve_dir_fd(old.parent) as src_dir_fd,
            self._resolve_dir_fd(new.parent) as dst_dir_fd,
        ):
            os.rename(old.name, new.name, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd)

    async def rename(self, old: PurePath, new: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._rename, old, new)

    def _disk_usage(self, path: PurePath) -> DiskUsage:
        with self._resolve_dir_fd(path) as fd:
            total, used, free = shutil.disk_usage(fd)
            return DiskUsage(
                total=total,
                used=used,
                free=free,
            )

    async def disk_usage(self, path: PurePath) -> DiskUsage:
        return await self._loop.run_in_executor(self._executor, self._disk_usage, path)

    async def disk_usage_by_file(self, *paths: PurePath) -> list[FileUsage]:
        async with aiofiles.tempfile.NamedTemporaryFile("w") as temp_file:
            await temp_file.write("\0".join(str(p) for p in paths))
            await temp_file.flush()
            process = await asyncio.subprocess.create_subprocess_exec(
                "du",
                "-sh",
                f"--files0-from={temp_file.name}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
        if process.returncode:
            raise FileSystemException(stderr.decode())
        result = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            size_str, *_, path = line.split()
            result.append(
                FileUsage(
                    path=PurePath(path.decode()),
                    size=parse_du_size_output(size_str.decode()),
                )
            )
        return result


_SIZE_UNIT_POWERS = {
    unit: power
    for power, unit in enumerate(("K", "M", "G", "T", "P", "E", "Z", "Y"), start=1)
}


def parse_du_size_output(size_str: str) -> int:
    if not size_str:
        return 0
    if size_str[-1] == "B":
        if size_str[-2] == "i":
            return int(float(size_str[:-3]) * 1024 ** _SIZE_UNIT_POWERS[size_str[-3]])
        return int(float(size_str[:-2]) * 1000 ** _SIZE_UNIT_POWERS[size_str[-2]])
    if size_str[-1].isdigit():
        return int(float(size_str))
    return int(float(size_str[:-1]) * 1024 ** _SIZE_UNIT_POWERS[size_str[-1]])


DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB


async def copy_streams(
    outstream: Any,
    instream: Any,
    *,
    size: Optional[int] = None,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> None:
    """perform chunked copying of data between two streams.

    It is assumed that stream implementations would handle retries themselves.
    """
    if size is None:
        while True:
            chunk = await outstream.read(chunk_size)
            if not chunk:
                break
            await instream.write(chunk)
    else:
        while size > 0:
            chunk = await outstream.read(min(size, chunk_size))
            if not chunk:
                break
            size -= len(chunk)
            await instream.write(chunk)


_T = TypeVar("_T")


async def sync_iterator_to_async(
    loop: asyncio.AbstractEventLoop,
    executor: Optional[ThreadPoolExecutor],
    iter: Iterable[_T],
    queue_size: int = 5000,
) -> AsyncIterator[_T]:
    class EndMark:
        pass

    chunk_size = max(queue_size / 20, 50)

    queue: asyncio.Queue[Union[_T, EndMark, Exception]] = asyncio.Queue(queue_size)

    async def put_to_queue(chunk: list[Union[_T, EndMark, Exception]]) -> None:
        for item in chunk:
            await queue.put(item)

    def sync_runner() -> None:
        chunk: list[Union[_T, EndMark, Exception]] = []
        try:
            try:
                for item in iter:
                    chunk.append(item)
                    if len(chunk) == chunk_size:
                        data = chunk
                        chunk = []
                        asyncio.run_coroutine_threadsafe(
                            put_to_queue(data), loop
                        ).done()
            except Exception as e:
                chunk.append(e)
                return
            chunk.append(EndMark())
        finally:
            asyncio.run_coroutine_threadsafe(put_to_queue(chunk), loop).done()

    loop.run_in_executor(executor, sync_runner)

    while True:
        item = await queue.get()
        if isinstance(item, EndMark):
            return
        if isinstance(item, Exception):
            raise item
        yield item
