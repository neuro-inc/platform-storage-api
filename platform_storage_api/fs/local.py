import abc
import asyncio
import contextlib
import enum
import errno
import logging
import os
import shutil
import stat as statmodule
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, replace
from itertools import islice
from pathlib import Path, PurePath
from types import TracebackType
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterator,
    Iterable,
    Iterator,
    List,
    Optional,
    Type,
    TypeVar,
    Union,
)

import aiofiles


SCANDIR_CHUNK_SIZE = 100

logger = logging.getLogger()


# TODO (A Danshyn 04/23/18): likely should be revisited
class StorageType(str, enum.Enum):
    LOCAL = "local"
    S3 = "s3"


class FileStatusType(str, enum.Enum):
    DIRECTORY = "DIRECTORY"
    FILE = "FILE"

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

    def with_permission(self, permission: FileStatusPermission) -> "FileStatus":
        return replace(self, permission=permission)


@dataclass(frozen=True)
class RemoveListing:
    path: PurePath
    is_dir: bool


@dataclass(frozen=True)
class DiskUsageInfo:
    total: int
    used: int
    free: int


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
        exc_type: Optional[Type[BaseException]],
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
    async def listdir(self, path: PurePath) -> List[PurePath]:
        pass

    @abc.abstractmethod
    async def mkdir(self, path: PurePath) -> None:
        pass

    @abc.abstractmethod
    def iterstatus(
        self, path: PurePath
    ) -> AsyncContextManager[AsyncIterator[FileStatus]]:
        pass

    async def liststatus(self, path: PurePath) -> List[FileStatus]:
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
    async def disk_usage(self, path: PurePath) -> DiskUsageInfo:
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
                fd = os.open(name, os.O_RDONLY, dir_fd=dirfd)
                try:
                    if not os.path.samestat(orig_st, os.stat(fd)):
                        raise FileNotFoundError(
                            errno.ENOENT, "No such file or directory", str(path)
                        )
                    if dirfd is not None:
                        os.close(dirfd)
                except:  # noqa: E722
                    os.close(fd)
                    raise
                dirfd = fd
            assert dirfd is not None
            yield dirfd
        finally:
            if dirfd is not None:
                os.close(dirfd)

    def _listdir(self, path: PurePath) -> List[PurePath]:
        with self._resolve_dir_fd(path) as dirfd:
            with os.scandir(dirfd) as scandir_it:
                entries = list(scandir_it)
            return [path / entry.name for entry in entries]

    async def listdir(self, path: PurePath) -> List[PurePath]:
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
                    fd = os.open(name, flags, dir_fd=dirfd)
                    try:
                        if not os.path.samestat(orig_st, os.stat(fd)):
                            raise FileNotFoundError(
                                errno.ENOENT, "No such file or directory", str(path)
                            )
                    except:  # noqa: E722
                        os.close(fd)
                        raise
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
            for name in path.parts:
                try:
                    orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                except FileNotFoundError:
                    os.mkdir(name, dir_fd=dirfd)
                    orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                fd = os.open(name, os.O_RDONLY, dir_fd=dirfd)
                try:
                    if not os.path.samestat(orig_st, os.stat(fd)):
                        raise NotADirectoryError(
                            errno.ENOTDIR, "Not a directory", str(path)
                        )
                    if dirfd is not None:
                        os.close(dirfd)
                except:  # noqa: E722
                    os.close(fd)
                    raise
                dirfd = fd
            if not statmodule.S_ISDIR(orig_st.st_mode):
                raise FileExistsError(errno.EEXIST, "File exists", str(path))
        finally:
            if dirfd is not None:
                os.close(dirfd)

    async def mkdir(self, path: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._mkdir, path)

    @classmethod
    def _create_filestatus(
        cls, path: PurePath, stat: Any, is_dir: bool
    ) -> "FileStatus":
        mod_time = int(stat.st_mtime)  # converting float to int
        if is_dir:
            return FileStatus.create_dir_status(path, modification_time=mod_time)
        else:
            size = stat.st_size
            return FileStatus.create_file_status(path, size, modification_time=mod_time)

    async def _iterate_in_chunks(
        self, it: Iterator[Any], chunk_size: int
    ) -> AsyncIterator[List[Any]]:
        done = False
        while not done:
            chunk = await self._loop.run_in_executor(
                self._executor, list, islice(it, 0, chunk_size)
            )
            if not chunk:
                break
            done = len(chunk) < chunk_size
            yield chunk

    def _scandir_iter(self, path: PurePath) -> Iterator[FileStatus]:
        with self._resolve_dir_fd(path) as dirfd:
            with os.scandir(dirfd) as scandir_it:
                for entry in scandir_it:
                    yield self._create_filestatus(
                        PurePath(entry.name),
                        entry.stat(),
                        entry.is_dir(),
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
                self._executor, dir_iter.close  # type: ignore
            )

    def _get_file_or_dir_status(self, path: PurePath) -> FileStatus:
        with self._resolve_dir_fd(path.parent) as dirfd:
            name = path.name
            assert name not in ("..", ".", "")
            orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
            if statmodule.S_ISLNK(orig_st.st_mode):
                raise FileNotFoundError(
                    errno.ENOENT, "No such file or directory", str(path)
                )
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
        self, dirfd: int, path: PurePath
    ) -> Iterator[RemoveListing]:
        with os.scandir(dirfd) as scandir_it:
            entries = list(scandir_it)

        for entry in entries:
            name = entry.name
            entry_path = path / name
            is_dir = entry.is_dir()
            if is_dir:
                orig_st = os.stat(name, dir_fd=dirfd, follow_symlinks=False)
                if statmodule.S_ISDIR(orig_st.st_mode):
                    fd = os.open(name, os.O_RDONLY, dir_fd=dirfd)
                    try:
                        if not os.path.samestat(orig_st, os.stat(fd)):
                            raise NotADirectoryError(
                                errno.ENOTDIR, "Not a directory", str(path)
                            )
                        yield from self._iterremovechildren(fd, entry_path)
                    finally:
                        os.close(fd)
                    os.rmdir(name, dir_fd=dirfd)
                    yield RemoveListing(entry_path, is_dir=True)
                continue
            os.unlink(name, dir_fd=dirfd)
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
            fd = os.open(name, os.O_RDONLY, dir_fd=dirfd)
            try:
                if not os.path.samestat(orig_st, os.stat(fd)):
                    os.unlink(name, dir_fd=dirfd)
                    yield RemoveListing(path, is_dir=False)
                    return

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
            fd = os.open(name, os.O_RDONLY, dir_fd=dirfd)
            try:
                if not os.path.samestat(orig_st, os.stat(fd)):
                    raise NotADirectoryError(
                        errno.ENOTDIR, "Not a directory", str(path)
                    )

                def onerror(*args: Any) -> None:
                    raise

                try:
                    shutil._rmtree_safe_fd(fd, "", onerror)  # type: ignore
                    os.rmdir(name, dir_fd=dirfd)
                except OSError as e:
                    self._log_remove_error(e, path)
                    raise e
            finally:
                os.close(fd)

    async def remove(self, path: PurePath, *, recursive: bool = False) -> None:
        await self._loop.run_in_executor(self._executor, self._remove, path, recursive)

    def _rename(self, old: PurePath, new: PurePath) -> None:
        with self._resolve_dir_fd(old.parent) as src_dir_fd:
            with self._resolve_dir_fd(new.parent) as dst_dir_fd:
                os.rename(
                    old.name, new.name, src_dir_fd=src_dir_fd, dst_dir_fd=dst_dir_fd
                )

    async def rename(self, old: PurePath, new: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._rename, old, new)

    def _disk_usage(self, path: PurePath) -> DiskUsageInfo:
        with self._resolve_dir_fd(path) as fd:
            total, used, free = shutil.disk_usage(fd)  # type: ignore
            return DiskUsageInfo(
                total=total,
                used=used,
                free=free,
            )

    async def disk_usage(self, path: PurePath) -> DiskUsageInfo:
        return await self._loop.run_in_executor(self._executor, self._disk_usage, path)


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

    async def put_to_queue(chunk: List[Union[_T, EndMark, Exception]]) -> None:
        for item in chunk:
            await queue.put(item)

    def sync_runner() -> None:
        chunk: List[Union[_T, EndMark, Exception]] = []
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
