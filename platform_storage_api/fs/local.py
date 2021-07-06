import abc
import asyncio
import contextlib
import enum
import errno
import logging
import os
import shutil
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

    def _listdir(self, path: PurePath) -> List[PurePath]:
        path = Path(path)
        return list(path.iterdir())

    async def listdir(self, path: PurePath) -> List[PurePath]:
        return await self._loop.run_in_executor(self._executor, self._listdir, path)

    # Actual return type is an async version of io.FileIO
    @contextlib.asynccontextmanager
    async def open(self, path: PurePath, mode: str = "r") -> Any:
        async with aiofiles.open(
            path,
            mode=mode,
            executor=self._executor,
        ) as f:
            yield f

    def _mkdir(self, path: PurePath) -> None:
        # TODO (A Danshyn 04/23/18): consider setting mode
        Path(path).mkdir(parents=True, exist_ok=True)

    async def mkdir(self, path: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._mkdir, path)

    @classmethod
    def _create_filestatus(
        cls, path: PurePath, basename_only: Optional[bool] = False
    ) -> "FileStatus":
        with Path(path) as real_path:
            stat = real_path.stat()
            mod_time = int(stat.st_mtime)  # converting float to int
            path = PurePath(path.name) if basename_only else path
            if real_path.is_dir():
                return FileStatus.create_dir_status(path, modification_time=mod_time)
            else:
                size = stat.st_size
                return FileStatus.create_file_status(
                    path, size, modification_time=mod_time
                )

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

    async def _scandir_iter(self, dir_iter: Iterator[Any]) -> AsyncIterator[FileStatus]:
        async for chunk in self._iterate_in_chunks(dir_iter, SCANDIR_CHUNK_SIZE):
            for entry in chunk:
                yield self._create_filestatus(PurePath(entry), basename_only=True)

    @asynccontextmanager
    async def iterstatus(
        self, path: PurePath
    ) -> AsyncIterator[AsyncIterator[FileStatus]]:
        with await self._loop.run_in_executor(
            self._executor, os.scandir, path
        ) as dir_iter:
            yield self._scandir_iter(dir_iter)

    @classmethod
    def _get_file_or_dir_status(cls, path: PurePath) -> FileStatus:
        return cls._create_filestatus(path, basename_only=False)

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

    def _iterremove(self, path: PurePath) -> Iterator[RemoveListing]:
        with os.scandir(path) as scandir_it:
            entries = list(scandir_it)

        for entry in entries:
            entry_path = PurePath(entry.path)
            if entry.is_dir():
                yield from self._iterremove(entry_path)
            else:
                os.unlink(entry_path)
                yield RemoveListing(entry_path, is_dir=False)
        os.rmdir(path)
        yield RemoveListing(path, is_dir=True)

    async def iterremove(
        self, path: PurePath, *, recursive: bool = False
    ) -> AsyncIterator[RemoveListing]:
        if self._remove_as_dir(path):
            if not recursive:
                raise IsADirectoryError(
                    errno.EISDIR, "Is a directory, use recursive remove", str(path)
                )
            try:
                async for remove_entry in sync_iterator_to_async(
                    self._loop, self._executor, self._iterremove(path)
                ):
                    yield remove_entry
            except OSError as e:
                await self._loop.run_in_executor(
                    self._executor, self._log_remove_error, e, path
                )
                raise e
        else:
            await self._loop.run_in_executor(self._executor, os.unlink, path)
            yield RemoveListing(path, is_dir=False)

    def _remove(self, path: PurePath, recursive: bool) -> None:
        if self._remove_as_dir(path):
            if not recursive:
                raise IsADirectoryError(
                    errno.EISDIR, "Is a directory, use recursive remove", str(path)
                )
            try:
                shutil.rmtree(path)
            except OSError as e:
                self._log_remove_error(e, path)
                raise e
        else:
            os.unlink(path)

    async def remove(self, path: PurePath, *, recursive: bool = False) -> None:
        await self._loop.run_in_executor(self._executor, self._remove, path, recursive)

    def _rename(self, old: PurePath, new: PurePath) -> None:
        concrete_old_path = Path(old)
        concrete_new_path = Path(new)
        concrete_old_path.rename(concrete_new_path)

    async def rename(self, old: PurePath, new: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._rename, old, new)

    async def disk_usage(self, path: PurePath) -> DiskUsageInfo:
        total, used, free = await self._loop.run_in_executor(
            self._executor, shutil.disk_usage, Path(path)
        )
        return DiskUsageInfo(
            total=total,
            used=used,
            free=free,
        )


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

    queue: asyncio.Queue[Union[_T, EndMark]] = asyncio.Queue(queue_size)

    async def put_to_queue(chunk: List[Union[_T, EndMark]]) -> None:
        for item in chunk:
            await queue.put(item)

    def sync_runner() -> None:
        chunk: List[Union[_T, EndMark]] = []
        for item in iter:
            chunk.append(item)
            if len(chunk) == chunk_size:
                asyncio.run_coroutine_threadsafe(put_to_queue(chunk), loop).done()
                chunk = []
        chunk.append(EndMark())
        asyncio.run_coroutine_threadsafe(put_to_queue(chunk), loop).done()

    loop.run_in_executor(executor, sync_runner)

    while True:
        item = await queue.get()
        if isinstance(item, EndMark):
            return
        yield item
