import abc
import asyncio
import contextlib
import enum
import errno
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, replace
from itertools import islice
from pathlib import Path, PurePath
from types import TracebackType
from typing import (
    Any,
    AnyStr,
    AsyncContextManager,
    AsyncIterator,
    Callable,
    Iterator,
    List,
    Optional,
    Tuple,
    Type,
    Union,
    cast,
)

import aiofiles
from aiohttp.typedefs import PathLike


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
        return cast(str, self.value)


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
    async def remove(self, path: PurePath) -> None:
        pass

    @abc.abstractmethod
    def iterremove(self, path: PurePath) -> AsyncIterator[FileStatus]:
        pass

    @abc.abstractmethod
    async def rename(self, old: PurePath, new: PurePath) -> None:
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
        async with aiofiles.open(path, mode=mode, executor=self._executor) as f:
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
        with Path(path) as real_path:
            return not real_path.is_symlink() and real_path.is_dir()

    def _remove_single(self, path: PurePath) -> FileStatus:
        with Path(path) as real_path:
            file_status = self._create_filestatus(real_path)
            if self._remove_as_dir(path):
                real_path.rmdir()
            else:
                real_path.unlink()
        return file_status

    async def iterremove(self, path: PurePath) -> AsyncIterator[FileStatus]:

        # Debug logging (sync code)
        def log_error(e: OSError, failed_path: PurePath) -> None:
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

        if self._remove_as_dir(path):
            try:
                async for (dirpath, __, filenames) in _async_walk(
                    self._loop, self._executor, path, topdown=False
                ):
                    dir = PurePath(dirpath)
                    for file in filenames:
                        filepath = dir / file
                        yield await self._loop.run_in_executor(
                            self._executor, self._remove_single, filepath
                        )
                    yield await self._loop.run_in_executor(
                        self._executor, self._remove_single, dir
                    )
            except OSError as e:
                await self._loop.run_in_executor(self._executor, log_error, e, path)
                raise e
        else:
            yield await self._loop.run_in_executor(
                self._executor, self._remove_single, path
            )

    async def remove(self, path: PurePath) -> None:
        async for __ in self.iterremove(path):
            pass

    def _rename(self, old: PurePath, new: PurePath) -> None:
        concrete_old_path = Path(old)
        concrete_new_path = Path(new)
        concrete_old_path.rename(concrete_new_path)

    async def rename(self, old: PurePath, new: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._rename, old, new)


DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB


async def copy_streams(
    outstream: Any, instream: Any, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> None:
    """perform chunked copying of data between two streams.

    It is assumed that stream implementations would handle retries themselves.
    """
    while True:
        chunk = await outstream.read(chunk_size)
        if not chunk:
            break
        await instream.write(chunk)


async def _async_walk(
    loop: asyncio.AbstractEventLoop,
    executor: Optional[ThreadPoolExecutor],
    # os.walk arguments:
    top: Union[AnyStr, PurePath],
    topdown: bool = True,
    onerror: Optional[Callable[[OSError], Any]] = None,
    followlinks: bool = False,
) -> AsyncIterator[Tuple[str, List[str], List[str]]]:
    class _EndMark:
        pass

    queue: asyncio.Queue[
        Union[Tuple[str, List[str], List[str]], Type[_EndMark]]
    ] = asyncio.Queue(500)

    casted_top = cast(PathLike, top)

    def sync_walker() -> None:
        for entry in os.walk(casted_top, topdown, onerror, followlinks):
            future = asyncio.run_coroutine_threadsafe(queue.put(entry), loop)
            future.result()
        asyncio.run_coroutine_threadsafe(queue.put(_EndMark), loop)

    loop.run_in_executor(executor, sync_walker)

    while True:
        item = await queue.get()
        if isinstance(item, tuple):
            yield item
        return
