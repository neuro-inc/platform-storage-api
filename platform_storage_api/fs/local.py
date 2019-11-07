import abc
import asyncio
import enum
import errno
import logging
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, replace
from pathlib import Path, PurePath
from types import TracebackType
from typing import Any, List, Optional, Type, cast

import aiofiles

from platform_storage_api.trace import trace, tracing_cm


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
    async def liststatus(self, path: PurePath) -> List[FileStatus]:
        pass

    @abc.abstractmethod
    async def get_filestatus(self, path: PurePath) -> FileStatus:
        pass

    @abc.abstractmethod
    async def remove(self, path: PurePath) -> None:
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

    @trace
    async def listdir(self, path: PurePath) -> List[PurePath]:  # type: ignore
        return await self._loop.run_in_executor(self._executor, self._listdir, path)

    # Actual return type is an async version of io.FileIO
    def open(self, path: PurePath, mode: str = "r") -> Any:
        async with tracing_cm("open"):
            return aiofiles.open(path, mode=mode, executor=self._executor)

    def _mkdir(self, path: PurePath) -> None:
        # TODO (A Danshyn 04/23/18): consider setting mode
        Path(path).mkdir(parents=True, exist_ok=True)

    @trace
    async def mkdir(self, path: PurePath) -> None:  # type: ignore
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

    @classmethod
    def _scandir(cls, path: PurePath) -> List[FileStatus]:
        statuses = []
        with os.scandir(path) as dir_iter:
            for entry in dir_iter:
                entry_path = PurePath(entry)
                status = cls._create_filestatus(entry_path, basename_only=True)
                statuses.append(status)
        return statuses

    @trace
    async def liststatus(self, path: PurePath) -> List[FileStatus]:  # type: ignore
        # TODO (A Danshyn 05/03/18): the listing size is disregarded for now
        return await self._loop.run_in_executor(self._executor, self._scandir, path)

    @classmethod
    def _get_file_or_dir_status(cls, path: PurePath) -> FileStatus:
        return cls._create_filestatus(path, basename_only=False)

    @trace
    async def get_filestatus(self, path: PurePath) -> FileStatus:  # type: ignore
        return await self._loop.run_in_executor(
            self._executor, self._get_file_or_dir_status, path
        )

    def _remove(self, path: PurePath) -> None:
        concrete_path = Path(path)
        if not concrete_path.is_symlink() and concrete_path.is_dir():
            try:
                shutil.rmtree(concrete_path)
            except OSError as e:
                # Debug logging
                if e.filename:
                    path = e.filename
                path_access_ok = os.access(path, os.W_OK)
                try:
                    path_mode = f"{os.stat(path).st_mode:03o}"
                except OSError:
                    path_mode = "?"

                parent_path = os.path.dirname(path)
                parent_path_access_ok = os.access(parent_path, os.W_OK)
                try:
                    parent_path_mode = f"{os.stat(parent_path).st_mode:03o}"
                except OSError:
                    parent_path_mode = "?"

                logger.warning(
                    "OSError for path = %s, path_mode = %s, access = %s, "
                    "parent_path_mode = %s, parent_access = %s, "
                    "error_message = %s, errno = %s",
                    path,
                    path_mode,
                    path_access_ok,
                    parent_path_mode,
                    parent_path_access_ok,
                    str(e),
                    errno.errorcode.get(e.errno, e.errno),
                )
                raise e
        else:
            concrete_path.unlink()

    @trace
    async def remove(self, path: PurePath) -> None:  # type: ignore
        await self._loop.run_in_executor(self._executor, self._remove, path)

    def _rename(self, old: PurePath, new: PurePath) -> None:
        concrete_old_path = Path(old)
        concrete_new_path = Path(new)
        concrete_old_path.rename(concrete_new_path)

    @trace
    async def rename(self, old: PurePath, new: PurePath) -> None:  # type: ignore
        await self._loop.run_in_executor(self._executor, self._rename, old, new)


DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB


async def copy_streams(
    outstream: Any, instream: Any, chunk_size: int = DEFAULT_CHUNK_SIZE
) -> None:
    """perform chunked copying of data between two streams.

    It is assumed that stream implementations would handle retries themselves.
    """
    while True:
        async with tracing_cm("recv_chunk"):
            chunk = await outstream.read(chunk_size)
        if not chunk:
            break
        async with tracing_cm("send_chunk"):
            await instream.write(chunk)
