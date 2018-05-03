import abc
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import enum
import io
import os
from pathlib import PurePath, Path
from typing import Optional, List

import aiofiles


# TODO (A Danshyn 04/23/18): likely should be revisited
class StorageType(str, enum.Enum):
    LOCAL = 'local'
    S3 = 's3'


@dataclass(frozen=True)
class FileStatus:
    path: PurePath
    size: int = 0
    is_dir: bool = False


class FileSystem(metaclass=abc.ABCMeta):
    @classmethod
    def create(cls, type_: StorageType, *args, **kwargs) -> 'FileSystem':
        if type_ == StorageType.LOCAL:
            return LocalFileSystem(*args, **kwargs)
        raise ValueError(f'Unsupported storage type: {type_}')

    async def __aenter__(self) -> 'FileSystem':
        await self.init()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    @abc.abstractmethod
    async def init(self) -> None:
        pass

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    @abc.abstractmethod
    def open(self, path: PurePath, mode='r') -> io.FileIO:
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


class LocalFileSystem(FileSystem):
    def __init__(
            self, *args, executor_max_workers: Optional[int]=None,
            **kwargs) -> None:
        self._executor_max_workers = executor_max_workers
        self._executor: Optional[ThreadPoolExecutor] = None

        # TODO: consider moving up
        self._loop = kwargs.pop('loop', None) or asyncio.get_event_loop()

    async def init(self) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=self._executor_max_workers,
            thread_name_prefix='LocalFileSystemThread')

    async def close(self) -> None:
        if self._executor:
            self._executor.shutdown()

    def _listdir(self, path: PurePath) -> List[PurePath]:
        path = Path(path)
        return list(path.iterdir())

    async def listdir(self, path: PurePath) -> List[PurePath]:
        return await self._loop.run_in_executor(
            self._executor, self._listdir, path)

    def open(self, path: PurePath, mode='r') -> io.FileIO:
        return aiofiles.open(path, mode=mode, executor=self._executor)

    def _mkdir(self, path: PurePath):
        # TODO (A Danshyn 04/23/18): consider setting mode
        Path(path).mkdir(parents=True, exist_ok=True)

    async def mkdir(self, path: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._mkdir, path)

    def _scandir(self, path: PurePath) -> List[FileStatus]:
        statuses = []
        with os.scandir(path) as dir_iter:
            for entry in dir_iter:
                status = self._convert_dir_entry_to_file_status(entry)
                statuses.append(status)
        return statuses

    def _convert_dir_entry_to_file_status(
            self, entry: os.DirEntry) -> FileStatus:
        is_dir = entry.is_dir()
        size = 0 if is_dir else entry.stat().st_size
        return FileStatus(
            path=PurePath(entry.name), size=size, is_dir=is_dir)

    async def liststatus(self, path: PurePath) -> List[FileStatus]:
        return await self._loop.run_in_executor(
            self._executor, self._scandir, path)


DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB


async def copy_streams(outstream, instream, chunk_size=DEFAULT_CHUNK_SIZE):
    """perform chunked copying of data between two streams.

    It is assumed that stream implementations would handle retries themselves.
    """
    while True:
        chunk = await outstream.read(chunk_size)
        if not chunk:
            break
        await instream.write(chunk)
