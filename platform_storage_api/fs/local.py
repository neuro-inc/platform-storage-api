import abc
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
import enum
import io
import os
from pathlib import PurePath, Path
import shutil
from typing import Optional, List

import aiofiles


# TODO (A Danshyn 04/23/18): likely should be revisited
class StorageType(str, enum.Enum):
    LOCAL = 'local'
    S3 = 's3'


class FileStatusType(str, enum.Enum):
    FILE = 'FILE'
    DIRECTORY = 'DIRECTORY'


@dataclass(frozen=True)
class FileStatus:
    path: PurePath
    size: int = 0
    type: FileStatusType = FileStatusType.FILE

    @property
    def is_dir(self):
        return self.type == FileStatusType.DIRECTORY

    @classmethod
    def create_file_status(cls, path: PurePath, size: int) -> 'FileStatus':
        return cls(path, size)  # type: ignore

    @classmethod
    def create_dir_status(cls, path: PurePath) -> 'FileStatus':
        return cls(path, type=FileStatusType.DIRECTORY)  # type: ignore


class FileSystem(metaclass=abc.ABCMeta):
    @classmethod
    def create(cls, type_: StorageType, *args, **kwargs) -> 'FileSystem':
        if type_ == StorageType.LOCAL:
            return LocalFileSystem(**kwargs)
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
            self, *, executor_max_workers: Optional[int]=None,
            loop=None, **kwargs) -> None:
        self._executor_max_workers = executor_max_workers
        self._executor: Optional[ThreadPoolExecutor] = None

        self._loop = loop or asyncio.get_event_loop()

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
        path = PurePath(entry.name)
        if entry.is_dir():
            return FileStatus.create_dir_status(path)
        else:
            return FileStatus.create_file_status(
                path, size=entry.stat().st_size)

    async def liststatus(self, path: PurePath) -> List[FileStatus]:
        # TODO (A Danshyn 05/03/18): the listing size is disregarded for now
        return await self._loop.run_in_executor(
            self._executor, self._scandir, path)

    def _remove(self, path: PurePath) -> None:
        concrete_path = Path(path)
        if concrete_path.is_dir():
            shutil.rmtree(concrete_path)
        else:
            concrete_path.unlink()

    async def remove(self, path: PurePath) -> None:
        await self._loop.run_in_executor(self._executor, self._remove, path)


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
