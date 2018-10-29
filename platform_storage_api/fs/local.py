import abc
import asyncio
import enum
import io
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path, PurePath
from typing import List, Optional

import aiofiles


# TODO (A Danshyn 04/23/18): likely should be revisited
class StorageType(str, enum.Enum):
    LOCAL = 'local'
    S3 = 's3'


class FileStatusType(str, enum.Enum):
    DIRECTORY = 'DIRECTORY'
    FILE = 'FILE'

    def __str__(self):
        return self.value


@dataclass(frozen=True)
class FileStatus:
    path: PurePath
    size: int
    type: FileStatusType
    modification_time: Optional[int] = None

    @property
    def is_dir(self):
        return self.type == FileStatusType.DIRECTORY

    @classmethod
    def create_file_status(cls,
                           path: PurePath,
                           size: int,
                           modification_time: int=None) -> 'FileStatus':
        return cls(path=path,
                   type=FileStatusType.FILE,
                   size=size,
                   modification_time=modification_time)

    @classmethod
    def create_dir_status(cls,
                          path: PurePath,
                          modification_time: int=None) -> 'FileStatus':
        return cls(path=path,
                   type=FileStatusType.DIRECTORY,
                   size=0,
                   modification_time=modification_time)


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

    @abc.abstractmethod
    async def get_filestatus(self, path: PurePath) -> FileStatus:
        pass

    @abc.abstractmethod
    async def remove(self, path: PurePath) -> None:
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
        # TODO (A Yushkovskiy, 26.10.2018) Refact: re-use `_get_filedir_status`
        # see issue #41
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

    @classmethod
    def _get_filedir_status(cls, path: PurePath, name_only: bool=False) \
            -> FileStatus:
        with Path(path) as real_path:
            stat = real_path.stat()
            mtime = int(stat.st_mtime)  # converting float to int
            path = PurePath(path.name) if name_only else path
            if real_path.is_dir():
                return FileStatus.create_dir_status(path,
                                                    modification_time=mtime)
            else:
                return FileStatus.create_file_status(path,
                                                     size=stat.st_size,
                                                     modification_time=mtime)

    async def get_filestatus(self, path: PurePath) -> FileStatus:
        return await self._loop.run_in_executor(self._executor,
                                                self._get_filedir_status,
                                                path)

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
