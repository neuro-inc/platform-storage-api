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
    LOCAL = "local"
    S3 = "s3"


class FileStatusType(str, enum.Enum):
    DIRECTORY = "DIRECTORY"
    FILE = "FILE"

    def __str__(self):
        return self.value

@dataclass(frozen=True)
class FileStatus:
    path: PurePath
    type: FileStatusType
    length: Optional[int] = None
    modification_time: Optional[int] = None
    # TODO (A Yushkovskiy 29.10.2018): permission should be Action, not a string
    permission: Optional[str] = None

    @classmethod
    def create(cls, path: PurePath,
               basename_only: bool = False,
               permission: str = None) -> 'FileStatus':
        with Path(path) as real_path:
            stat = real_path.stat()
            mod_time = int(stat.st_mtime)  # converting float to int
            path = PurePath(path.name) if basename_only else path
            if real_path.is_dir():
                length = 0
                type = FileStatusType.DIRECTORY
            else:
                length = stat.st_size
                type = FileStatusType.FILE
            return cls(path=path,
                       type=type,
                       length=length,
                       modification_time=mod_time,
                       permission=permission)

    def with_permission(self, permission: str) -> 'FileStatus':
        return FileStatus(path=self.path,
                          type=self.type,
                          length=self.length,
                          modification_time=self.modification_time,
                          permission=permission)

    @classmethod
    def from_primitive(cls, **kwargs) -> 'FileStatus':
        return cls(path=kwargs['path'],
                   type=FileStatusType[kwargs['type']],
                   length=int(kwargs['length']),
                   modification_time=kwargs['modificationTime'],
                   permission=kwargs['permission'])

    def to_primitive(self):
        return {
            'path':             str(self.path),
            'length':           self.length,
            'modificationTime': self.modification_time,
            'permission':       self.permission,
            'type':             self.type
        }


class FileSystem(metaclass=abc.ABCMeta):
    @classmethod
    def create(cls, type_: StorageType, *args, **kwargs) -> "FileSystem":
        if type_ == StorageType.LOCAL:
            return LocalFileSystem(**kwargs)
        raise ValueError(f"Unsupported storage type: {type_}")

    async def __aenter__(self) -> "FileSystem":
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
    def open(self, path: PurePath, mode="r") -> io.FileIO:
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
        self, *, executor_max_workers: Optional[int] = None, loop=None, **kwargs
    ) -> None:
        self._executor_max_workers = executor_max_workers
        self._executor: Optional[ThreadPoolExecutor] = None

        self._loop = loop or asyncio.get_event_loop()

    async def init(self) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=self._executor_max_workers,
            thread_name_prefix="LocalFileSystemThread",
        )

    async def close(self) -> None:
        if self._executor:
            self._executor.shutdown()

    def _listdir(self, path: PurePath) -> List[PurePath]:
        path = Path(path)
        return list(path.iterdir())

    async def listdir(self, path: PurePath) -> List[PurePath]:
        return await self._loop.run_in_executor(self._executor, self._listdir, path)

    def open(self, path: PurePath, mode="r") -> io.FileIO:
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
                status = FileStatus.create(PurePath(entry), basename_only=True)
                statuses.append(status)
        return statuses

    async def liststatus(self, path: PurePath) -> List[FileStatus]:
        # TODO (A Danshyn 05/03/18): the listing size is disregarded for now
        return await self._loop.run_in_executor(self._executor,
                                                self._scandir,
                                                path)

    @classmethod
    def _get_file_or_dir_status(cls, path: PurePath) -> FileStatus:
        return FileStatus.create(path)

    async def get_filestatus(self, path: PurePath) -> FileStatus:
        return await self._loop.run_in_executor(self._executor,
                                                self._get_file_or_dir_status,
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
