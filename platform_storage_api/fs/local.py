import abc
import enum
from pathlib import Path

import aiofiles


class StorageType(str, enum.Enum):
    LOCAL = 'local'


class FileSystem(metaclass=abc.ABCMeta):
    @classmethod
    def create(cls, type_: StorageType, *args, **kwargs):
        if type_ == StorageType.LOCAL:
            return LocalFileSystem(*args, **kwargs)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    @abc.abstractmethod
    async def close(self):
        pass

    @abc.abstractmethod
    def open(self, path: Path, mode='r'):
        pass


class LocalFileSystem(FileSystem):
    def __init__(self, *args, **kwargs):
        pass

    async def close(self):
        pass

    def open(self, path: Path, mode='r'):
        return aiofiles.open(path, mode=mode)


DEFAULT_CHUNK_SIZE = 1 * 1024 * 1024  # 1 MB


async def copy_streams(outstream, instream, chunk_size=DEFAULT_CHUNK_SIZE):
    # streams should handle reties etc
    while True:
        chunk = await outstream.read(chunk_size)
        if not chunk:
            break
        await instream.write(chunk)
