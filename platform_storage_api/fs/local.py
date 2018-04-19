import abc
import enum
import io
from pathlib import Path

import aiofiles


class StorageType(str, enum.Enum):
    LOCAL = 'local'
    S3 = 's3'


class FileSystem(metaclass=abc.ABCMeta):
    @classmethod
    def create(cls, type_: StorageType, *args, **kwargs) -> 'FileSystem':
        if type_ == StorageType.LOCAL:
            return LocalFileSystem(*args, **kwargs)
        raise ValueError(f'Unsupported storage type: {type_}')

    async def __aenter__(self) -> 'FileSystem':
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    @abc.abstractmethod
    async def close(self) -> None:
        pass

    @abc.abstractmethod
    def open(self, path: Path, mode='r') -> io.FileIO:
        pass


class LocalFileSystem(FileSystem):
    def __init__(self, *args, **kwargs):
        pass

    async def close(self) -> None:
        """no need to close anything, because there is no state for now.

        it may soon be the case if we start using explicit thread pool
        executors.
        """

    def open(self, path: Path, mode='r') -> io.FileIO:
        return aiofiles.open(path, mode=mode)


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
