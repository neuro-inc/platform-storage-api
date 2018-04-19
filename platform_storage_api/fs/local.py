import abc
import enum

import aiofiles


class StorageType(str, enum.Enum):
    LOCAL = 'local'


class FileSystem(metaclass=abc.ABCMeta):
    @classmethod
    def create(cls, type_: StorageType, *args, **kwargs):
        if type_ == StorageType.LOCAL:
            return LocalFileSystem(*args, **kwargs)

    @abc.abstractmethod
    async def close(self):
        pass

    @abc.abstractmethod
    def open(self, path, mode='r'):
        pass


class LocalFileSystem(FileSystem):
    def __init__(self, *args, **kwargs):
        pass

    async def close(self):
        pass

    def open(self, path, mode='r'):
        return aiofiles.open(path, mode=mode)
