from dataclasses import dataclass
from pathlib import PurePath
import os

from .fs.local import StorageType


@dataclass(frozen=True)
class ServerConfig:
    port: int = 8080

    @classmethod
    def from_environ(cls, environ=None) -> 'ServerConfig':
        environ = environ or os.environ
        port = int(environ.get('NP_STORAGE_API_PORT', cls.port))
        return cls(port=port)  # type: ignore


@dataclass(frozen=True)
class StorageConfig:
    fs_local_base_path: PurePath

    @classmethod
    def from_environ(cls, environ=None) -> 'StorageConfig':
        environ = environ or os.environ
        fs_local_base_path = environ['NP_STORAGE_LOCAL_BASE_PATH']
        return cls(fs_local_base_path=fs_local_base_path)  # type: ignore


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig

    @classmethod
    def from_environ(cls, environ=None) -> 'Config':
        environ = environ or os.environ
        server = ServerConfig.from_environ(environ)
        storage = StorageConfig.from_environ(environ)
        return cls(server=server, storage=storage)  # type: ignore
