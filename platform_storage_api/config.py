from typing import Optional, Dict

from dataclasses import dataclass, field
from pathlib import PurePath
import os

from yarl import URL


@dataclass(frozen=True)
class ServerConfig:
    host: str = '0.0.0.0'
    port: int = 8080
    name: str = 'Storage API'

    @classmethod
    def from_environ(cls, environ=None) -> 'ServerConfig':
        return EnvironConfigFactory(environ).create_server()


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: URL
    service_token: str = field(repr=False)


@dataclass(frozen=True)
class StorageConfig:
    fs_local_base_path: PurePath

    @classmethod
    def from_environ(cls, environ=None) -> 'StorageConfig':
        return EnvironConfigFactory(environ).create_storage()


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    auth: AuthConfig

    @classmethod
    def from_environ(cls, environ=None) -> 'Config':
        return EnvironConfigFactory(environ).create()



class EnvironConfigFactory:
    def __init__(self, environ: Optional[Dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def create_storage(self) -> StorageConfig:
        fs_local_base_path = self._environ['NP_STORAGE_LOCAL_BASE_PATH']
        return StorageConfig(fs_local_base_path=fs_local_base_path)

    def create_server(self) -> ServerConfig:
        port = int(self._environ.get('NP_STORAGE_API_PORT', ServerConfig.port))
        return ServerConfig(port=port)

    def create_auth(self) -> AuthConfig:
        url = URL(self._environ['NP_STORAGE_AUTH_URL'])
        token = self._environ['NP_STORAGE_AUTH_TOKEN']
        return AuthConfig(  # type: ignore
            server_endpoint_url=url,
            service_token=token,
        )

    def create(self) -> Config:
        server_config = self.create_server()
        storage_config = self.create_storage()
        auth_config = self.create_auth()
        return Config(  # type: ignore
            server=server_config,
            storage=storage_config,
            auth=auth_config,
        )