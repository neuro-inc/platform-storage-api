import enum
import os
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Optional

from yarl import URL


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    name: str = "Storage API"
    keep_alive_timeout_s: float = 75

    @classmethod
    def from_environ(cls, environ: Optional[dict[str, str]] = None) -> "ServerConfig":
        return EnvironConfigFactory(environ).create_server()


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: Optional[URL]
    service_token: str = field(repr=False)


class StorageMode(str, enum.Enum):
    SINGLE = "single"
    MULTIPLE = "multiple"


@dataclass(frozen=True)
class StorageConfig:
    fs_local_base_path: PurePath
    fs_local_thread_pool_size: int = 100

    mode: StorageMode = StorageMode.SINGLE

    @classmethod
    def from_environ(cls, environ: Optional[dict[str, str]] = None) -> "StorageConfig":
        return EnvironConfigFactory(environ).create_storage()


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    auth: AuthConfig
    cluster_name: str
    permission_expiration_interval_s: float = 0
    permission_forgetting_interval_s: float = 0

    @classmethod
    def from_environ(cls, environ: Optional[dict[str, str]] = None) -> "Config":
        return EnvironConfigFactory(environ).create()


class EnvironConfigFactory:
    def __init__(self, environ: Optional[dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def _get_url(self, name: str) -> Optional[URL]:
        value = self._environ[name]
        if value == "-":
            return None
        else:
            return URL(value)

    def create_storage(self) -> StorageConfig:
        fs_local_base_path = self._environ["NP_STORAGE_LOCAL_BASE_PATH"]
        fs_local_thread_pool_size = int(
            self._environ.get(
                "NP_STORAGE_LOCAL_THREAD_POOL_SIZE",
                StorageConfig.fs_local_thread_pool_size,
            )
        )
        return StorageConfig(
            mode=StorageMode(
                self._environ.get("NP_STORAGE_MODE", StorageConfig.mode).lower()
            ),
            fs_local_base_path=PurePath(fs_local_base_path),
            fs_local_thread_pool_size=fs_local_thread_pool_size,
        )

    def create_server(self) -> ServerConfig:
        port = int(self._environ.get("NP_STORAGE_API_PORT", ServerConfig.port))
        keep_alive_timeout_s = int(
            self._environ.get(
                "NP_STORAGE_API_KEEP_ALIVE_TIMEOUT", ServerConfig.keep_alive_timeout_s
            )
        )
        return ServerConfig(port=port, keep_alive_timeout_s=keep_alive_timeout_s)

    def create_auth(self) -> AuthConfig:
        url = self._get_url("NP_STORAGE_AUTH_URL")
        token = self._environ["NP_STORAGE_AUTH_TOKEN"]
        return AuthConfig(server_endpoint_url=url, service_token=token)

    def create(self) -> Config:
        server_config = self.create_server()
        storage_config = self.create_storage()
        auth_config = self.create_auth()
        cluster_name = self._environ["NP_CLUSTER_NAME"]
        assert cluster_name
        permission_expiration_interval_s: float = float(
            self._environ.get(
                "NP_PERMISSION_EXPIRATION_INTERVAL",
                Config.permission_expiration_interval_s,
            )
        )
        permission_forgetting_interval_s: float = float(
            self._environ.get(
                "NP_PERMISSION_FORGETTING_INTERVAL",
                Config.permission_forgetting_interval_s,
            )
        )
        return Config(
            server=server_config,
            storage=storage_config,
            auth=auth_config,
            cluster_name=cluster_name,
            permission_expiration_interval_s=permission_expiration_interval_s,
            permission_forgetting_interval_s=permission_forgetting_interval_s,
        )
