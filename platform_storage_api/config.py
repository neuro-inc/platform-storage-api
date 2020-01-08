import os
from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Dict, Optional

from yarl import URL


@dataclass(frozen=True)
class ServerConfig:
    host: str = "0.0.0.0"
    port: int = 8080
    name: str = "Storage API"

    @classmethod
    def from_environ(cls, environ: Optional[Dict[str, str]] = None) -> "ServerConfig":
        return EnvironConfigFactory(environ).create_server()


@dataclass(frozen=True)
class ZipkinConfig:
    url: URL
    sample_rate: float


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: URL
    service_token: str = field(repr=False)


@dataclass(frozen=True)
class StorageConfig:
    fs_local_base_path: PurePath
    fs_local_thread_pool_size: int = 100

    @classmethod
    def from_environ(cls, environ: Optional[Dict[str, str]] = None) -> "StorageConfig":
        return EnvironConfigFactory(environ).create_storage()


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    auth: AuthConfig
    zipkin: ZipkinConfig
    cluster_name: str
    permission_expiration_interval_s: float = 0
    permission_forgetting_interval_s: float = 0

    @classmethod
    def from_environ(cls, environ: Optional[Dict[str, str]] = None) -> "Config":
        return EnvironConfigFactory(environ).create()


class EnvironConfigFactory:
    def __init__(self, environ: Optional[Dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def create_storage(self) -> StorageConfig:
        fs_local_base_path = self._environ["NP_STORAGE_LOCAL_BASE_PATH"]
        fs_local_thread_pool_size = int(
            self._environ.get(
                "NP_STORAGE_LOCAL_THREAD_POOL_SIZE",
                StorageConfig.fs_local_thread_pool_size,
            )
        )
        return StorageConfig(
            fs_local_base_path=PurePath(fs_local_base_path),
            fs_local_thread_pool_size=fs_local_thread_pool_size,
        )

    def create_server(self) -> ServerConfig:
        port = int(self._environ.get("NP_STORAGE_API_PORT", ServerConfig.port))
        return ServerConfig(port=port)

    def create_auth(self) -> AuthConfig:
        url = URL(self._environ["NP_STORAGE_AUTH_URL"])
        token = self._environ["NP_STORAGE_AUTH_TOKEN"]
        return AuthConfig(server_endpoint_url=url, service_token=token)

    def create_zipkin(self) -> ZipkinConfig:
        url = URL(self._environ["NP_STORAGE_ZIPKIN_URL"])
        sample_rate = float(self._environ["NP_STORAGE_ZIPKIN_SAMPLE_RATE"])
        return ZipkinConfig(url=url, sample_rate=sample_rate)

    def create(self) -> Config:
        server_config = self.create_server()
        storage_config = self.create_storage()
        auth_config = self.create_auth()
        zipkin_config = self.create_zipkin()
        cluster_name = self._environ["NP_CLUSTER_NAME"]
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
            zipkin=zipkin_config,
            cluster_name=cluster_name,
            permission_expiration_interval_s=permission_expiration_interval_s,
            permission_forgetting_interval_s=permission_forgetting_interval_s,
        )
