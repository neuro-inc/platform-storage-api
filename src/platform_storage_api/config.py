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


@dataclass(frozen=True)
class StorageServerConfig(ServerConfig):
    name: str = "Storage API"
    keep_alive_timeout_s: float = 75

    @classmethod
    def from_environ(
        cls, environ: Optional[dict[str, str]] = None
    ) -> "StorageServerConfig":
        return EnvironConfigFactory(environ).create_storage_server()


@dataclass(frozen=True)
class PlatformConfig:
    auth_url: Optional[URL]
    admin_url: Optional[URL]
    token: str = field(repr=False)
    cluster_name: str


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
class S3Config:
    region: str
    bucket_name: str
    key_prefix: str = ""
    access_key_id: Optional[str] = field(repr=False, default=None)
    secret_access_key: Optional[str] = field(repr=False, default=None)
    endpoint_url: Optional[str] = None


@dataclass(frozen=True)
class Config:
    server: StorageServerConfig
    storage: StorageConfig
    platform: PlatformConfig
    s3: S3Config
    permission_expiration_interval_s: float = 0
    permission_forgetting_interval_s: float = 0

    @classmethod
    def from_environ(cls, environ: Optional[dict[str, str]] = None) -> "Config":
        return EnvironConfigFactory(environ).create()


@dataclass(frozen=True)
class MetricsConfig:
    s3: S3Config
    server: ServerConfig = ServerConfig()


class EnvironConfigFactory:
    def __init__(self, environ: Optional[dict[str, str]] = None) -> None:
        self._environ = environ or os.environ

    def _get_url(self, name: str) -> Optional[URL]:
        value = self._environ[name]
        return None if value == "-" else URL(value)

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
        return ServerConfig(
            host=self._environ.get("SERVER_HOST", ServerConfig.host),
            port=int(self._environ.get("SERVER_PORT", ServerConfig.port)),
        )

    def create_storage_server(self) -> StorageServerConfig:
        port = int(self._environ.get("NP_STORAGE_API_PORT", StorageServerConfig.port))
        keep_alive_timeout_s = int(
            self._environ.get(
                "NP_STORAGE_API_KEEP_ALIVE_TIMEOUT",
                StorageServerConfig.keep_alive_timeout_s,
            )
        )
        return StorageServerConfig(port=port, keep_alive_timeout_s=keep_alive_timeout_s)

    def create_platform(self) -> PlatformConfig:
        admin_url = self._get_url("NP_PLATFORM_ADMIN_URL")
        if admin_url:
            admin_url = admin_url / "apis/admin/v1"
        return PlatformConfig(
            auth_url=self._get_url("NP_PLATFORM_AUTH_URL"),
            admin_url=admin_url,
            token=self._environ["NP_PLATFORM_TOKEN"],
            cluster_name=self._environ["NP_PLATFORM_CLUSTER_NAME"],
        )

    def create_s3(self) -> S3Config:
        return S3Config(
            region=self._environ["S3_REGION"],
            endpoint_url=self._environ.get("S3_ENDPOINT_URL"),
            bucket_name=self._environ["S3_BUCKET_NAME"],
            key_prefix=self._environ.get("S3_KEY_PREFIX", S3Config.key_prefix),
        )

    def create(self) -> Config:
        server_config = self.create_storage_server()
        storage_config = self.create_storage()
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
            platform=self.create_platform(),
            s3=self.create_s3(),
            permission_expiration_interval_s=permission_expiration_interval_s,
            permission_forgetting_interval_s=permission_forgetting_interval_s,
        )

    def create_metrics(self) -> MetricsConfig:
        return MetricsConfig(
            server=self.create_server(),
            s3=self.create_s3(),
        )
