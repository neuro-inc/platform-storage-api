import enum
import os
from collections.abc import Sequence
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
class ZipkinConfig:
    url: URL
    app_name: str = "platform-storage"
    sample_rate: float = 0


@dataclass(frozen=True)
class SentryConfig:
    dsn: URL
    cluster_name: str
    app_name: str = "platform-storage"
    sample_rate: float = 0


@dataclass(frozen=True)
class AuthConfig:
    server_endpoint_url: Optional[URL]
    service_token: str = field(repr=False)


@dataclass(frozen=True)
class AdminConfig:
    server_endpoint_url: URL
    service_token: str = field(repr=False)


@dataclass(frozen=True)
class PlatformConfigConfig:
    server_endpoint_url: URL
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
class CORSConfig:
    allowed_origins: Sequence[str] = ()


@dataclass(frozen=True)
class Config:
    server: ServerConfig
    storage: StorageConfig
    auth: AuthConfig
    admin: AdminConfig
    platform_config: PlatformConfigConfig
    cors: CORSConfig
    cluster_name: str
    permission_expiration_interval_s: float = 0
    permission_forgetting_interval_s: float = 0

    zipkin: Optional[ZipkinConfig] = None
    sentry: Optional[SentryConfig] = None

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

    def create_admin(self) -> AdminConfig:
        url = self._get_url("NP_STORAGE_ADMIN_URL")
        assert url
        token = self._environ["NP_STORAGE_AUTH_TOKEN"]
        return AdminConfig(server_endpoint_url=url, service_token=token)

    def create_platform_config(self) -> PlatformConfigConfig:
        url = self._get_url("NP_STORAGE_PLATFORM_CONFIG_URL")
        assert url
        token = self._environ["NP_STORAGE_AUTH_TOKEN"]
        return PlatformConfigConfig(server_endpoint_url=url, service_token=token)

    def create_zipkin(self) -> Optional[ZipkinConfig]:
        if "NP_ZIPKIN_URL" not in self._environ:
            return None

        url = URL(self._environ["NP_ZIPKIN_URL"])
        app_name = self._environ.get("NP_ZIPKIN_APP_NAME", ZipkinConfig.app_name)
        sample_rate = float(
            self._environ.get("NP_ZIPKIN_SAMPLE_RATE", ZipkinConfig.sample_rate)
        )
        return ZipkinConfig(url=url, app_name=app_name, sample_rate=sample_rate)

    def create_sentry(self) -> Optional[SentryConfig]:
        if "NP_SENTRY_DSN" not in self._environ:
            return None

        return SentryConfig(
            dsn=URL(self._environ["NP_SENTRY_DSN"]),
            cluster_name=self._environ["NP_SENTRY_CLUSTER_NAME"],
            app_name=self._environ.get("NP_SENTRY_APP_NAME", SentryConfig.app_name),
            sample_rate=float(
                self._environ.get("NP_SENTRY_SAMPLE_RATE", SentryConfig.sample_rate)
            ),
        )

    def create_cors(self) -> CORSConfig:
        origins: Sequence[str] = CORSConfig.allowed_origins
        origins_str = self._environ.get("NP_CORS_ORIGINS", "").strip()
        if origins_str:
            origins = origins_str.split(",")
        return CORSConfig(allowed_origins=origins)

    def create(self) -> Config:
        server_config = self.create_server()
        storage_config = self.create_storage()
        auth_config = self.create_auth()
        admin_config = self.create_admin()
        platform_config_config = self.create_platform_config()
        zipkin_config = self.create_zipkin()
        sentry_config = self.create_sentry()
        cors_config = self.create_cors()
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
            admin=admin_config,
            platform_config=platform_config_config,
            zipkin=zipkin_config,
            sentry=sentry_config,
            cors=cors_config,
            cluster_name=cluster_name,
            permission_expiration_interval_s=permission_expiration_interval_s,
            permission_forgetting_interval_s=permission_forgetting_interval_s,
        )
