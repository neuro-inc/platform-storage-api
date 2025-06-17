import enum
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path, PurePath

from apolo_kube_client.config import KubeClientAuthType, KubeConfig
from yarl import URL

logger = logging.getLogger(__name__)


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
        cls, environ: dict[str, str] | None = None
    ) -> "StorageServerConfig":
        return EnvironConfigFactory(environ).create_storage_server()


@dataclass(frozen=True)
class PlatformConfig:
    auth_url: URL | None
    admin_url: URL | None
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
    def from_environ(cls, environ: dict[str, str] | None = None) -> "StorageConfig":
        return EnvironConfigFactory(environ).create_storage()


@dataclass(frozen=True)
class S3Config:
    region: str
    bucket_name: str
    key_prefix: str = ""
    access_key_id: str | None = field(repr=False, default=None)
    secret_access_key: str | None = field(repr=False, default=None)
    endpoint_url: str | None = None


@dataclass(frozen=True)
class AdmissionControllerConfig:
    cert_secret_name: str

    @classmethod
    def from_environ(
        cls,
        environ: dict[str, str] | None = None,
    ) -> "AdmissionControllerConfig":
        return EnvironConfigFactory(environ).create_admission_controller()


@dataclass(frozen=True)
class Config:
    server: StorageServerConfig
    storage: StorageConfig
    platform: PlatformConfig
    s3: S3Config
    admission_controller_config: AdmissionControllerConfig
    kube: KubeConfig | None = None
    permission_expiration_interval_s: float = 0
    permission_forgetting_interval_s: float = 0

    @classmethod
    def from_environ(cls, environ: dict[str, str] | None = None) -> "Config":
        return EnvironConfigFactory(environ).create()


@dataclass(frozen=True)
class MetricsConfig:
    s3: S3Config
    server: ServerConfig = ServerConfig()


class EnvironConfigFactory:
    def __init__(self, environ: dict[str, str] | None = None) -> None:
        self._environ = environ or os.environ

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
            admission_controller_config=self.create_admission_controller(),
            kube=self.create_kube(),
            permission_expiration_interval_s=permission_expiration_interval_s,
            permission_forgetting_interval_s=permission_forgetting_interval_s,
        )

    def _get_url(self, name: str) -> URL | None:
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

    def create_metrics(self) -> MetricsConfig:
        return MetricsConfig(
            server=self.create_server(),
            s3=self.create_s3(),
        )

    def create_kube(self) -> KubeConfig | None:
        endpoint_url = self._environ.get("NP_STORAGE_API_K8S_API_URL")
        if not endpoint_url:
            logger.info("kube client won't be initialized due to a missing url")
            return None
        auth_type = KubeClientAuthType(
            self._environ.get(
                "NP_STORAGE_API_K8S_AUTH_TYPE", KubeConfig.auth_type.value
            )
        )
        ca_path = self._environ.get("NP_STORAGE_API_K8S_CA_PATH")
        ca_data = Path(ca_path).read_text() if ca_path else None

        token_path = self._environ.get("NP_STORAGE_API_K8S_TOKEN_PATH")

        return KubeConfig(
            endpoint_url=endpoint_url,
            cert_authority_data_pem=ca_data,
            auth_type=auth_type,
            auth_cert_path=self._environ.get("NP_STORAGE_API_K8S_AUTH_CERT_PATH"),
            auth_cert_key_path=self._environ.get(
                "NP_STORAGE_API_K8S_AUTH_CERT_KEY_PATH"
            ),
            token=None,
            token_path=token_path,
            namespace=self._environ.get("NP_STORAGE_API_K8S_NS", KubeConfig.namespace),
            client_conn_timeout_s=int(
                self._environ.get("NP_STORAGE_API_K8S_CLIENT_CONN_TIMEOUT")
                or KubeConfig.client_conn_timeout_s
            ),
            client_read_timeout_s=int(
                self._environ.get("NP_STORAGE_API_K8S_CLIENT_READ_TIMEOUT")
                or KubeConfig.client_read_timeout_s
            ),
            client_watch_timeout_s=int(
                self._environ.get("NP_STORAGE_API_K8S_CLIENT_WATCH_TIMEOUT")
                or KubeConfig.client_watch_timeout_s
            ),
            client_conn_pool_size=int(
                self._environ.get("NP_STORAGE_API_K8S_CLIENT_CONN_POOL_SIZE")
                or KubeConfig.client_conn_pool_size
            ),
        )

    def create_admission_controller(self) -> AdmissionControllerConfig:
        cert_secret_name = self._environ[
            "NP_STORAGE_ADMISSION_CONTROLLER_CERT_SECRET_NAME"
        ]
        return AdmissionControllerConfig(
            cert_secret_name=cert_secret_name,
        )
