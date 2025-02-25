from pathlib import Path, PurePath

import pytest
from yarl import URL

from platform_storage_api.config import (
    Config,
    StorageConfig,
    StorageMode,
    StorageServerConfig,
)


CA_DATA_PEM = "this-is-certificate-authority-public-key"
TOKEN = "this-is-token"


@pytest.fixture
def cert_authority_path(tmp_path: Path) -> str:
    ca_path = tmp_path / "ca.crt"
    ca_path.write_text(CA_DATA_PEM)
    return str(ca_path)


@pytest.fixture
def token_path(tmp_path: Path) -> str:
    token_path = tmp_path / "token"
    token_path.write_text(TOKEN)
    return str(token_path)


class TestServerConfig:
    def test_from_environ(self) -> None:
        environ = {"NP_STORAGE_API_PORT": "1234"}
        config = StorageServerConfig.from_environ(environ)
        assert config.port == 1234

    def test_default_port(self) -> None:
        environ: dict[str, str] = {}
        config = StorageServerConfig.from_environ(environ)
        assert config.port == 8080


class TestStorageConfig:
    def test_from_environ(self) -> None:
        environ = {"NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir"}
        config = StorageConfig.from_environ(environ)
        assert config.fs_local_base_path == PurePath("/path/to/dir")

    def test_from_environ_failed(self) -> None:
        environ: dict[str, str] = {}
        with pytest.raises(KeyError, match="NP_STORAGE_LOCAL_BASE_PATH"):
            StorageConfig.from_environ(environ)


class TestConfig:
    def test_from_environ_defaults(self) -> None:
        environ = {
            "NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir",
            "NP_PLATFORM_AUTH_URL": "-",
            "NP_PLATFORM_ADMIN_URL": "-",
            "NP_PLATFORM_TOKEN": "test-token",
            "NP_PLATFORM_CLUSTER_NAME": "test-cluster",
            "S3_REGION": "test-region",
            "S3_BUCKET_NAME": "test-bucket",
            "NP_STORAGE_API_K8S_API_URL": "https://localhost:8443",
            "NP_STORAGE_ADMISSION_CONTROLLER_SERVICE_NAME": "admission-controller",
            "NP_STORAGE_ADMISSION_CONTROLLER_CERT_SECRET_NAME": "secret",
        }
        config = Config.from_environ(environ)
        assert config.server.port == 8080
        assert config.server.keep_alive_timeout_s == 75
        assert config.storage.mode == StorageMode.SINGLE
        assert config.storage.fs_local_base_path == PurePath("/path/to/dir")
        assert config.storage.fs_local_thread_pool_size == 100
        assert config.platform.auth_url is None
        assert config.platform.admin_url is None
        assert config.platform.token == "test-token"
        assert config.platform.cluster_name == "test-cluster"
        assert config.s3.region == "test-region"
        assert config.s3.bucket_name == "test-bucket"

    def test_from_environ_custom(
        self,
        cert_authority_path: str,
        token_path: str
    ) -> None:
        environ = {
            "NP_STORAGE_MODE": "multiple",
            "NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir",
            "NP_STORAGE_LOCAL_THREAD_POOL_SIZE": "123",
            "NP_PLATFORM_AUTH_URL": "http://platform-auth",
            "NP_PLATFORM_ADMIN_URL": "http://platform-admin",
            "NP_PLATFORM_TOKEN": "test-token",
            "NP_PLATFORM_CLUSTER_NAME": "test-cluster",
            "NP_STORAGE_API_KEEP_ALIVE_TIMEOUT": "900",
            "S3_REGION": "test-region",
            "S3_BUCKET_NAME": "test-bucket",
            "S3_KEY_PREFIX": "test-key-prefix",
            "NP_STORAGE_API_K8S_API_URL": "https://localhost:8443",
            "NP_STORAGE_API_K8S_AUTH_TYPE": "token",
            "NP_STORAGE_API_K8S_CA_PATH": cert_authority_path,
            "NP_STORAGE_API_K8S_TOKEN_PATH": token_path,
            "NP_STORAGE_API_K8S_AUTH_CERT_PATH": "/cert_path",
            "NP_STORAGE_API_K8S_AUTH_CERT_KEY_PATH": "/cert_key_path",
            "NP_STORAGE_API_K8S_NS": "other-namespace",
            "NP_STORAGE_API_K8S_CLIENT_CONN_TIMEOUT": "111",
            "NP_STORAGE_API_K8S_CLIENT_READ_TIMEOUT": "222",
            "NP_STORAGE_API_K8S_CLIENT_WATCH_TIMEOUT": "555",
            "NP_STORAGE_API_K8S_CLIENT_CONN_POOL_SIZE": "333",
            "NP_STORAGE_API_K8S_STORAGE_CLASS": "some-class",
            "NP_STORAGE_ADMISSION_CONTROLLER_SERVICE_NAME": "admission-controller",
            "NP_STORAGE_ADMISSION_CONTROLLER_CERT_SECRET_NAME": "secret",
        }
        config = Config.from_environ(environ)
        assert config.server.port == 8080
        assert config.storage.mode == StorageMode.MULTIPLE
        assert config.storage.fs_local_base_path == PurePath("/path/to/dir")
        assert config.storage.fs_local_thread_pool_size == 123
        assert config.platform.auth_url == URL("http://platform-auth")
        assert config.platform.admin_url == URL("http://platform-admin/apis/admin/v1")
        assert config.platform.token == "test-token"
        assert config.platform.cluster_name == "test-cluster"
        assert config.s3.region == "test-region"
        assert config.s3.bucket_name == "test-bucket"
        assert config.s3.key_prefix == "test-key-prefix"
