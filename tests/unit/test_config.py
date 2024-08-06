from pathlib import PurePath

import pytest
from yarl import URL

from platform_storage_api.config import (
    Config,
    StorageConfig,
    StorageMode,
    StorageServerConfig,
)


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
            "AWS_REGION": "test-region",
            "AWS_METRICS_S3_BUCKET_NAME": "test-bucket",
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
        assert config.aws.region == "test-region"
        assert config.aws.metrics_s3_bucket_name == "test-bucket"

    def test_from_environ_custom(self) -> None:
        environ = {
            "NP_STORAGE_MODE": "multiple",
            "NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir",
            "NP_STORAGE_LOCAL_THREAD_POOL_SIZE": "123",
            "NP_PLATFORM_AUTH_URL": "http://platform-auth",
            "NP_PLATFORM_ADMIN_URL": "http://platform-admin",
            "NP_PLATFORM_TOKEN": "test-token",
            "NP_PLATFORM_CLUSTER_NAME": "test-cluster",
            "NP_STORAGE_API_KEEP_ALIVE_TIMEOUT": "900",
            "AWS_REGION": "test-region",
            "AWS_METRICS_S3_BUCKET_NAME": "test-bucket",
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
        assert config.aws.region == "test-region"
        assert config.aws.metrics_s3_bucket_name == "test-bucket"
