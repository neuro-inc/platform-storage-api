from pathlib import PurePath

import pytest
from yarl import URL

from platform_storage_api.config import (
    Config,
    ServerConfig,
    StorageConfig,
    StorageMode,
)


class TestServerConfig:
    def test_from_environ(self) -> None:
        environ = {"NP_STORAGE_API_PORT": "1234"}
        config = ServerConfig.from_environ(environ)
        assert config.port == 1234

    def test_default_port(self) -> None:
        environ: dict[str, str] = {}
        config = ServerConfig.from_environ(environ)
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
            "NP_STORAGE_AUTH_URL": "-",
            "NP_STORAGE_AUTH_TOKEN": "hello-token",
            "NP_CLUSTER_NAME": "test-cluster",
        }
        config = Config.from_environ(environ)
        assert config.server.port == 8080
        assert config.server.keep_alive_timeout_s == 75
        assert config.storage.mode == StorageMode.SINGLE
        assert config.storage.fs_local_base_path == PurePath("/path/to/dir")
        assert config.storage.fs_local_thread_pool_size == 100
        assert config.auth.server_endpoint_url is None
        assert config.auth.service_token == "hello-token"
        assert config.cluster_name == "test-cluster"

    def test_from_environ_custom(self) -> None:
        environ = {
            "NP_STORAGE_MODE": "multiple",
            "NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir",
            "NP_STORAGE_LOCAL_THREAD_POOL_SIZE": "123",
            "NP_STORAGE_AUTH_URL": "http://127.0.0.1/",
            "NP_STORAGE_AUTH_TOKEN": "hello-token",
            "NP_CLUSTER_NAME": "test-cluster",
            "NP_STORAGE_API_KEEP_ALIVE_TIMEOUT": "900",
        }
        config = Config.from_environ(environ)
        assert config.server.port == 8080
        assert config.storage.mode == StorageMode.MULTIPLE
        assert config.storage.fs_local_base_path == PurePath("/path/to/dir")
        assert config.storage.fs_local_thread_pool_size == 123
        assert config.auth.server_endpoint_url == URL("http://127.0.0.1/")
        assert config.auth.service_token == "hello-token"
        assert config.cluster_name == "test-cluster"
