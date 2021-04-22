from pathlib import PurePath
from typing import Dict

import pytest
from yarl import URL

from platform_storage_api.config import (
    Config,
    EnvironConfigFactory,
    SentryConfig,
    ServerConfig,
    StorageConfig,
    ZipkinConfig,
)


class TestServerConfig:
    def test_from_environ(self) -> None:
        environ = {"NP_STORAGE_API_PORT": "1234"}
        config = ServerConfig.from_environ(environ)
        assert config.port == 1234

    def test_default_port(self) -> None:
        environ: Dict[str, str] = {}
        config = ServerConfig.from_environ(environ)
        assert config.port == 8080


class TestStorageConfig:
    def test_from_environ(self) -> None:
        environ = {"NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir"}
        config = StorageConfig.from_environ(environ)
        assert config.fs_local_base_path == PurePath("/path/to/dir")

    def test_from_environ_failed(self) -> None:
        environ: Dict[str, str] = {}
        with pytest.raises(KeyError, match="NP_STORAGE_LOCAL_BASE_PATH"):
            StorageConfig.from_environ(environ)


class TestZipkinConfig:
    def test_create_zipkin_none(self) -> None:
        result = EnvironConfigFactory({}).create_zipkin()

        assert result is None

    def test_create_zipkin_default(self) -> None:
        env = {"NP_ZIPKIN_URL": "https://zipkin:9411"}
        result = EnvironConfigFactory(env).create_zipkin()

        assert result == ZipkinConfig(url=URL("https://zipkin:9411"))

    def test_create_zipkin_custom(self) -> None:
        env = {
            "NP_ZIPKIN_URL": "https://zipkin:9411",
            "NP_ZIPKIN_APP_NAME": "api",
            "NP_ZIPKIN_SAMPLE_RATE": "1",
        }
        result = EnvironConfigFactory(env).create_zipkin()

        assert result == ZipkinConfig(
            url=URL("https://zipkin:9411"), app_name="api", sample_rate=1
        )


class TestSentryConfig:
    def test_create_sentry_none(self) -> None:
        result = EnvironConfigFactory({}).create_sentry()

        assert result is None

    def test_create_sentry_default(self) -> None:
        env = {
            "NP_SENTRY_DSN": "https://sentry",
            "NP_SENTRY_CLUSTER_NAME": "test",
        }
        result = EnvironConfigFactory(env).create_sentry()

        assert result == SentryConfig(dsn=URL("https://sentry"), cluster_name="test")

    def test_create_sentry_custom(self) -> None:
        env = {
            "NP_SENTRY_DSN": "https://sentry",
            "NP_SENTRY_APP_NAME": "api",
            "NP_SENTRY_CLUSTER_NAME": "test",
            "NP_SENTRY_SAMPLE_RATE": "1",
        }
        result = EnvironConfigFactory(env).create_sentry()

        assert result == SentryConfig(
            dsn=URL("https://sentry"),
            app_name="api",
            cluster_name="test",
            sample_rate=1,
        )


class TestConfig:
    def test_from_environ_defaults(self) -> None:
        environ = {
            "NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir",
            "NP_STORAGE_AUTH_URL": "http://127.0.0.1/",
            "NP_STORAGE_AUTH_TOKEN": "hello-token",
            "NP_CLUSTER_NAME": "test-cluster",
        }
        config = Config.from_environ(environ)
        assert config.server.port == 8080
        assert config.server.keep_alive_timeout_s == 75
        assert config.storage.fs_local_base_path == PurePath("/path/to/dir")
        assert config.storage.fs_local_thread_pool_size == 100
        assert config.auth.server_endpoint_url == URL("http://127.0.0.1/")
        assert config.auth.service_token == "hello-token"
        assert config.zipkin is None
        assert config.sentry is None
        assert config.cluster_name == "test-cluster"
        assert config.cors.allowed_origins == ()

    def test_from_environ_custom(self) -> None:
        environ = {
            "NP_STORAGE_LOCAL_BASE_PATH": "/path/to/dir",
            "NP_STORAGE_LOCAL_THREAD_POOL_SIZE": "123",
            "NP_STORAGE_AUTH_URL": "http://127.0.0.1/",
            "NP_STORAGE_AUTH_TOKEN": "hello-token",
            "NP_CLUSTER_NAME": "test-cluster",
            "NP_STORAGE_API_KEEP_ALIVE_TIMEOUT": "900",
            "NP_CORS_ORIGINS": "https://domain1.com,http://do.main",
            "NP_ZIPKIN_URL": "https://zipkin.io:9411/",
            "NP_SENTRY_DSN": "https://sentry",
            "NP_SENTRY_CLUSTER_NAME": "test",
        }
        config = Config.from_environ(environ)
        assert config.server.port == 8080
        assert config.storage.fs_local_base_path == PurePath("/path/to/dir")
        assert config.storage.fs_local_thread_pool_size == 123
        assert config.auth.server_endpoint_url == URL("http://127.0.0.1/")
        assert config.auth.service_token == "hello-token"
        assert config.zipkin
        assert config.zipkin.url == URL("https://zipkin.io:9411/")
        assert config.sentry
        assert config.sentry.dsn == URL("https://sentry")
        assert config.sentry.cluster_name == "test"
        assert config.cluster_name == "test-cluster"
        assert config.cors.allowed_origins == ["https://domain1.com", "http://do.main"]
