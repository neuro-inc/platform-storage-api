from collections.abc import Iterator
from unittest import mock

import botocore.client
import botocore.session
import pytest
from botocore.exceptions import ClientError
from botocore.stub import Stubber

from platform_storage_api.config import S3Config
from platform_storage_api.s3 import create_s3_client
from platform_storage_api.s3_storage import (
    StorageMetricsAsyncS3Storage,
    StorageMetricsS3Storage,
)
from platform_storage_api.storage_usage import StorageUsage


@pytest.fixture
def s3_client() -> Iterator[botocore.client.BaseClient]:
    with create_s3_client(
        session=botocore.session.get_session(),
        config=S3Config(
            region="us-east-1",
            bucket_name="test-bucket",
            access_key_id="test-access-key",
            secret_access_key="test-secret-key",
        ),
    ) as client:
        yield client


def _create_client_error(status_code: int, code: str) -> ClientError:
    return ClientError(
        {"Error": {"Code": code}, "ResponseMetadata": {"HTTPStatusCode": status_code}},
        "GetObject",
    )


class TestS3Client:
    def test_timeouts(self, s3_client: botocore.client.BaseClient) -> None:
        assert s3_client.meta.config.connect_timeout == 1.5
        assert s3_client.meta.config.read_timeout == 3
        assert s3_client.meta.config.retries == {
            "mode": "standard",
            "total_max_attempts": 2,
        }


class TestStorageMetricsS3Storage:
    @pytest.fixture
    def storage(self, s3_client: botocore.client.BaseClient) -> StorageMetricsS3Storage:
        return StorageMetricsS3Storage(s3_client, bucket_name="test-bucket")

    def test_get_storage_usage__no_key(
        self, s3_client: botocore.client.BaseClient, storage: StorageMetricsS3Storage
    ) -> None:
        with Stubber(s3_client) as stubber:
            stubber.add_client_error(
                "get_object", service_error_code="NoSuchKey", http_status_code=404
            )

            storage_usage = storage.get_storage_usage()

        assert storage_usage == StorageUsage(projects=[])

    def test_get_storage_usage__access_denied(
        self, s3_client: botocore.client.BaseClient, storage: StorageMetricsS3Storage
    ) -> None:
        with Stubber(s3_client) as stubber:
            stubber.add_client_error(
                "get_object", service_error_code="AccessDenied", http_status_code=403
            )

            with pytest.raises(ClientError, match="AccessDenied"):
                storage.get_storage_usage()


class TestStorageMetricsAsyncS3Storage:
    async def test_get_storage_usage__no_key(self) -> None:
        s3_client = mock.AsyncMock()
        s3_client.get_object.side_effect = _create_client_error(404, "NoSuchKey")
        storage = StorageMetricsAsyncS3Storage(s3_client, bucket_name="test-bucket")

        storage_usage = await storage.get_storage_usage()

        assert storage_usage == StorageUsage(projects=[])

    async def test_get_storage_usage__access_denied(self) -> None:
        s3_client = mock.AsyncMock()
        s3_client.get_object.side_effect = _create_client_error(403, "AccessDenied")
        storage = StorageMetricsAsyncS3Storage(s3_client, bucket_name="test-bucket")

        with pytest.raises(ClientError, match="AccessDenied"):
            await storage.get_storage_usage()
