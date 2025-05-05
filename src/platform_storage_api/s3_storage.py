import functools
from collections.abc import Sequence

import aiobotocore.client
import botocore.client
import pydantic
from botocore.exceptions import ClientError

from .storage_usage import StorageUsage

_S3_STORAGE_USAGE_KEY = "storage_usage.json"


@pydantic.dataclasses.dataclass(frozen=True)
class _StorageUsage:
    @pydantic.dataclasses.dataclass(frozen=True)
    class Project:
        project_name: str
        used: int
        org_name: str | None = None

    projects: Sequence[Project]


class _PayloadFactory:
    @classmethod
    def create_storage_usage(cls, storage_usage: StorageUsage) -> bytes:
        data = _StorageUsage(
            projects=[
                _StorageUsage.Project(
                    org_name=p.org_name,
                    project_name=p.project_name,
                    used=p.used,
                )
                for p in storage_usage.projects
            ]
        )
        return pydantic.TypeAdapter(_StorageUsage).dump_json(data)


class _EntityFactory:
    @classmethod
    def create_storage_usage(cls, payload: str | bytes) -> StorageUsage:
        storage_usage = pydantic.TypeAdapter(_StorageUsage).validate_json(payload)
        return StorageUsage(
            projects=[
                StorageUsage.Project(
                    org_name=p.org_name,
                    project_name=p.project_name,
                    used=p.used,
                )
                for p in storage_usage.projects
            ]
        )


class StorageMetricsS3Storage:
    def __init__(
        self,
        s3_client: botocore.client.BaseClient,
        bucket_name: str,
        key_prefix: str = "",
    ) -> None:
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._key_prefix = key_prefix

    @property
    def _key(self) -> str:
        return f"{self._key_prefix}{_S3_STORAGE_USAGE_KEY}"

    def get_storage_usage(self) -> StorageUsage:
        try:
            response = self._s3_client.get_object(
                Bucket=self._bucket_name, Key=self._key
            )
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return StorageUsage(projects=[])
        with response["Body"]:
            payload = response["Body"].read()
        return _EntityFactory.create_storage_usage(payload)


class StorageMetricsAsyncS3Storage:
    def __init__(
        self,
        s3_client: aiobotocore.client.AioBaseClient,
        bucket_name: str,
        key_prefix: str = "",
    ) -> None:
        self._s3_client = s3_client
        self._bucket_name = bucket_name
        self._key_prefix = key_prefix

    @property
    def _key(self) -> str:
        return f"{self._key_prefix}{_S3_STORAGE_USAGE_KEY}"

    async def put_storage_usage(self, storage_usage: StorageUsage) -> None:
        data = _PayloadFactory.create_storage_usage(storage_usage)
        put_object = functools.partial(
            self._s3_client.put_object,
            Bucket=self._bucket_name,
            Key=self._key,
            Body=data,
        )
        try:
            await put_object()
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
                raise
            await self._s3_client.create_bucket(Bucket=self._bucket_name)
            await put_object()

    async def get_storage_usage(self) -> StorageUsage:
        try:
            response = await self._s3_client.get_object(
                Bucket=self._bucket_name, Key=self._key
            )
        except ClientError as err:
            if err.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return StorageUsage(projects=[])
        async with response["Body"]:
            payload = await response["Body"].read()
        return _EntityFactory.create_storage_usage(payload)
