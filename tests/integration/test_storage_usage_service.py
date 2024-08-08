from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest import mock

import pytest
from aioresponses import aioresponses

from platform_storage_api.s3_storage import StorageMetricsAsyncS3Storage
from platform_storage_api.storage_usage import StorageUsage, StorageUsageService


@pytest.fixture()
def aiohttp_mock() -> Iterator[aioresponses]:
    with aioresponses(passthrough=["http://0.0.0.0", "http://127.0.0.1"]) as mocked:
        yield mocked


class TestStorageUsageService:
    async def test_upload_storage_usage(
        self,
        aiohttp_mock: aioresponses,
        storage_usage_service: StorageUsageService,
        storage_metrics_s3_storage: StorageMetricsAsyncS3Storage,
        cluster_name: str,
        local_tmp_dir_path: Path,
    ) -> None:
        aiohttp_mock.get(
            f"http://platform-admin/apis/admin/v1/clusters/{cluster_name}/projects",
            payload=[
                {
                    "name": "test-project",
                    "org_name": None,
                    "cluster_name": cluster_name,
                    "default_role": "writer",
                    "is_default": False,
                },
            ],
        )

        (local_tmp_dir_path / "test-project").mkdir()

        await storage_usage_service.upload_storage_usage()

        storage_usage = await storage_metrics_s3_storage.get_storage_usage()

        assert storage_usage == StorageUsage(
            projects=[
                StorageUsage.Project(project_name="test-project", used=mock.ANY),
            ]
        )

    async def test_upload_storage_usage__multiple_times(
        self,
        aiohttp_mock: aioresponses,
        storage_usage_service: StorageUsageService,
        storage_metrics_s3_storage: StorageMetricsAsyncS3Storage,
        cluster_name: str,
        local_tmp_dir_path: Path,
    ) -> None:
        aiohttp_mock.get(
            f"http://platform-admin/apis/admin/v1/clusters/{cluster_name}/projects",
            payload=[
                {
                    "name": "test-project",
                    "org_name": None,
                    "cluster_name": cluster_name,
                    "default_role": "writer",
                    "is_default": False,
                },
            ],
        )

        (local_tmp_dir_path / "test-project").mkdir()

        await storage_usage_service.upload_storage_usage()

        await storage_metrics_s3_storage.get_storage_usage()
        storage_usage = await storage_metrics_s3_storage.get_storage_usage()

        assert storage_usage == StorageUsage(
            projects=[
                StorageUsage.Project(project_name="test-project", used=mock.ANY),
            ]
        )
