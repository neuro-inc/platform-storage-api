import os
from collections.abc import AsyncIterator
from pathlib import Path, PurePath
from unittest import mock

import pytest
from aioresponses import aioresponses
from neuro_admin_client import AdminClient
from yarl import URL

from platform_storage_api.config import (
    Config,
    KubeConfig,
    PlatformConfig,
    S3Config,
    StorageConfig,
    StorageServerConfig,
)
from platform_storage_api.fs.local import FileSystem
from platform_storage_api.storage import SingleStoragePathResolver
from platform_storage_api.storage_usage import StorageUsage, StorageUsageService


@pytest.fixture
def config() -> Config:
    return Config(
        server=StorageServerConfig(),
        storage=StorageConfig(
            fs_local_base_path=PurePath(os.path.realpath("/tmp/np_storage"))
        ),
        platform=PlatformConfig(
            auth_url=URL("http://platform-auth"),
            admin_url=URL("http://platform-admin"),
            token="test-token",
            cluster_name="test-cluster",
        ),
        s3=S3Config(
            region="test-region",
            bucket_name="test-bucket",
        ),
        kube=KubeConfig(
            endpoint_url="https://localhost:8443",
        )
    )


class TestStorageUsage:
    @pytest.fixture
    async def admin_client(self) -> AsyncIterator[AdminClient]:
        async with AdminClient(
            base_url=URL("http://platform-admin/apis/admin/v1")
        ) as client:
            yield client

    @pytest.fixture
    def storage_usage_service(
        self,
        config: Config,
        local_fs: FileSystem,
        local_tmp_dir_path: Path,
        admin_client: AdminClient,
    ) -> StorageUsageService:
        return StorageUsageService(
            config=config,
            path_resolver=SingleStoragePathResolver(local_tmp_dir_path),
            fs=local_fs,
            admin_client=admin_client,
            storage_metrics_s3_storage=mock.AsyncMock(),
        )

    async def test_disk_usage(
        self,
        storage_usage_service: StorageUsageService,
        local_tmp_dir_path: Path,
        aiohttp_mock: aioresponses,
    ) -> None:
        aiohttp_mock.get(
            URL("http://platform-admin/apis/admin/v1/clusters/test-cluster/projects"),
            payload=[
                {
                    "name": "test-project-1",
                    "org_name": None,
                    "cluster_name": "test-cluster",
                    "default_role": "writer",
                    "is_default": False,
                },
                {
                    "name": "test-project-2",
                    "org_name": "test-org",
                    "cluster_name": "test-cluster",
                    "default_role": "writer",
                    "is_default": False,
                },
            ],
        )
        (local_tmp_dir_path / "test-project-1").mkdir()
        (local_tmp_dir_path / "test-org" / "test-project-2").mkdir(parents=True)

        storage_usage = await storage_usage_service.get_storage_usage()

        assert storage_usage == StorageUsage(
            projects=[
                StorageUsage.Project(project_name="test-project-1", used=mock.ANY),
                StorageUsage.Project(
                    org_name="test-org", project_name="test-project-2", used=mock.ANY
                ),
            ]
        )

    async def test_disk_usage__empty_storage(
        self, storage_usage_service: StorageUsageService, aiohttp_mock: aioresponses
    ) -> None:
        aiohttp_mock.get(
            URL("http://platform-admin/apis/admin/v1/clusters/test-cluster/projects"),
            payload=[
                {
                    "name": "test-project-1",
                    "org_name": None,
                    "cluster_name": "test-cluster",
                    "default_role": "writer",
                    "is_default": False,
                },
                {
                    "name": "test-project-2",
                    "org_name": "test-org",
                    "cluster_name": "test-cluster",
                    "default_role": "writer",
                    "is_default": False,
                },
            ],
        )

        storage_usage = await storage_usage_service.get_storage_usage()

        assert storage_usage == StorageUsage(projects=[])

    async def test_disk_usage__no_projects(
        self, storage_usage_service: StorageUsageService, aiohttp_mock: aioresponses
    ) -> None:
        aiohttp_mock.get(
            URL("http://platform-admin/apis/admin/v1/clusters/test-cluster/projects"),
            payload=[],
        )

        storage_usage = await storage_usage_service.get_storage_usage()

        assert storage_usage == StorageUsage(projects=[])
