from pathlib import Path
from unittest import mock

from aioresponses import aioresponses

from platform_storage_api.config import Config
from platform_storage_api.s3_storage import StorageMetricsAsyncS3Storage
from platform_storage_api.storage_usage import StorageUsage
from platform_storage_api.worker import run


class TestUploadStorageUsage:
    async def test_run(
        self, config: Config, storage_metrics_s3_storage: StorageMetricsAsyncS3Storage
    ) -> None:
        Path(config.storage.fs_local_base_path / "test-project").mkdir()

        with aioresponses(
            passthrough=["http://0.0.0.0", "http://127.0.0.1"]
        ) as aiohttp_mock:
            aiohttp_mock.get(
                "http://platform-admin/apis/admin/v1/clusters"
                f"/{config.platform.cluster_name}/orgs",
                payload=[],
            )

            await run(config)

        storage_usage = await storage_metrics_s3_storage.get_storage_usage()

        assert storage_usage == StorageUsage(
            projects=[
                StorageUsage.Project(project_name="test-project", used=mock.ANY),
            ]
        )
