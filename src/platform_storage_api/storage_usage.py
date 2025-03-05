from __future__ import annotations

import logging
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import timezone
from pathlib import PurePath
from typing import TYPE_CHECKING, Optional, Union

from neuro_admin_client import AdminClient
from prometheus_client.metrics_core import GaugeMetricFamily, Metric
from prometheus_client.registry import Collector

from .config import Config, S3Config
from .fs.local import FileStatusType, FileSystem
from .storage import StoragePathResolver


UTC = timezone.utc

if TYPE_CHECKING:
    from .s3_storage import StorageMetricsAsyncS3Storage, StorageMetricsS3Storage


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class StorageUsage:
    @dataclass(frozen=True)
    class Project:
        project_name: str
        used: int
        org_name: Optional[str] = None

    projects: Sequence[Project]


@dataclass(frozen=True)
class ProjectPath:
    project_name: str
    path: PurePath
    org_name: Optional[str] = None


class StorageUsageService:
    def __init__(
        self,
        config: Config,
        admin_client: AdminClient,
        storage_metrics_s3_storage: StorageMetricsAsyncS3Storage,
        fs: FileSystem,
        path_resolver: StoragePathResolver,
    ) -> None:
        self._config = config
        self._admin_client = admin_client
        self._storage_metrics_s3_storage = storage_metrics_s3_storage
        self._fs = fs
        self._path_resolver = path_resolver

    async def get_storage_usage(self) -> StorageUsage:
        org_project_paths = await self._get_project_paths()
        file_usages = await self._fs.disk_usage_by_file(
            *(p.path for p in org_project_paths)
        )
        file_sizes = {u.path: u.size for u in file_usages}
        return StorageUsage(
            projects=[
                StorageUsage.Project(
                    org_name=p.org_name,
                    project_name=p.project_name,
                    used=file_sizes[p.path],
                )
                for p in org_project_paths
            ],
        )

    async def _get_project_paths(self) -> list[ProjectPath]:
        projects_by_org = await self._get_projects_by_org()
        result = []
        for org_name, project_names in projects_by_org.items():
            org_path = await self._resolve_org_path(org_name)
            try:
                async with self._fs.iterstatus(org_path) as statuses:
                    async for status in statuses:
                        if status.type != FileStatusType.DIRECTORY:
                            continue
                        project_name = status.path.name
                        if project_name not in project_names:
                            continue
                        LOGGER.debug(
                            "Collecting storage usage for org %s, project %s",
                            org_name or "NO_ORG",
                            project_name,
                        )
                        result.append(
                            ProjectPath(
                                org_name=org_name,
                                project_name=project_name,
                                path=org_path / project_name,
                            )
                        )
            except FileNotFoundError:
                continue
        return result

    async def _get_projects_by_org(self) -> dict[Union[str, None], set[str]]:
        projects = await self._admin_client.list_projects(
            self._config.platform.cluster_name
        )
        projects_by_org: dict[Union[str, None], set[str]] = defaultdict(set)
        for project in projects:
            projects_by_org[project.org_name].add(project.name)
        return projects_by_org

    async def _resolve_org_path(self, org_name: Optional[str]) -> PurePath:
        if org_name:
            return await self._path_resolver.resolve_path(PurePath(f"/{org_name}"))
        return await self._path_resolver.resolve_base_path(
            PurePath(f"/{self._config.platform.cluster_name}")
        )

    async def upload_storage_usage(self) -> None:
        storage_usage = await self.get_storage_usage()
        await self._storage_metrics_s3_storage.put_storage_usage(storage_usage)


class StorageUsageCollector(Collector):
    def __init__(
        self, config: S3Config, storage_metrics_s3_storage: StorageMetricsS3Storage
    ) -> None:
        super().__init__()

        self._config = config
        self._storage_metrics_s3_storage = storage_metrics_s3_storage

    def collect(self) -> Iterable[Metric]:
        storage_usage = self._storage_metrics_s3_storage.get_storage_usage()
        metric_family = GaugeMetricFamily(
            "storage_used_bytes",
            "The amount of used storage space in bytes",
            labels=["org_name", "project_name"],
        )
        for project in storage_usage.projects:
            metric_family.add_metric(
                [project.org_name or "no_org", project.project_name], project.used
            )
        yield metric_family
