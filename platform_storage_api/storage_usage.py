from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timezone
from pathlib import PurePath
from typing import Optional

from neuro_admin_client import AdminClient

from .fs.local import FileStatusType, FileSystem
from .storage import StoragePathResolver

UTC = timezone.utc


@dataclass(frozen=True)
class StorageUsage:
    @dataclass(frozen=True)
    class Project:
        project_name: str
        used: int
        org_name: Optional[str] = None

    projects: Sequence[Project]


@dataclass(frozen=True)
class OrgProjectPath:
    project_name: str
    path: PurePath
    org_name: Optional[str] = None


class StorageUsageService:
    def __init__(
        self,
        path_resolver: StoragePathResolver,
        fs: FileSystem,
        admin_client: AdminClient,
        cluster_name: str,
    ) -> None:
        self._fs = fs
        self._path_resolver = path_resolver
        self._admin_client = admin_client
        self._cluster_name = cluster_name

    async def get_storage_usage(self) -> StorageUsage:
        org_project_paths = await self._get_org_project_paths()
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

    async def _get_org_project_paths(self) -> list[OrgProjectPath]:
        org_clusters = await self._admin_client.list_org_clusters(self._cluster_name)
        org_names = {org_cluster.org_name for org_cluster in org_clusters}
        result = await self._get_no_org_project_paths(org_names)
        for org_cluster in org_clusters:
            org_path = await self._path_resolver.resolve_path(
                PurePath(f"/{org_cluster.org_name}")
            )
            try:
                async with self._fs.iterstatus(org_path) as statuses:
                    async for status in statuses:
                        if status.type != FileStatusType.DIRECTORY:
                            continue
                        result.append(
                            OrgProjectPath(
                                org_name=org_cluster.org_name,
                                project_name=status.path.name,
                                path=org_path / status.path.name,
                            )
                        )
            except FileNotFoundError:
                continue
        return result

    async def _get_no_org_project_paths(
        self, org_names: set[str]
    ) -> list[OrgProjectPath]:
        result = []
        no_org_path = await self._path_resolver.resolve_base_path(
            PurePath(f"/{self._cluster_name}")
        )
        async with self._fs.iterstatus(no_org_path) as statuses:
            async for status in statuses:
                if (
                    status.type != FileStatusType.DIRECTORY
                    or status.path.name in org_names
                ):
                    continue
                result.append(
                    OrgProjectPath(
                        project_name=status.path.name,
                        path=no_org_path / status.path.name,
                    )
                )
        return result
