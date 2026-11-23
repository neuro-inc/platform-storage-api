from collections.abc import Iterator
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from apolo_kube_client import (
    ResourceNotFound,
    V1AWSElasticBlockStoreVolumeSource,
    V1Container,
    V1NFSVolumeSource,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeClaimVolumeSource,
    V1PersistentVolumeSpec,
    V1Pod,
    V1PodSpec,
    V1Volume,
    V1VolumeMount,
)

from platform_storage_api.admission_controller.volume_resolver import (
    KubeApi,
    KubeVolumeResolver,
    NfsVolumeSpec,
    VolumeBackend,
    VolumeResolverError,
)
from platform_storage_api.config import AdmissionControllerConfig
from platform_storage_api.fs.local import FileSystem
from platform_storage_api.storage import MultipleStoragePathResolver


@pytest.fixture
def logger_mock() -> Iterator[Mock]:
    with patch(
        "platform_storage_api.admission_controller.volume_resolver.logger"
    ) as logger_mock:
        yield logger_mock


async def test__no_mounted_pod_raises_error(
    volume_resolver_with_nfs: KubeVolumeResolver,
    kube_api_with_nfs: Mock,
) -> None:
    """
    Volume resolver is unable to get a spec of itself
    """
    kube_api_with_nfs.get_pod = AsyncMock(side_effect=ResourceNotFound)
    with pytest.raises(VolumeResolverError):
        async with volume_resolver_with_nfs:
            pass


async def test__pod_without_volumes_will_raise_an_error(
    volume_resolver_with_nfs: KubeVolumeResolver,
    kube_api_with_nfs: Mock,
) -> None:
    """
    No volumes mounted to a pod
    """
    pod_spec = V1Pod(
        spec=V1PodSpec(
            containers=[V1Container(name="test", volume_mounts=[])], volumes=[]
        )
    )
    kube_api_with_nfs.get_pod = AsyncMock(return_value=pod_spec)
    with pytest.raises(VolumeResolverError) as e:
        async with volume_resolver_with_nfs:
            pass

    expected_err = "No eligible volumes are mounted to this pod"
    assert expected_err == str(e.value)


async def test__unsupported_volume_type_will_raise_an_error(
    volume_resolver_with_nfs: KubeVolumeResolver,
    kube_api_with_nfs: Mock,
    volume_name: str,
    logger_mock: Mock,
) -> None:
    """
    Mounted volume has an unsupported backend (e.g., currently, not an NFS)
    """
    kube_api_with_nfs.get_pvc = AsyncMock(
        return_value=V1PersistentVolumeClaim(
            spec=V1PersistentVolumeClaimSpec(
                volume_name=volume_name,
            )
        )
    )
    kube_api_with_nfs.get_pv = AsyncMock(
        return_value=V1PersistentVolume(
            spec=V1PersistentVolumeSpec(
                aws_elastic_block_store=V1AWSElasticBlockStoreVolumeSource(
                    volume_id="volume-id"
                )
            )
        )
    )

    with pytest.raises(VolumeResolverError) as e:
        async with volume_resolver_with_nfs:
            pass

    expected_err = "No eligible volumes are mounted to this pod"
    assert expected_err == str(e.value)

    assert (
        logger_mock.info.call_args[0][0]
        == "volume did not produce any valid mapping: %s"
    )
    assert logger_mock.info.call_args[0][1].name == volume_name


async def test__resolve_successfully(
    volume_resolver_with_nfs: KubeVolumeResolver,
    kube_api_with_nfs: Mock,
    logger_mock: Mock,
    volume_path: str,
) -> None:
    """
    Successful resolving. The nfs path is scoped to the project and the
    sub path holds only the remainder.
    """
    storage_path = "/org/project/some/path"

    async with volume_resolver_with_nfs as vr:
        volume_mount = await vr.resolve_volume_mount(storage_path)

        assert volume_mount.sub_path == "some/path"
        assert volume_mount.volume.backend == VolumeBackend.NFS
        spec = cast(NfsVolumeSpec, volume_mount.volume.spec)
        assert spec.server == "0.0.0.0"
        assert spec.path == f"{volume_path}/org/project"


async def test__resolve_project_root_has_empty_sub_path(
    volume_resolver_with_nfs: KubeVolumeResolver,
    kube_api_with_nfs: Mock,
    volume_path: str,
) -> None:
    """
    A mount of the project root itself produces an empty sub path.
    """
    async with volume_resolver_with_nfs as vr:
        volume_mount = await vr.resolve_volume_mount("/org/project")

        assert volume_mount.sub_path == ""
        spec = cast(NfsVolumeSpec, volume_mount.volume.spec)
        assert spec.path == f"{volume_path}/org/project"


async def test__resolve_two_projects_of_one_org(
    volume_resolver_with_nfs: KubeVolumeResolver,
    kube_api_with_nfs: Mock,
    volume_path: str,
) -> None:
    """
    Two projects of one org resolve to two distinct nfs paths.
    """
    async with volume_resolver_with_nfs as vr:
        first = await vr.resolve_volume_mount("/org/project-a/data")
        second = await vr.resolve_volume_mount("/org/project-b/data")

        assert cast(NfsVolumeSpec, first.volume.spec).path == (
            f"{volume_path}/org/project-a"
        )
        assert cast(NfsVolumeSpec, second.volume.spec).path == (
            f"{volume_path}/org/project-b"
        )
        assert first.sub_path == "data"
        assert second.sub_path == "data"


def _multi_storage_resolver(
    local_mount_path: str,
    local_fs: FileSystem,
    admission_controller_config: AdmissionControllerConfig,
    exports: dict[str, str],
) -> KubeVolumeResolver:
    mounts = {
        f"storage-{index}": (f"{local_mount_path}{path}", export)
        for index, (path, export) in enumerate(exports.items())
    }
    for mount_path, _ in mounts.values():
        Path(mount_path).mkdir(parents=True, exist_ok=True)
    pod = V1Pod(
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="test",
                    volume_mounts=[
                        V1VolumeMount(name=name, mount_path=mount_path)
                        for name, (mount_path, _) in mounts.items()
                    ],
                )
            ],
            volumes=[
                V1Volume(
                    name=name,
                    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                        claim_name=name
                    ),
                )
                for name in mounts
            ],
        )
    )
    kube_api = Mock(spec=KubeApi)
    kube_api.get_pod = AsyncMock(return_value=pod)
    kube_api.get_pvc = AsyncMock(
        side_effect=lambda pvc_name, namespace=None: V1PersistentVolumeClaim(
            spec=V1PersistentVolumeClaimSpec(volume_name=pvc_name)
        )
    )
    kube_api.get_pv = AsyncMock(
        side_effect=lambda pv_name: V1PersistentVolume(
            spec=V1PersistentVolumeSpec(
                nfs=V1NFSVolumeSource(server="0.0.0.0", path=mounts[pv_name][1])
            )
        )
    )
    return KubeVolumeResolver(
        kube_api=kube_api,
        path_resolver=MultipleStoragePathResolver(
            local_fs, local_mount_path, f"{local_mount_path}/default"
        ),
        admission_controller_config=admission_controller_config,
    )


@pytest.mark.parametrize(
    ("exports", "storage_path", "expected_path", "expected_sub_path"),
    [
        (
            {"/default": "/default-export", "/org/proj/data": "/data-export"},
            "/org/proj/data/run",
            "/data-export",
            "run",
        ),
        (
            {"/default": "/default-export", "/org/proj/data": "/data-export"},
            "/org/proj/data",
            "/data-export",
            "",
        ),
        (
            {"/default": "/default-export", "/org/proj/data": "/data-export"},
            "/org2/proj/run",
            "/default-export/org2/proj",
            "run",
        ),
        (
            {
                "/default": "/default-export",
                "/org": "/org-export",
                "/org/proj/data": "/data-export",
            },
            "/org/proj/data/run",
            "/data-export",
            "run",
        ),
        (
            {
                "/default": "/default-export",
                "/org": "/org-export",
                "/org/proj/data": "/data-export",
            },
            "/org/proj/other",
            "/org-export/proj",
            "other",
        ),
        (
            {
                "/default": "/default-export",
                "/org": "/org-export",
                "/org/proj/data": "/data-export",
            },
            "/org/proj",
            "/org-export/proj",
            "",
        ),
    ],
)
async def test__resolve_multi_storage(
    local_mount_path: str,
    local_fs: FileSystem,
    admission_controller_config: AdmissionControllerConfig,
    exports: dict[str, str],
    storage_path: str,
    expected_path: str,
    expected_sub_path: str,
) -> None:
    resolver = _multi_storage_resolver(
        local_mount_path, local_fs, admission_controller_config, exports
    )
    async with resolver as vr:
        volume_mount = await vr.resolve_volume_mount(storage_path)

    assert cast(NfsVolumeSpec, volume_mount.volume.spec).path == expected_path
    assert volume_mount.sub_path == expected_sub_path


async def test__resolve_multi_storage_resolves_the_base_once(
    local_mount_path: str,
    local_fs: FileSystem,
    admission_controller_config: AdmissionControllerConfig,
) -> None:
    resolver = _multi_storage_resolver(
        local_mount_path,
        local_fs,
        admission_controller_config,
        {"/default": "/default-export", "/org/proj/data": "/data-export"},
    )
    async with resolver as vr:
        with patch.object(
            local_fs, "exists", AsyncMock(side_effect=[False, True, True, True])
        ):
            volume_mount = await vr.resolve_volume_mount("/org/proj/data")

    spec = cast(NfsVolumeSpec, volume_mount.volume.spec)
    assert spec.path == "/default-export/org/proj"
    assert volume_mount.sub_path == "data"
