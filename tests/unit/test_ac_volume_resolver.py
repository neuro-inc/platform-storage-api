from collections.abc import Iterator
from typing import cast
from unittest.mock import AsyncMock, Mock, patch

import pytest
from apolo_kube_client import (
    ResourceNotFound,
    V1AWSElasticBlockStoreVolumeSource,
    V1Container,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimSpec,
    V1PersistentVolumeSpec,
    V1Pod,
    V1PodSpec,
)

from platform_storage_api.admission_controller.volume_resolver import (
    KubeVolumeResolver,
    NfsVolumeSpec,
    VolumeBackend,
    VolumeResolverError,
)


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
