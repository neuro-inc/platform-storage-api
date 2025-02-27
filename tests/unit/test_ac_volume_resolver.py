from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, Mock, patch

import pytest
from apolo_kube_client.errors import ResourceNotFound

from platform_storage_api.admission_controller.schema import SCHEMA_STORAGE
from platform_storage_api.admission_controller.volume_resolver import (
    KubeVolumeResolver,
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
    volume_resolver: KubeVolumeResolver,
    valid_kube_api: Mock,
) -> None:
    """
    Volume resolver is unable to get a spec of itself
    """
    valid_kube_api.get_pod = AsyncMock(side_effect=ResourceNotFound)
    with pytest.raises(VolumeResolverError):
        async with volume_resolver:
            pass


async def test__pod_without_volumes_will_raise_an_error(
    volume_resolver: KubeVolumeResolver,
    valid_kube_api: Mock,
) -> None:
    """
    No volumes mounted to a pod
    """
    pod_spec: dict[str, Any] = {
        "spec": {
            "containers": [
                {
                    "volumeMounts": []
                }
            ],
            "volumes": []
        }
    }
    valid_kube_api.get_pod = AsyncMock(return_value=pod_spec)
    with pytest.raises(VolumeResolverError) as e:
        async with volume_resolver:
            pass

    expected_err = "No eligible volumes are mounted to this pod"
    assert expected_err == str(e.value)


async def test__unsupported_volume_type_will_raise_an_error(
    volume_resolver: KubeVolumeResolver,
    valid_kube_api: Mock,
    volume_name: str,
    logger_mock: Mock,
) -> None:
    """
    Mounted volume has an unsupported backend (e.g., currently, not an NFS)
    """
    non_supported_backend = "non-supported-backend"

    valid_kube_api.get_pvc = AsyncMock(
        return_value={
            "spec": {
                "volumeName": volume_name,
            }
        }
    )
    valid_kube_api.get_pv = AsyncMock(
        return_value={
            "spec": {
                non_supported_backend: {}
            }
        }
    )

    with pytest.raises(VolumeResolverError) as e:
        async with volume_resolver:
            pass

    expected_err = "No eligible volumes are mounted to this pod"
    assert expected_err == str(e.value)

    assert logger_mock.info.call_args[0][0] == \
           "storage `%s` doesn't define supported volume backends"
    assert logger_mock.info.call_args[0][1] == volume_name


async def test__resolve_successfully(
    volume_resolver: KubeVolumeResolver,
    valid_kube_api: Mock,
    logger_mock: Mock,
    nfs_path: str,
) -> None:
    """
    Successful resolving
    """
    storage_path = "org/project"

    async with volume_resolver as vr:
        volume = await vr.resolve_to_mount_volume(f"{SCHEMA_STORAGE}{storage_path}")

        assert volume.backend == VolumeBackend.NFS
        assert volume.spec.server == "0.0.0.0"
        assert volume.spec.path == f"{nfs_path}/{storage_path}"
