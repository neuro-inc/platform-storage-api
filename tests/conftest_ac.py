from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

from platform_storage_api.admission_controller.volume_resolver import (
    KubeApi,
    KubeVolumeResolver,
    VolumeBackend,
)
from platform_storage_api.config import AdmissionControllerConfig
from platform_storage_api.storage import SingleStoragePathResolver, StoragePathResolver


@pytest.fixture
def local_mount_path() -> str:
    return "/var/storage"


@pytest.fixture
def nfs_path() -> str:
    return "/var/exports"


@pytest.fixture
def volume_name() -> str:
    return "volume-name"


@pytest.fixture
def valid_pod_spec(
    local_mount_path: str,
    volume_name: str,
) -> dict[str, Any]:
    """
    A pod with mounted NFS volume.
    Simulates a POD spec of the webhook service itself.
    """
    return {
        "spec": {
            "containers": [
                {
                    "volumeMounts": [
                        {
                            "name": volume_name,
                            "mountPath": local_mount_path,
                        }
                    ]
                }
            ],
            "volumes": [
                {
                    "name": volume_name,
                    "persistentVolumeClaim": {
                        "claimName": "pvc-claim-name",
                    }
                }
            ]
        }
    }


@pytest.fixture
def valid_pvc_spec(volume_name: str) -> dict[str, Any]:
    return {
        "spec": {
            "volumeName": volume_name,
        }
    }


@pytest.fixture
def valid_pv_spec(
    nfs_path: str,
) -> dict[str, Any]:
    return {
        "spec": {
            VolumeBackend.NFS.value: {
                "server": "0.0.0.0",
                "path": nfs_path,
            }
        }
    }


@pytest.fixture
def kube_api() -> Mock:
    return Mock(spec=KubeApi)


@pytest.fixture
def valid_kube_api(
    kube_api: Mock,
    valid_pod_spec: dict[str, Any],
    valid_pv_spec: dict[str, Any],
    valid_pvc_spec: dict[str, Any],
) -> Mock:
    """
    A kube API used by a volume resolver,
    to basically resolve a future requests.
    """
    kube_api.get_pod = AsyncMock(return_value=valid_pod_spec)
    kube_api.get_pv = AsyncMock(return_value=valid_pv_spec)
    kube_api.get_pvc = AsyncMock(return_value=valid_pvc_spec)
    return kube_api


@pytest.fixture
def path_resolver(local_mount_path: str) -> StoragePathResolver:
    return SingleStoragePathResolver(base_path=local_mount_path)


@pytest.fixture
def config() -> AdmissionControllerConfig:
    return AdmissionControllerConfig(
        cert_secret_name='secret',
    )


@pytest.fixture
def volume_resolver(
    valid_kube_api: Mock,
    path_resolver: StoragePathResolver,
    config: AdmissionControllerConfig,
) -> KubeVolumeResolver:
    return KubeVolumeResolver(
        kube_api=valid_kube_api,
        path_resolver=path_resolver,
        admission_controller_config=config,
    )
