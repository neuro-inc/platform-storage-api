import os
import tempfile
from collections.abc import Iterator
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest
from apolo_kube_client import (
    V1Container,
    V1HostPathVolumeSource,
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
)
from platform_storage_api.config import AdmissionControllerConfig
from platform_storage_api.fs.local import FileSystem
from platform_storage_api.storage import (
    SingleStoragePathResolver,
    Storage,
    StoragePathResolver,
)


@pytest.fixture
def local_mount_path() -> Iterator[str]:
    with tempfile.TemporaryDirectory() as d:
        yield os.path.realpath(d)


@pytest.fixture
def volume_path() -> str:
    return "/var/exports"


@pytest.fixture
def volume_name() -> str:
    return "volume-name"


@pytest.fixture
def pod_spec_with_nfs(
    local_mount_path: str,
    volume_name: str,
) -> V1Pod:
    """
    A pod with mounted NFS volume.
    Simulates a POD spec of the webhook service itself.
    """
    return V1Pod(
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="test",
                    volume_mounts=[
                        V1VolumeMount(
                            name=volume_name,
                            mount_path=local_mount_path,
                        ),
                    ],
                )
            ],
            volumes=[
                V1Volume(
                    name=volume_name,
                    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                        claim_name="pvc-claim-name",
                    ),
                ),
            ],
        )
    )


@pytest.fixture
def pod_spec_with_host_path(
    local_mount_path: str,
    volume_name: str,
    volume_path: str,
) -> V1Pod:
    """
    A pod with mounted hostPath volume.
    Simulates a POD spec of the webhook service itself.
    """
    return V1Pod(
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="test",
                    volume_mounts=[
                        V1VolumeMount(
                            name=volume_name,
                            mount_path=local_mount_path,
                        ),
                    ],
                )
            ],
            volumes=[
                V1Volume(
                    name=volume_name, host_path=V1HostPathVolumeSource(path=volume_path)
                )
            ],
        )
    )


@pytest.fixture
def valid_pvc_spec(volume_name: str) -> V1PersistentVolumeClaim:
    return V1PersistentVolumeClaim(
        spec=V1PersistentVolumeClaimSpec(
            volume_name=volume_name,
        )
    )


@pytest.fixture
def valid_pv_spec(
    volume_path: str,
) -> V1PersistentVolume:
    return V1PersistentVolume(
        spec=V1PersistentVolumeSpec(
            nfs=V1NFSVolumeSource(
                server="0.0.0.0",
                path=volume_path,
            )
        )
    )


@pytest.fixture
def kube_api() -> Mock:
    return Mock(spec=KubeApi)


@pytest.fixture
def kube_api_with_nfs(
    kube_api: Mock,
    pod_spec_with_nfs: dict[str, Any],
    valid_pv_spec: dict[str, Any],
    valid_pvc_spec: dict[str, Any],
) -> Mock:
    """
    A kube API used by a volume resolver,
    to basically resolve a future requests.
    Uses a POD definition with the mounted NFS volume.
    """
    kube_api.get_pod = AsyncMock(return_value=pod_spec_with_nfs)
    kube_api.get_pv = AsyncMock(return_value=valid_pv_spec)
    kube_api.get_pvc = AsyncMock(return_value=valid_pvc_spec)
    return kube_api


@pytest.fixture
def kube_api_with_host_path(
    kube_api: Mock,
    pod_spec_with_host_path: dict[str, Any],
    valid_pv_spec: dict[str, Any],
    valid_pvc_spec: dict[str, Any],
) -> Mock:
    """
    A kube API used by a volume resolver,
    to basically resolve a future requests.
    Uses a POD definition with the mounted hostPath volume.
    """
    kube_api.get_pod = AsyncMock(return_value=pod_spec_with_host_path)
    kube_api.get_pv = AsyncMock(return_value=valid_pv_spec)
    kube_api.get_pvc = AsyncMock(return_value=valid_pvc_spec)
    return kube_api


@pytest.fixture
def path_resolver(local_mount_path: str) -> StoragePathResolver:
    return SingleStoragePathResolver(base_path=local_mount_path)


@pytest.fixture
def config() -> AdmissionControllerConfig:
    return AdmissionControllerConfig(
        cert_secret_name="secret",
    )


@pytest.fixture
def storage(path_resolver: StoragePathResolver, local_fs: FileSystem) -> Storage:
    return Storage(path_resolver, local_fs)


@pytest.fixture
def volume_resolver_with_nfs(
    kube_api_with_nfs: Mock,
    path_resolver: StoragePathResolver,
    config: AdmissionControllerConfig,
) -> KubeVolumeResolver:
    return KubeVolumeResolver(
        kube_api=kube_api_with_nfs,
        path_resolver=path_resolver,
        admission_controller_config=config,
    )


@pytest.fixture
def volume_resolver_with_host_path(
    kube_api_with_host_path: Mock,
    path_resolver: StoragePathResolver,
    config: AdmissionControllerConfig,
) -> KubeVolumeResolver:
    return KubeVolumeResolver(
        kube_api=kube_api_with_host_path,
        path_resolver=path_resolver,
        admission_controller_config=config,
    )
