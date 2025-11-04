import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any
from uuid import uuid4

import pytest
from apolo_kube_client import (
    KubeClient,
    ResourceInvalid,
    V1Container,
    V1Job,
    V1JobSpec,
    V1ObjectMeta,
    V1Pod,
    V1PodSpec,
    V1PodTemplateSpec,
)

from platform_storage_api.admission_controller.api import (
    INJECTED_VOLUME_NAME_PREFIX,
    LABEL_APOLO_ORG_NAME,
    LABEL_APOLO_PROJECT_NAME,
)

# values are also defined at `tests/k8s/admission-controller-deployment.yaml`
ACTUAL_HOST_PATH = "/tmp/mnt"
ACTUAL_VOLUME_MOUNT_PATH = "/var/storage"


@asynccontextmanager
async def pod_cm(
    kube_client: KubeClient,
    annotations: dict[str, Any] | None = None,
    labels: dict[str, Any] | None = None,
) -> AsyncIterator[V1Pod]:
    """
    A context manager for creating the pod, returning the response,
    and deleting the POD at the end
    """
    pod_name = str(uuid4())
    payload = V1Pod(
        metadata=V1ObjectMeta(
            name=pod_name,
        ),
        spec=V1PodSpec(
            containers=[
                V1Container(
                    name="hello",
                    image="busybox",
                    command=["sh", "-c", "sleep 5"],
                )
            ]
        ),
    )
    if annotations is not None:
        payload.metadata.annotations = annotations

    if labels is not None:
        payload.metadata.labels = labels

    response = await kube_client.core_v1.pod.create(payload)

    # wait until a POD is running
    async with asyncio.timeout(60):
        while True:
            pod = await kube_client.core_v1.pod.get(pod_name)
            if pod.status.phase != "Running":
                await asyncio.sleep(0.1)
            else:
                break

    yield response

    await kube_client.core_v1.pod.delete(pod_name)


async def test__not_a_pod_will_be_ignored(
    kube_client: KubeClient,
) -> None:
    """
    Non-pod resource will be skipped, even if defines a necessary annotation
    """
    job_name = str(uuid4())

    payload = V1Job(
        metadata=V1ObjectMeta(
            name=job_name,
            annotations={
                "platform.apolo.us/inject-storage": "INVALID",
            },
        ),
        spec=V1JobSpec(
            template=V1PodTemplateSpec(
                spec=V1PodSpec(
                    restart_policy="Never",
                    containers=[
                        V1Container(
                            name="hello",
                            image="busybox",
                            command=["sh", "-c", "sleep 1"],
                        )
                    ],
                )
            )
        ),
    )

    response = await kube_client.batch_v1.job.create(payload)
    assert response.kind == "Job"

    await kube_client.batch_v1.job.delete(job_name)


async def test__pod_without_labels_will_be_ignored(
    kube_client: KubeClient,
) -> None:
    """
    Ensures that POD will be created if it doesn't define a necessary labels.
    """
    async with pod_cm(kube_client) as response:
        assert response.kind == "Pod"


async def test__pod_without_annotation_will_be_ignored(
    kube_client: KubeClient,
) -> None:
    """
    Ensures that POD will be created if it doesn't define a necessary annotation.
    """
    async with pod_cm(
        kube_client,
        labels={
            LABEL_APOLO_ORG_NAME: "org",
            LABEL_APOLO_PROJECT_NAME: "project",
        },
    ) as response:
        assert response.kind == "Pod"


async def test__pod_invalid_annotation_will_prohibit_pod_creation(
    kube_client: KubeClient,
) -> None:
    """
    POD with invalid value in the annotation will not be created,
    and an appropriate error will be returned.
    """
    with pytest.raises(ResourceInvalid) as e:
        async with pod_cm(
            kube_client,
            labels={
                LABEL_APOLO_ORG_NAME: "org",
                LABEL_APOLO_PROJECT_NAME: "project",
            },
            annotations={"platform.apolo.us/inject-storage": "invalid"},
        ):
            pass

    # the exception value is str(json) that was returned from k8s, apply substr search
    assert (
        'admission webhook \\"admission-controller.apolo.us\\" denied the request: '
        "injection spec is invalid"
    ) in str(e.value)


async def test_inject_single_storage(kube_client: KubeClient) -> None:
    """
    Creates a POD with the proper annotations,
    and expect that a storage will be mounted
    """
    org, project = "org", "proj"

    async with pod_cm(
        kube_client,
        annotations={
            "platform.apolo.us/inject-storage": json.dumps(
                [
                    {
                        "mount_path": "/var/pod_mount",
                        "storage_uri": f"storage://default/{org}/{project}",
                    }
                ]
            )
        },
        labels={
            LABEL_APOLO_ORG_NAME: org,
            LABEL_APOLO_PROJECT_NAME: project,
        },
    ) as response:
        spec = response.spec
        assert spec is not None
        volumes = spec.volumes
        container = spec.containers[0]

        # finds a hostPath volume
        actual_host_path_volume = next(
            iter(v for v in volumes if v.host_path is not None)
        )
        volume_name = actual_host_path_volume.name
        assert actual_host_path_volume.host_path is not None

        # ensures it has a proper name and a proper mount path
        assert volume_name.startswith(INJECTED_VOLUME_NAME_PREFIX)
        actual_host_path = actual_host_path_volume.host_path.path
        assert actual_host_path == ACTUAL_HOST_PATH

        actual_host_path_volume_mount = next(
            iter(v for v in container.volume_mounts if v.name == volume_name)
        )
        assert actual_host_path_volume_mount.name == volume_name
        assert actual_host_path_volume_mount.mount_path == "/var/pod_mount"
        assert actual_host_path_volume_mount.sub_path == f"{org}/{project}"


async def test_inject_multiple_storages(kube_client: KubeClient) -> None:
    """
    Creates a POD with the proper annotation, which includes two volume mounts
    """
    org, project = "org", "proj"

    async with pod_cm(
        kube_client,
        annotations={
            "platform.apolo.us/inject-storage": json.dumps(
                [
                    {
                        "mount_path": "/var/pod_mount",
                        "storage_uri": f"storage://default/{org}/{project}/1",
                    },
                    {
                        "mount_path": "/var/pod_mount_2",
                        "storage_uri": f"storage://default/{org}/{project}/2",
                    },
                ]
            ),
        },
        labels={
            LABEL_APOLO_ORG_NAME: org,
            LABEL_APOLO_PROJECT_NAME: project,
        },
    ) as response:
        spec = response.spec
        assert spec is not None
        container = spec.containers[0]

        volumes = [v for v in spec.volumes if v.host_path is not None]

        assert len(volumes) == 1

        for volume in volumes:
            assert volume.host_path is not None
            assert volume.name.startswith(INJECTED_VOLUME_NAME_PREFIX)
            assert volume.host_path.path == ACTUAL_HOST_PATH

        volume_names = {v.name for v in volumes}
        volume_mounts = [v for v in container.volume_mounts if v.name in volume_names]

        assert len(volume_mounts) == 2

        for volume_mount in volume_mounts:
            assert volume_mount.mount_path.startswith("/var/pod_mount")
            assert volume_mount.sub_path is not None
            assert volume_mount.sub_path.startswith(f"{org}/{project}")
