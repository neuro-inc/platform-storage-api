import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, Optional
from uuid import uuid4

import pytest
from apolo_kube_client.client import KubeClient
from apolo_kube_client.errors import ResourceInvalid

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
    annotations: Optional[dict[str, Any]] = None,
    labels: Optional[dict[str, Any]] = None,
) -> AsyncIterator[dict[str, Any]]:
    """
    A context manager for creating the pod, returning the response,
    and deleting the POD at the end
    """
    pod_name = str(uuid4())
    payload = {
        "kind": "Pod",
        "apiVersion": "v1",
        "metadata": {
            "name": pod_name,
        },
        "spec": {
            "containers": [
                {
                    "name": "hello",
                    "image": "busybox",
                    "command": ["sh", "-c", "sleep 1"],
                }
            ]
        },
    }
    if annotations is not None:
        payload["metadata"]["annotations"] = annotations  # type: ignore[index]

    if labels is not None:
        payload["metadata"]["labels"] = labels  # type: ignore[index]

    url = f"{kube_client.namespace_url}/pods"
    response = await kube_client.post(
        url=url,
        json=payload,
    )

    yield response

    await kube_client.delete(f"{url}/{pod_name}")


async def test__not_a_pod_will_be_ignored(
    kube_client: KubeClient,
) -> None:
    """
    Non-pod resource will be skipped, even if defines a necessary annotation
    """
    job_name = str(uuid4())

    payload = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name,
            "annotations": {
                "platform.apolo.us/inject-storage": "INVALID",
            },
        },
        "spec": {
            "template": {
                "spec": {
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "hello",
                            "image": "busybox",
                            "command": ["sh", "-c", "sleep 1"],
                        }
                    ],
                }
            }
        },
    }

    url = (
        f"{kube_client._base_url}/apis/batch/v1/namespaces/{kube_client.namespace}/jobs"
    )
    response = await kube_client.post(
        url,
        json=payload,
    )
    assert response["kind"] == "Job"

    await kube_client.delete(f"{url}/{job_name}")


async def test__pod_without_labels_will_be_ignored(
    kube_client: KubeClient,
) -> None:
    """
    Ensures that POD will be created if it doesn't define a necessary labels.
    """
    async with pod_cm(kube_client) as response:
        assert response["kind"] == "Pod"


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
        assert response["kind"] == "Pod"


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

    assert str(e.value) == (
        'admission webhook "admission-controller.apolo.us" denied the request: '
        "injection spec is invalid"
    )


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
        spec = response["spec"]
        volumes = spec["volumes"]
        container = spec["containers"][0]

        # finds a hostPath volume
        actual_host_path_volume = next(iter(v for v in volumes if "hostPath" in v))
        volume_name = actual_host_path_volume["name"]

        # ensures it has a proper name and a proper mount path
        assert volume_name.startswith(INJECTED_VOLUME_NAME_PREFIX)
        actual_host_path = actual_host_path_volume["hostPath"]["path"]
        expected_host_path = f"{ACTUAL_HOST_PATH}/{org}/{project}"
        assert actual_host_path == expected_host_path

        actual_host_path_volume_mount = next(
            iter(v for v in container["volumeMounts"] if v["name"] == volume_name)
        )
        assert actual_host_path_volume_mount["name"] == volume_name
        assert actual_host_path_volume_mount["mountPath"] == "/var/pod_mount"


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
        spec = response["spec"]
        volumes = spec["volumes"]
        container = spec["containers"][0]

        volume_by_name = {v["name"]: v for v in volumes if "hostPath" in v}
        volume_mount_by_name = {
            v["name"]: v
            for v in container["volumeMounts"]
            if v["name"] in volume_by_name
        }

        assert len(volume_by_name) == 2
        assert len(volume_mount_by_name) == 2

        for volume_name, volume in volume_by_name.items():
            volume_mount = volume_mount_by_name[volume_name]
            assert volume_name.startswith(INJECTED_VOLUME_NAME_PREFIX)
            assert volume_name == volume_mount["name"]
            assert volume["hostPath"]["path"].startswith(ACTUAL_HOST_PATH)
            assert volume_mount["mountPath"].startswith("/var/pod_mount")
