import base64
import json
from collections.abc import AsyncIterator, Iterator
from contextlib import AsyncExitStack, asynccontextmanager
from itertools import count
from typing import Any, Optional
from unittest.mock import Mock, patch
from uuid import uuid4

import aiohttp
import pytest
from aiohttp import ClientResponse, ClientSession, web

from platform_storage_api.admission_controller.api import (
    ANNOTATION_APOLO_INJECT_STORAGE,
    LABEL_APOLO_ORG_NAME,
    LABEL_APOLO_PROJECT_NAME,
    AdmissionControllerApi,
)
from platform_storage_api.admission_controller.app_keys import (
    STORAGE_KEY,
    VOLUME_RESOLVER_KEY,
)
from platform_storage_api.admission_controller.volume_resolver import KubeVolumeResolver
from platform_storage_api.storage import Storage
from tests.integration.conftest import ApiConfig


@asynccontextmanager
async def _api(
    volume_resolver: KubeVolumeResolver,
    storage: Storage,
) -> AsyncIterator[ApiConfig]:
    """
    Runs an admission-controller webhook API
    """
    app = web.Application()

    async def _init_app(app: web.Application) -> AsyncIterator[None]:
        async with AsyncExitStack() as exit_stack:
            vr = await exit_stack.enter_async_context(
                volume_resolver
            )
            app[VOLUME_RESOLVER_KEY] = vr
            app[STORAGE_KEY] = storage
            yield

    app.cleanup_ctx.append(_init_app)
    admission_controller_app = web.Application()
    admission_controller_api = AdmissionControllerApi(app)
    admission_controller_api.register(admission_controller_app)
    app.add_subapp("/admission-controller", admission_controller_app)
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    api_config = ApiConfig(host="0.0.0.0", port=8080)
    site = aiohttp.web.TCPSite(runner, api_config.host, api_config.port)
    await site.start()
    yield api_config
    await runner.cleanup()


@pytest.fixture
async def nfs_api(
    volume_resolver_with_nfs: KubeVolumeResolver,  # prepared volume resolver
    storage: Storage,
) -> AsyncIterator[ApiConfig]:
    async with _api(volume_resolver=volume_resolver_with_nfs, storage=storage) as api:
        yield api


@pytest.fixture
async def host_path_api(
    volume_resolver_with_host_path: KubeVolumeResolver,  # prepared volume resolver
    storage: Storage,
) -> AsyncIterator[ApiConfig]:
    async with _api(
        volume_resolver=volume_resolver_with_host_path,
        storage=storage
    ) as api:
        yield api


# todo: we need this bug to fixed to use a dynamic fixtures parametrizing.
#  https://github.com/pytest-dev/pytest-asyncio/issues/112
class TestMutateApi:
    http: ClientSession

    @pytest.fixture(autouse=True)
    def setup(
        self,
        client: aiohttp.ClientSession,
    ) -> None:
        self.http = client

    @pytest.fixture
    def logger_mock(self) -> Iterator[Mock]:
        with patch("platform_storage_api.admission_controller.api.logger") as mock:
            yield mock

    @pytest.fixture
    def uuid_mock(self) -> Iterator[None]:
        """
        We need to mock a UUID because it's used to generate a volume mount name.
        """
        id_generator = count(start=1, step=1)
        with patch("platform_storage_api.admission_controller.api.uuid4") as uuid_mock:
            uuid_mock.side_effect = lambda: str(next(id_generator))
            yield

    async def test__nfs__not_a_pod(self, nfs_api: ApiConfig) -> None:
        return await self._test__not_a_pod(api=nfs_api)

    async def test__host_path__not_a_pod(self, host_path_api: ApiConfig) -> None:
        return await self._test__not_a_pod(api=host_path_api)

    async def _test__not_a_pod(
        self,
        api: ApiConfig,
    ) -> None:
        """
        Ensure a webhook will react only to a PODs requests.
        """
        url = (
            f"http://{api.host}:{api.port}/admission-controller/mutate"
        )

        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "not-a-pod"
                    }
                }
            }
        )
        await self._ensure_allowed(response)

    async def test__nfs__pod_without_injection_spec(
        self,
        nfs_api: ApiConfig,
    ) -> None:
        return await self._test__pod_without_injection_spec(api=nfs_api)

    async def test__host_path__pod_without_injection_spec(
        self,
        host_path_api: ApiConfig,
    ) -> None:
        return await self._test__pod_without_injection_spec(api=host_path_api)

    async def _test__pod_without_injection_spec(
        self,
        api: ApiConfig,
    ) -> None:
        """
        No injection spec provided - this should be allowed.
        """
        url = (
            f"http://{api.host}:{api.port}/admission-controller/mutate"
        )
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {}
                    }
                }
            }
        )
        await self._ensure_allowed(response)

    async def test__nfs__pod_defines_no_containers(
        self,
        nfs_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        return await self._test__pod_defines_no_containers(
            api=nfs_api, logger_mock=logger_mock
        )

    async def test__host_path__pod_defines_no_containers(
        self,
        host_path_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        return await self._test__pod_defines_no_containers(
            api=host_path_api, logger_mock=logger_mock
        )

    async def _test__pod_defines_no_containers(
        self,
        api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        """
        Ensure POD won't be mutated if it doesn't define any containers
        """
        url = f"http://{api.host}:{api.port}/admission-controller/mutate"
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: "spec"
                            }
                        },
                        "spec": {
                            "containers": [],
                        }
                    }
                }
            }
        )
        await self._ensure_allowed(response)
        assert logger_mock.info.call_args[0][0] == \
               "POD won't be mutated because doesnt define containers"

    async def test__nfs__pod_invalid_injection_spec(
        self,
        nfs_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_invalid_injection_spec(nfs_api, logger_mock)

    async def test__host_path__pod_invalid_injection_spec(
        self,
        host_path_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_invalid_injection_spec(host_path_api, logger_mock)

    async def _test__pod_invalid_injection_spec(
        self,
        api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        """
        Ensure we'll disallow a creation of a POD if it defines the
        annotation, but such an annotation couldn't be properly validated
        """
        url = (
            f"http://{api.host}:{api.port}/admission-controller/mutate"
        )
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: "spec"
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "container"
                                }
                            ],
                        }
                    }
                }
            }
        )
        expected_message = "injection spec is invalid"
        await self._ensure_not_allowed(
            response,
            code=422,
            expected_message=expected_message
        )
        assert logger_mock.exception.call_args[0][0] == "injection spec is invalid"

    async def test__nfs__pod_no_org_label(
        self,
        nfs_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_no_org_label(nfs_api, logger_mock)

    async def test__host_path__pod_no_org_label(
        self,
        host_path_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_no_org_label(host_path_api, logger_mock)

    async def _test__pod_no_org_label(
        self,
        api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        """
        Ensure we'll disallow a creation of a POD if it defines the
        annotation, but doesn't have an org label
        """
        url = (
            f"http://{api.host}:{api.port}/admission-controller/mutate"
        )
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri": "storage://default/org/proj",
                                        "mount_mode": "rw"
                                    }
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "container"
                                }
                            ],
                        }
                    }
                }
            }
        )
        await self._ensure_not_allowed(
            response,
            code=422,
            expected_message=f"Missing label {LABEL_APOLO_ORG_NAME}"
        )

    async def test__nfs__pod_no_project_label(
        self,
        nfs_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_no_project_label(nfs_api, logger_mock)

    async def test__host_path__pod_no_project_label(
        self,
        host_path_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_no_project_label(host_path_api, logger_mock)

    async def _test__pod_no_project_label(
        self,
        api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        """
        Ensure we'll disallow a creation of a POD if it defines the
        annotation, but doesn't have a project label
        """
        url = (
            f"http://{api.host}:{api.port}/admission-controller/mutate"
        )
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "labels": {
                                LABEL_APOLO_ORG_NAME: "org"
                            },
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri": "storage://default/org/proj",
                                        "mount_mode": "rw"
                                    }
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "container"
                                }
                            ],
                        }
                    }
                }
            }
        )
        await self._ensure_not_allowed(
            response,
            code=422,
            expected_message=f"Missing label {LABEL_APOLO_PROJECT_NAME}"
        )

    async def test__nfs__pod_org_label_mismatch(
        self,
        nfs_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_org_label_mismatch(nfs_api, logger_mock)

    async def test__host_path__pod_org_label_mismatch(
        self,
        host_path_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_org_label_mismatch(host_path_api, logger_mock)

    async def _test__pod_org_label_mismatch(
        self,
        api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        """
        Ensure we'll disallow a creation of a POD if the org/proj pair
        doesn't correlate with the mounted path
        """
        url = (
            f"http://{api.host}:{api.port}/admission-controller/mutate"
        )
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "labels": {
                                LABEL_APOLO_ORG_NAME: "invalid-org",
                                LABEL_APOLO_PROJECT_NAME: "invalid-proj"
                            },
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri": "storage://default/org/proj",
                                        "mount_mode": "rw"
                                    }
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "container"
                                }
                            ],
                        }
                    }
                }
            }
        )
        await self._ensure_not_allowed(
            response,
            code=403,
            expected_message="org mismatch: `org`"
        )

    async def test__nfs__pod_project_label_mismatch(
        self,
        nfs_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_org_label_mismatch(nfs_api, logger_mock)

    async def test__host_path__pod_project_label_mismatch(
        self,
        host_path_api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        await self._test__pod_org_label_mismatch(host_path_api, logger_mock)

    async def _test__pod_project_label_mismatch(
        self,
        api: ApiConfig,
        logger_mock: Mock,
    ) -> None:
        """
        Ensure we'll disallow a creation of a POD if the org/proj pair
        doesn't correlate with the mounted path
        """
        url = (
            f"http://{api.host}:{api.port}/admission-controller/mutate"
        )
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "labels": {
                                LABEL_APOLO_ORG_NAME: "org",
                                LABEL_APOLO_PROJECT_NAME: "invalid-proj"
                            },
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri": "storage://default/org/proj",
                                        "mount_mode": "rw"
                                    }
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "container"
                                }
                            ],
                        }
                    }
                }
            }
        )
        await self._ensure_not_allowed(
            response,
            code=403,
            expected_message="project mismatch: `invalid-proj`"
        )

    async def test__nfs__ensure_volumes_will_be_added(
        self,
        nfs_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__ensure_volumes_will_be_added(
            nfs_api,
            uuid_mock,
            volume_expected={
                'nfs': {'path': '/var/exports/org/proj', 'server': '0.0.0.0'}
            }
        )

    async def test__host_path__ensure_volumes_will_be_added(
        self,
        host_path_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__ensure_volumes_will_be_added(
            host_path_api,
            uuid_mock,
            volume_expected={'hostPath': {'path': '/var/exports/org/proj', 'type': ''}},
        )

    async def _test__ensure_volumes_will_be_added(
        self,
        api: ApiConfig,
        uuid_mock: Mock,
        volume_expected: dict[str, Any],
    ) -> None:
        """
        If container doesn't define any volumes,
        we expect that a webhook will add them via an `add` operations
        """
        url = f"http://{api.host}:{api.port}/admission-controller/mutate"
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "labels": {
                                LABEL_APOLO_ORG_NAME: "org",
                                LABEL_APOLO_PROJECT_NAME: "proj",
                            },
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri": "storage://default/org/proj",
                                        "mount_mode": "rw"
                                    }
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {"name": "container"}
                            ],
                        }
                    }
                }
            }
        )
        data = await self._ensure_allowed(response)
        expected_ops = [
            # add a blank volumes and a blank volume mounts for the first container.
            {"op": "add", "path": "/spec/volumes", "value": []},
            {'op': 'add', 'path': '/spec/containers/0/volumeMounts', 'value': []},

            # add an actual volumes and volumeMounts.
            {
                'op': 'add',
                'path': '/spec/volumes/-',
                'value': {
                    'name': 'storage-auto-injected-volume-1',
                    **volume_expected
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/0/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume',
                    'name': 'storage-auto-injected-volume-1'
                }
            }
        ]

        assert data["patch"] == expected_ops

    async def test__nfs__resolve_single_volume(
        self,
        nfs_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__resolve_single_volume(
            nfs_api,
            uuid_mock,
            volume_expected={
                'nfs': {'path': '/var/exports/org/proj', 'server': '0.0.0.0'}
            }
        )

    async def test__host_path__resolve_single_volume(
        self,
        host_path_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__resolve_single_volume(
            host_path_api,
            uuid_mock,
            volume_expected={'hostPath': {'path': '/var/exports/org/proj', 'type': ''}}
        )

    async def _test__resolve_single_volume(
        self,
        api: ApiConfig,
        uuid_mock: Mock,
        volume_expected: dict[str, Any],
    ) -> None:
        """
        Successful use-case.
        Pod already defines volumes and volume mounts,
        so we expect only two add ops just to add an injected volume
        """
        url = f"http://{api.host}:{api.port}/admission-controller/mutate"

        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "labels": {
                                LABEL_APOLO_ORG_NAME: "org",
                                LABEL_APOLO_PROJECT_NAME: "proj",
                            },
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri": "storage://default/org/proj",
                                        "mount_mode": "rw"
                                    }
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {
                                    "name": "container",
                                    "volumeMounts": []
                                }
                            ],
                            "volumes": [],
                        }
                    }
                }
            }
        )
        data = await self._ensure_allowed(response)
        expected_ops = [
            {
                'op': 'add',
                'path': '/spec/volumes/-',
                'value': {
                    'name': 'storage-auto-injected-volume-1',
                    **volume_expected,
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/0/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume',
                    'name': 'storage-auto-injected-volume-1'
                }
            }
        ]
        assert data["patch"] == expected_ops

    async def test__nfs__resolve_multiple_volumes(
        self,
        nfs_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__resolve_multiple_volumes(
            nfs_api,
            uuid_mock,
            first_volume_expected={
                'nfs': {'path': '/var/exports/org/proj/data1', 'server': '0.0.0.0'}
            },
            second_volume_expected={
                'nfs': {'path': '/var/exports/org/proj/data2', 'server': '0.0.0.0'}
            }
        )

    async def test__host_path__resolve_multiple_volumes(
        self,
        host_path_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__resolve_multiple_volumes(
            host_path_api,
            uuid_mock,
            first_volume_expected={
                'hostPath': {'path': '/var/exports/org/proj/data1', 'type': ''}
            },
            second_volume_expected={
                'hostPath': {'path': '/var/exports/org/proj/data2', 'type': ''}
            },
        )

    async def _test__resolve_multiple_volumes(
        self,
        api: ApiConfig,
        uuid_mock: Mock,
        first_volume_expected: dict[str, Any],
        second_volume_expected: dict[str, Any],
    ) -> None:
        """
        Adding two volumes to the pod.
        One if a read-write, while another one is a read-only
        """
        url = f"http://{api.host}:{api.port}/admission-controller/mutate"
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "labels": {
                                LABEL_APOLO_ORG_NAME: "org",
                                LABEL_APOLO_PROJECT_NAME: "proj",
                            },
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri": "storage://default/org/proj/data1",
                                        "mount_mode": "rw"
                                    },
                                    {
                                        "mount_path": "/var/mount-volume-2",
                                        "storage_uri":
                                            "storage://default/org/proj/data2",
                                        "mount_mode": "r"
                                    },
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {"name": "container"}
                            ],
                        }
                    }
                }
            }
        )

        data = await self._ensure_allowed(response)
        expected_ops = [

            # since pod doesn't define volumes and mounts,
            # we expect to see those ops here
            {"op": "add", "path": "/spec/volumes", "value": []},
            {'op': 'add', 'path': '/spec/containers/0/volumeMounts', 'value': []},

            {
                'op': 'add',
                'path': '/spec/volumes/-',
                'value': {
                    'name': 'storage-auto-injected-volume-1',
                    **first_volume_expected
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/0/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume',
                    'name': 'storage-auto-injected-volume-1'
                }
            },
            {
                'op': 'add',
                'path': '/spec/volumes/-',
                'value': {
                    'name': 'storage-auto-injected-volume-2',
                    **second_volume_expected,
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/0/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume-2',
                    'name': 'storage-auto-injected-volume-2',
                    'readOnly': True
                }
            }
        ]

        assert data["patch"] == expected_ops

    async def test__nfs__resolve_multiple_volumes_to_multiple_containers(
        self,
        nfs_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__resolve_multiple_volumes_to_multiple_containers(
            nfs_api,
            uuid_mock,
            first_volume_expected={
                'nfs': {'path': '/var/exports/org/proj/data1', 'server': '0.0.0.0'}
            },
            second_volume_expected={
                'nfs': {'path': '/var/exports/org/proj/data2', 'server': '0.0.0.0'}
            }
        )

    async def test__host_path__resolve_multiple_volumes_to_multiple_containers(
        self,
        host_path_api: ApiConfig,
        uuid_mock: Mock,
    ) -> None:
        return await self._test__resolve_multiple_volumes_to_multiple_containers(
            host_path_api,
            uuid_mock,
            first_volume_expected={
                'hostPath': {'path': '/var/exports/org/proj/data1', 'type': ''}
            },
            second_volume_expected={
                'hostPath': {'path': '/var/exports/org/proj/data2', 'type': ''}
            },
        )

    async def _test__resolve_multiple_volumes_to_multiple_containers(
        self,
        api: ApiConfig,
        uuid_mock: Mock,
        first_volume_expected: dict[str, Any],
        second_volume_expected: dict[str, Any],
    ) -> None:
        """
        Adding two volumes to the pod, which defines two containers.
        Volumes should be added to each container.
        """
        url = f"http://{api.host}:{api.port}/admission-controller/mutate"
        response = await self.http.post(
            url,
            json={
                "request": {
                    "uid": str(uuid4()),
                    "object": {
                        "kind": "Pod",
                        "metadata": {
                            "labels": {
                                LABEL_APOLO_ORG_NAME: "org",
                                LABEL_APOLO_PROJECT_NAME: "proj",
                            },
                            "annotations": {
                                ANNOTATION_APOLO_INJECT_STORAGE: json.dumps([
                                    {
                                        "mount_path": "/var/mount-volume",
                                        "storage_uri":
                                            "storage://default/org/proj/data1",
                                        "mount_mode": "rw"
                                    },
                                    {
                                        "mount_path": "/var/mount-volume-2",
                                        "storage_uri":
                                            "storage://default/org/proj/data2",
                                        "mount_mode": "r"
                                    },
                                ])
                            }
                        },
                        "spec": {
                            "containers": [
                                {"name": "container-1"},
                                {"name": "container-2"},
                            ],
                        }
                    }
                }
            }
        )

        data = await self._ensure_allowed(response)
        expected_ops = [

            # volume mounts should be added to both containers
            {"op": "add", "path": "/spec/volumes", "value": []},
            {'op': 'add', 'path': '/spec/containers/0/volumeMounts', 'value': []},
            {'op': 'add', 'path': '/spec/containers/1/volumeMounts', 'value': []},

            {
                'op': 'add',
                'path': '/spec/volumes/-',
                'value': {
                    'name': 'storage-auto-injected-volume-1',
                    **first_volume_expected
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/0/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume',
                    'name': 'storage-auto-injected-volume-1'
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/1/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume',
                    'name': 'storage-auto-injected-volume-1'
                }
            },
            {
                'op': 'add',
                'path': '/spec/volumes/-',
                'value': {
                    'name': 'storage-auto-injected-volume-2',
                    **second_volume_expected
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/0/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume-2',
                    'name': 'storage-auto-injected-volume-2',
                    'readOnly': True
                }
            },
            {
                'op': 'add',
                'path': '/spec/containers/1/volumeMounts/-',
                'value': {
                    'mountPath': '/var/mount-volume-2',
                    'name': 'storage-auto-injected-volume-2',
                    'readOnly': True
                }
            }
        ]

        assert data["patch"] == expected_ops

    async def _ensure_allowed(self, response: ClientResponse) -> dict[str, Any]:
        data = await self.__ensure_success_response(response)
        assert data["allowed"] is True
        return data

    async def _ensure_not_allowed(
        self,
        response: ClientResponse,
        code: int,
        expected_message: str,
    ) -> dict[str, Any]:
        data = await self.__ensure_success_response(response, code, expected_message)
        assert data["allowed"] is False
        return data

    @staticmethod
    async def __ensure_success_response(
        response: ClientResponse,
        code: Optional[int] = None,
        expected_message: Optional[str] = None,
    ) -> dict[str, Any]:
        assert response.status == 200
        data = await response.json()
        assert data["kind"] == "AdmissionReview"
        if code:
            assert data["response"]["status"]["code"] == code
        if expected_message:
            assert data["response"]["status"]["message"] == expected_message
        patch_data = data["response"].get("patch")
        if patch_data:
            data["response"]["patch"] = json.loads(base64.b64decode(patch_data))
        return data["response"]
