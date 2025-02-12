import base64
import dataclasses
import json
import logging
from enum import Enum
from typing import Any, Optional

from aiohttp import web

from platform_storage_api.admission_controller.app_keys import VOLUME_RESOLVER_KEY
from platform_storage_api.admission_controller.volume_resolver import (
    KubeVolumeResolver,
    VolumeResolverError,
)
from platform_storage_api.config import Config


logger = logging.getLogger(__name__)


LABEL_APOLO_ORG_NAME = "platform.apolo.us/org"
LABEL_APOLO_PROJECT_NAME = "platform.apolo.us/project"
LABEL_APOLO_STORAGE_MOUNT_PATH = "platform.apolo.us/storage/mountPath"
LABEL_APOLO_STORAGE_HOST_PATH = "platform.apolo.us/storage/hostPath"

POD_INJECTED_VOLUME_NAME = "storage-auto-injected-volume"


class AdmissionReviewPatchType(str, Enum):
    JSON = "JSONPatch"


class AdmissionControllerApi:
    def __init__(self, app: web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    @property
    def _volume_resolver(self) -> KubeVolumeResolver:
        return self._app[VOLUME_RESOLVER_KEY]

    def register(self, app: web.Application) -> None:
        app.add_routes([web.post("/mutate", self.handle_post_mutate)])

    async def handle_post_mutate(self, request: web.Request) -> Any:
        payload: dict[str, Any] = await request.json()

        uid = payload["request"]["uid"]
        response = AdmissionReviewResponse(uid=uid)

        obj = payload["request"]["object"]
        kind = obj.get("kind")

        if kind != "Pod":
            # not a pod creation request. early-exit
            return web.json_response(response.to_dict())

        metadata = obj.get("metadata", {})
        annotations = metadata.get("annotations", {})

        mount_path_value = annotations.get(LABEL_APOLO_STORAGE_MOUNT_PATH)
        host_path_value = annotations.get(LABEL_APOLO_STORAGE_HOST_PATH)

        if not (mount_path_value and host_path_value):
            # a pod does not request storage. we can do early-exit here
            return web.json_response(response.to_dict())

        pod_spec = obj["spec"]
        containers = pod_spec.get("containers") or []

        if not containers:
            # pod does not define any containers. we can exit
            return web.json_response(response.to_dict())

        # now let's try to resolve a path which POD wants to mount
        try:
            volume_spec = await self._volume_resolver.resolve_to_mount_volume(
                path=host_path_value
            )
        except VolumeResolverError:
            # report an error and disallow spawning a POD
            logger.exception("unable to resolve a volume for a provided path")
            response.allowed = False
            return web.json_response(response.to_dict())

        # ensure volumes
        if "volumes" not in pod_spec:
            response.add_patch(
                path="/spec/volumes",
                value=[]
            )

        # add a volume host path
        response.add_patch(
            path="/spec/volumes/-",
            value={
                "name": POD_INJECTED_VOLUME_NAME,
                **volume_spec,
            }
        )

        # add a volumeMount with mount path for all the POD containers
        for idx, container in enumerate(containers):
            if "volumeMounts" not in container:
                response.add_patch(
                    path=f"/spec/containers/{idx}/volumeMounts",
                    value=[]
                )

            response.add_patch(
                path=f"/spec/containers/{idx}/volumeMounts/-",
                value={
                    "name": POD_INJECTED_VOLUME_NAME,
                    "mountPath": mount_path_value,
                }
            )

        return web.json_response(response.to_dict())


@dataclasses.dataclass
class AdmissionReviewResponse:
    uid: str
    allowed: bool = True
    patch: Optional[list[dict[str, Any]]] = None
    patch_type: AdmissionReviewPatchType = AdmissionReviewPatchType.JSON

    def add_patch(self, path: str, value: Any) -> None:
        if self.patch is None:
            self.patch = []

        self.patch.append({
            "op": "add",
            "path": path,
            "value": value,
        })

    def to_dict(self) -> dict[str, Any]:
        patch: Optional[str] = None

        if self.patch is not None:
            # convert patch changes to a b64
            dumped = json.dumps(self.patch).encode()
            patch = base64.b64encode(dumped).decode()

        return {
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "response": {
                "uid": self.uid,
                "allowed": self.allowed,
                "patch": patch,
                "patchType": AdmissionReviewPatchType.JSON.value
            }
        }
