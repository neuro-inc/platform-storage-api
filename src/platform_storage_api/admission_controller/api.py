import base64
import dataclasses
import json
import logging
from enum import Enum
from typing import Any, Optional
from uuid import uuid4

from aiohttp import web

from platform_storage_api.admission_controller.app_keys import VOLUME_RESOLVER_KEY
from platform_storage_api.admission_controller.volume_resolver import (
    KubeVolumeResolver,
    VolumeResolverError,
)
from platform_storage_api.config import Config


logger = logging.getLogger(__name__)


ANNOTATION_APOLO_INJECT_STORAGE = "platform.apolo.us/injectStorage"

LABEL_APOLO_ORG_NAME = "platform.apolo.us/org"
LABEL_APOLO_PROJECT_NAME = "platform.apolo.us/project"
LABEL_PLATFORM_STORAGE_POD = "platform-storage"

INJECTED_VOLUME_NAME_PREFIX = "storage-auto-injected-volume"


def create_injection_volume_name() -> str:
    """Creates a random volume name"""
    return f"{INJECTED_VOLUME_NAME_PREFIX}-{uuid4().hex}"


class AdmissionReviewPatchType(str, Enum):
    JSON = "JSONPatch"


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

    def to_response(self) -> web.Response:
        return web.json_response(self.to_dict())


class AdmissionControllerApi:

    def __init__(
        self,
        app: web.Application,
        config: Config
    ) -> None:

        self._app = app
        self._config = config

    @property
    def _volume_resolver(self) -> KubeVolumeResolver:
        return self._app[VOLUME_RESOLVER_KEY]

    def register(self, app: web.Application) -> None:
        app.add_routes([
            web.post("/mutate", self.handle_post_mutate)
        ])

    async def handle_post_mutate(self, request: web.Request) -> Any:
        logger.info("mutate call")
        payload: dict[str, Any] = await request.json()

        uid = payload["request"]["uid"]
        admission_review = AdmissionReviewResponse(uid=uid)

        pod = payload["request"]["object"]
        kind = pod.get("kind")

        if kind != "Pod":
            # not a pod creation request. early-exit
            return admission_review.to_response()

        metadata = pod.get("metadata", {})
        labels = metadata.get("labels")

        # let's check if this is a new platform storage POD
        if (
            labels.get("app") == LABEL_PLATFORM_STORAGE_POD and
            labels.get("service") == LABEL_PLATFORM_STORAGE_POD
        ):
            return await self._handle_new_platform_storage_pod(
                pod=pod,
                admission_review=admission_review,
            )

        annotations = metadata.get("annotations", {})
        raw_injection_spec = annotations.get(ANNOTATION_APOLO_INJECT_STORAGE)

        if not raw_injection_spec:
            # a pod does not request storage. we can do early-exit here
            logger.info("POD won't be mutated because doesnt define proper annotations")
            return admission_review.to_response()

        return await self._handle_injection(
            pod=pod,
            raw_injection_spec=raw_injection_spec,
            admission_review=admission_review,
        )

    async def _handle_new_platform_storage_pod(
        self,
        pod: dict[str, Any],
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        await self._volume_resolver.refresh_internal_state(pod=pod)
        return admission_review.to_response()

    async def _handle_injection(
        self,
        pod: dict[str, Any],
        raw_injection_spec: str,
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        pod_spec = pod["spec"]
        containers = pod_spec.get("containers") or []

        if not containers:
            logger.info("POD won't be mutated because doesnt define containers")
            # pod does not define any containers. we can exit
            return admission_review.to_response()

        logger.info("Going to inject volumes")
        try:
            injection_spec = json.loads(raw_injection_spec)
        except ValueError:
            logger.info("Invalid injection spec. Denying the request")
            admission_review.allowed = False
            return admission_review.to_response()

        # let's ensure POD has volumes
        if "volumes" not in pod_spec:
            admission_review.add_patch(
                path="/spec/volumes",
                value=[]
            )

        # and ensure that each container has a volume mounts
        for idx, container in enumerate(containers):
            if "volumeMounts" not in container:
                admission_review.add_patch(
                    path=f"/spec/containers/{idx}/volumeMounts",
                    value=[]
                )

        for mount_path, storage_path in injection_spec.items():
            # now let's try to resolve a path which POD wants to mount
            try:
                volume_spec = await self._volume_resolver.resolve_to_mount_volume(
                    path=storage_path
                )
            except VolumeResolverError:
                # report an error and disallow spawning a POD
                logger.exception("Unable to resolve a volume for a provided path")
                admission_review.allowed = False
                return admission_review.to_response()

            future_volume_name = create_injection_volume_name()

            # add a volume host path
            admission_review.add_patch(
                path="/spec/volumes/-",
                value={
                    "name": future_volume_name,
                    **volume_spec.to_kube(),
                }
            )

            # add a volumeMount with mount path for all the POD containers
            for container_idx in range(len(containers)):
                admission_review.add_patch(
                    path=f"/spec/containers/{container_idx}/volumeMounts/-",
                    value={
                        "name": future_volume_name,
                        "mountPath": mount_path,
                    }
                )

        return admission_review.to_response()
