import logging
from typing import Any, Union
from uuid import uuid4

from aiohttp import web

from platform_storage_api.admission_controller.app_keys import VOLUME_RESOLVER_KEY
from platform_storage_api.admission_controller.schema import (
    AdmissionReviewResponse,
    InjectionSchema,
    MountMode,
)
from platform_storage_api.admission_controller.volume_resolver import (
    KubeVolumeResolver,
    VolumeResolverError,
)


logger = logging.getLogger(__name__)

ANNOTATION_APOLO_INJECT_STORAGE = "platform.apolo.us/inject-storage"

LABEL_APOLO_ORG_NAME = "platform.apolo.us/org"
LABEL_APOLO_PROJECT_NAME = "platform.apolo.us/project"
LABEL_PLATFORM_STORAGE_POD = "platform-storage"

INJECTED_VOLUME_NAME_PREFIX = "storage-auto-injected-volume"


def create_injection_volume_name() -> str:
    """Creates a random volume name"""
    return f"{INJECTED_VOLUME_NAME_PREFIX}-{str(uuid4())[:8]}"


class AdmissionControllerApi:

    def __init__(
        self,
        app: web.Application,
    ) -> None:

        self._app = app

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
            return admission_review.allow()

        metadata = pod.get("metadata", {})

        annotations = metadata.get("annotations", {})

        if ANNOTATION_APOLO_INJECT_STORAGE not in annotations:
            # a pod does not request storage. we can do early-exit here
            logger.info("POD won't be mutated because doesnt define proper annotations")
            return admission_review.allow()

        pod_spec = pod["spec"]

        containers = pod_spec.get("containers") or []
        if not containers:
            logger.info("POD won't be mutated because doesnt define containers")
            # pod does not define any containers. we can exit
            return admission_review.allow()

        raw_injection_spec = annotations[ANNOTATION_APOLO_INJECT_STORAGE]

        return await self._handle_injection(
            pod_spec=pod_spec,
            raw_injection_spec=raw_injection_spec,
            admission_review=admission_review,
        )

    async def _handle_injection(
        self,
        pod_spec: dict[str, Any],
        raw_injection_spec: str,
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        containers = pod_spec.get("containers") or []

        logger.info("Going to inject volumes")
        try:
            injection_spec = InjectionSchema.validate_json(raw_injection_spec)
        except Exception:
            error_message = "injection spec is invalid"
            logger.exception(error_message)
            return admission_review.decline(status_code=422, message=error_message)

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

        for injection_schema in injection_spec:
            mount_path = injection_schema.mount_path
            storage_path = injection_schema.storage_path
            mount_mode = injection_schema.mount_mode

            # now let's try to resolve a path which POD wants to mount
            try:
                volume_spec = await self._volume_resolver.resolve_to_mount_volume(
                    path=storage_path
                )
            except VolumeResolverError:
                error_message = "Unable to resolve a volume for a provided path"
                # report an error and disallow spawning a POD
                logger.exception(error_message)
                return admission_review.decline(status_code=400, message=error_message)

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
                patch_value: dict[str, Union[str, bool]] = {
                    "name": future_volume_name,
                    "mountPath": mount_path,
                }
                if mount_mode is MountMode.READ_ONLY:
                    patch_value["readOnly"] = True

                admission_review.add_patch(
                    path=f"/spec/containers/{container_idx}/volumeMounts/-",
                    value=patch_value,
                )

        return admission_review.allow()
