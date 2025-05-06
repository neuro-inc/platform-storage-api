import logging
from typing import Any
from uuid import uuid4

from aiohttp import web

from platform_storage_api.admission_controller.app_keys import (
    STORAGE_KEY,
    VOLUME_RESOLVER_KEY,
)
from platform_storage_api.admission_controller.schema import (
    AdmissionReviewResponse,
    InjectionSchema,
    MountMode,
    MountSchema,
)
from platform_storage_api.admission_controller.volume_resolver import (
    KubeVolumeResolver,
    VolumeResolverError,
)
from platform_storage_api.storage import Storage


logger = logging.getLogger(__name__)

ANNOTATION_APOLO_INJECT_STORAGE = "platform.apolo.us/inject-storage"

LABEL_APOLO_ORG_NAME = "platform.apolo.us/org"
LABEL_APOLO_PROJECT_NAME = "platform.apolo.us/project"
LABEL_PLATFORM_STORAGE_POD = "platform-storage"

INJECTED_VOLUME_NAME_PREFIX = "storage-auto-injected-volume"


class AdmissionControllerError(Exception):
    """Base admission controller error"""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message


class ForbiddenError(AdmissionControllerError):
    def __init__(self, message: str):
        super().__init__(status_code=403, message=message)


class ValidationError(AdmissionControllerError):
    """Unable to prepare a proper injection spec"""

    def __init__(self, message: str):
        super().__init__(status_code=422, message=message)


class MutationError(AdmissionControllerError):
    """Unable to mutate"""

    def __init__(self, message: str):
        super().__init__(status_code=400, message=message)


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

    @property
    def _storage(self) -> Storage:
        return self._app[STORAGE_KEY]

    def register(self, app: web.Application) -> None:
        app.add_routes(
            [
                web.post("/mutate", self.handle_post_mutate),
            ]
        )

    async def handle_post_mutate(self, request: web.Request) -> Any:
        logger.info("mutate call")
        payload: dict[str, Any] = await request.json()
        uid = payload["request"]["uid"]
        pod = payload["request"]["object"]
        metadata = pod.get("metadata", {}) or {}
        annotations = metadata.get("annotations", {}) or {}
        labels = metadata.get("labels", {}) or {}

        admission_review = AdmissionReviewResponse(uid=uid)

        if not self._should_mutate(pod, labels=labels, annotations=annotations):
            return admission_review.allow()

        try:
            spec = pod["spec"]
            injection_spec = await self._prepare(
                labels=labels,
                annotations=annotations,
            )
            # enrichment of `admission_review` happens inside
            await self._mutate(
                pod_spec=spec,
                injection_spec=injection_spec,
                admission_review=admission_review,
            )
        except AdmissionControllerError as e:
            return admission_review.decline(
                status_code=e.status_code,
                message=e.message,
            )
        except Exception:
            return admission_review.decline(
                status_code=400,
                message="storage injector unhandled error",
            )
        else:
            return admission_review.allow()

    @staticmethod
    def _should_mutate(
        pod: dict[str, Any],
        labels: dict[str, str],
        annotations: dict[str, str],
    ) -> bool:
        """
        Returns a boolean indicating whether a resource
        should actually be mutated at all.
        """
        kind = pod.get("kind")
        if kind != "Pod":
            return False

        # ensure labels are present
        if LABEL_APOLO_ORG_NAME not in labels:
            logger.info("Pod is not ready, missing label %s", LABEL_APOLO_ORG_NAME)
            return False
        if LABEL_APOLO_PROJECT_NAME not in labels:
            logger.info("Pod is not ready, missing label %s", LABEL_APOLO_PROJECT_NAME)
            return False

        if ANNOTATION_APOLO_INJECT_STORAGE not in annotations:
            # a pod does not request storage. we can do early-exit here
            logger.info(
                "POD won't be mutated, "
                "because doesn't define a storage-request annotation"
            )
            return False

        pod_spec = pod["spec"]

        containers = pod_spec.get("containers") or []
        if not containers:
            logger.info("POD won't be mutated because doesnt define containers")
            return False

        return True

    async def _prepare(
        self,
        labels: dict[str, Any],
        annotations: dict[str, Any],
    ) -> list[MountSchema]:
        """
        Prepares a POD mutation.
        Ensures that all the annotations and labels are actually correct.
        Ensures that the requested path actually exists.
        Can decline a POD-creation request.
        """
        logger.info("preparing for storage injection")
        raw_injection_spec = annotations[ANNOTATION_APOLO_INJECT_STORAGE]

        try:
            injection_spec = InjectionSchema.validate_json(raw_injection_spec)
        except Exception as e:
            error_message = "injection spec is invalid"
            logger.exception(error_message)
            raise ValidationError(error_message) from e

        expected_org = labels[LABEL_APOLO_ORG_NAME]
        expected_project = labels[LABEL_APOLO_PROJECT_NAME]

        for injection_schema in injection_spec:
            if injection_schema.org != expected_org:
                error_message = f"org mismatch: `{injection_schema.org}`"
                logger.error(error_message)
                raise ForbiddenError(error_message)

            if injection_schema.project != expected_project:
                error_message = f"project mismatch: `{injection_schema.project}`"
                logger.error(error_message)
                raise ForbiddenError(error_message)

        # here we are already confident that paths are valid,
        # so in addition, we try to create them
        for injection_schema in injection_spec:
            await self._storage.mkdir(injection_schema.storage_path)

        return injection_spec

    async def _mutate(
        self,
        pod_spec: dict[str, Any],
        injection_spec: list[MountSchema],
        admission_review: AdmissionReviewResponse,
    ) -> web.Response:
        """
        Performs mutation and enriches an `admission_review` object
        with various patch operations.
        Raises an error if mutation is impossible.
        """
        containers = pod_spec.get("containers") or []
        logger.info("Injecting volumes")

        # let's ensure POD has volumes
        if "volumes" not in pod_spec:
            admission_review.add_patch(
                path="/spec/volumes",
                value=[],
            )

        # and ensure that each container has a volume mounts
        for idx, container in enumerate(containers):
            if "volumeMounts" not in container:
                admission_review.add_patch(
                    path=f"/spec/containers/{idx}/volumeMounts",
                    value=[],
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
            except VolumeResolverError as e:
                error_message = "Unable to resolve a volume for a provided path"
                # report an error and disallow spawning a POD
                logger.exception(error_message)
                raise MutationError(error_message) from e

            future_volume_name = create_injection_volume_name()

            # add a volume host path
            admission_review.add_patch(
                path="/spec/volumes/-",
                value={
                    "name": future_volume_name,
                    **volume_spec.to_kube(),
                },
            )

            # add a volumeMount with mount path for all the POD containers
            for container_idx in range(len(containers)):
                patch_value: dict[str, str | bool] = {
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
