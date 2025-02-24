import dataclasses
import logging
from enum import Enum
from pathlib import PurePath
from types import TracebackType
from typing import Any, Optional

from apolo_kube_client.client import KubeClient

from platform_storage_api.config import AdmissionControllerConfig
from platform_storage_api.storage import StoragePathResolver


logger = logging.getLogger(__name__)


class VolumeResolverError(Exception):
    ...


class VolumeBackend(str, Enum):
    """Supported volume backends"""
    NFS = "nfs"


@dataclasses.dataclass
class BaseVolumeSpec:

    def to_kube(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


@dataclasses.dataclass
class NfsVolumeSpec(BaseVolumeSpec):
    server: str
    path: str

    @classmethod
    def from_pv(cls, pv: dict[str, Any]) -> "NfsVolumeSpec":
        """
        Constructs a volume spec from a PV object
        """
        return cls(
            server=pv["spec"][VolumeBackend.NFS]["server"],
            path=pv["spec"][VolumeBackend.NFS]["path"]
        )


T_VolumeSpec = NfsVolumeSpec  # we'll add others later?


@dataclasses.dataclass
class KubeVolume:
    backend: VolumeBackend
    spec: T_VolumeSpec

    def to_kube(self) -> dict[str, Any]:
        return {
            self.backend.value: self.spec.to_kube()
        }


class KubeVolumeResolver:

    def __init__(
        self,
        kube_client: KubeClient,
        path_resolver: StoragePathResolver,
        admission_controller_config: AdmissionControllerConfig,
    ):
        self._kube = kube_client
        self._path_resolver = path_resolver
        self._config = admission_controller_config

        self._local_fs_prefix_to_kube_volume: dict[str, KubeVolume] = {}
        """will store a mapping, where a key is a local mounted path,
        and a value is a kube volume definition.
        """

    async def __aenter__(self) -> "KubeVolumeResolver":
        """
        Initialize the kube volume resolver.
        Gets the most fresh platform-storage POD and passes it to an
        internal state updater, so we can resolve the real mount paths.
        """
        logger.info("initializing volume resolver")
        namespace_url = self._kube.generate_namespace_url()

        # get all storage admission controllers PODs,
        # and choose the freshest one
        pods_response = await self._kube.get(
            f"{namespace_url}/pods",
            params={
                "labelSelector": f"app={self._config.service_name}",
            }
        )
        try:
            most_fresh_pod = next(
                iter(
                    sorted(
                        pods_response["items"],
                        key=lambda pod: pod["metadata"]["creationTimestamp"],
                        reverse=True,
                    )
                )
            )
        except StopIteration:
            raise VolumeResolverError() from None

        await self._refresh_internal_state(pod=most_fresh_pod)
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> bool:
        return exc_type is None

    async def _refresh_internal_state(
        self,
        pod: dict[str, Any],
    ) -> None:
        """
        Refreshes an internal mapping of volumes based on a provided POD.
        This method expects that the POD will have the most up-to-date mapping
        of volumes
        """
        logger.info("refreshing internal state")
        namespace_url = self._kube.generate_namespace_url()

        # internal storage name to a PV and PVC names
        storage_name_to_pvc: dict[str, str] = {}
        storage_name_to_pv: dict[str, str] = {}

        # storage name to a local mounted path
        storage_name_to_local_path: dict[str, str] = {}

        # go over volumes to identify linked PVCs
        for volume in pod["spec"]["volumes"]:
            pvc = volume.get("persistentVolumeClaim")
            if not pvc:
                continue
            storage_name, pvc_name = volume["name"], pvc["claimName"]
            storage_name_to_pvc[storage_name] = pvc_name

        # now let's go over containers and figure out mount paths for volumes
        for container in pod["spec"]["containers"]:
            for volume_mount in container["volumeMounts"]:
                volume_name = volume_mount["name"]
                if volume_name not in storage_name_to_pvc:
                    continue
                mount_path = volume_mount["mountPath"]
                storage_name_to_local_path[volume_name] = mount_path

        # get PVs by claim names
        for storage_name, claim_name in storage_name_to_pvc.items():
            claim = await self._kube.get(
                f"{namespace_url}/persistentvolumeclaims/{claim_name}")
            storage_name_to_pv[storage_name] = claim["spec"]["volumeName"]

        # finally, get real underlying storage paths
        for storage_name, pv_name in storage_name_to_pv.items():
            pv = await self._kube.get(
                f"{self._kube.api_v1_url}/persistentvolumes/{pv_name}")

            # find a supported volume backend for this storage
            try:
                volume_backend = next(
                    iter(
                        vb for vb in VolumeBackend if vb in pv["spec"]
                    )
                )
            except StopIteration:
                logger.info(
                    "storage `%s` doesn't define supported volume backends",
                    storage_name
                )
                continue

            local_path = storage_name_to_local_path[storage_name]
            volume_definition = KubeVolume(
                backend=volume_backend,
                spec=NfsVolumeSpec.from_pv(pv=pv),
            )
            self._local_fs_prefix_to_kube_volume[local_path] = volume_definition

    async def resolve_to_mount_volume(
        self,
        path: str
    ) -> KubeVolume:
        """
        Resolves a path to a proper mount volume, so later it can be used
        in a kube spec of a POD.
        """
        normalized_path = PurePath(path.replace("storage://", "/"))
        local_path = await self._path_resolver.resolve_path(normalized_path)
        str_local_path = str(local_path)

        for fs_path_prefix, kube_volume in self._local_fs_prefix_to_kube_volume.items():
            if not str_local_path.startswith(fs_path_prefix):
                continue

            # patch match, so we create a new volume with the adjusted path
            new_mount_path = str_local_path.replace(
                fs_path_prefix,
                kube_volume.spec.path,
                1,  # replace it only once at the beginning of the string
            )
            return KubeVolume(
                backend=kube_volume.backend,
                spec=NfsVolumeSpec(
                    server=kube_volume.spec.server,
                    path=new_mount_path
                )
            )

        raise VolumeResolverError()
