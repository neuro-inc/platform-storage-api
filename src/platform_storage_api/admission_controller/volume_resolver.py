import dataclasses
import logging
import socket
from copy import deepcopy
from enum import Enum
from pathlib import PurePath
from types import TracebackType
from typing import Any, Optional, Union

from apolo_kube_client.client import KubeClient

from platform_storage_api.admission_controller.schema import SCHEMA_STORAGE
from platform_storage_api.config import AdmissionControllerConfig
from platform_storage_api.storage import StoragePathResolver


logger = logging.getLogger(__name__)


class VolumeResolverError(Exception):
    ...


class VolumeBackend(str, Enum):
    """Supported volume backends"""
    NFS = "nfs"
    HOST_PATH = "hostPath"


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
        Constructs an NFS volume spec from a PV object
        """
        return cls(
            server=pv["spec"][VolumeBackend.NFS]["server"],
            path=pv["spec"][VolumeBackend.NFS]["path"]
        )


class HostPathType(str, Enum):
    EMPTY = ""
    DIRECTORY_OR_CREATE = "DirectoryOrCreate"
    DIRECTORY = "Directory"
    FILE_OR_CREATE = "FileOrCreate"
    FILE = "File"
    SOCKET = "Socket"
    CHAR_DEVICE = "CharDevice"
    BLOCK_DEVICE = "BlockDevice"


@dataclasses.dataclass
class HostPathVolumeSpec(BaseVolumeSpec):
    path: str
    type: HostPathType

    @classmethod
    def from_spec(cls, spec: dict[str, Any]) -> "HostPathVolumeSpec":
        """
        Constructs a host-path volume spec from a PV object
        """
        return cls(
            path=spec[VolumeBackend.HOST_PATH]["path"],
            type=spec[VolumeBackend.HOST_PATH].get("type") or HostPathType.EMPTY
        )


VOLUME_BACKEND_SPEC = {
    VolumeBackend.NFS: NfsVolumeSpec,
    VolumeBackend.HOST_PATH: HostPathVolumeSpec,
}


T_VolumeSpec = Union[NfsVolumeSpec, HostPathVolumeSpec]


@dataclasses.dataclass
class KubeVolume:
    backend: VolumeBackend
    spec: T_VolumeSpec

    def to_kube(self) -> dict[str, Any]:
        return {
            self.backend.value: self.spec.to_kube()
        }


class KubeApi:
    """
    Kube methods used by a volume resolver
    """
    def __init__(
        self,
        kube_client: KubeClient
    ):
        self._kube = kube_client

    def generate_namespace_url(self) -> str:
        return self._kube.generate_namespace_url()

    async def get_pod(
        self,
        pod_name: str,
        namespace_url: Optional[str] = None
    ) -> dict[str, Any]:
        namespace_url = namespace_url or self.generate_namespace_url()
        url = f"{namespace_url}/pods/{pod_name}"
        return await self._kube.get(url)

    async def get_pvc(
        self,
        pvc_name: str,
        namespace_url: Optional[str] = None
    ) -> dict[str, Any]:
        namespace_url = namespace_url or self.generate_namespace_url()
        url = f"{namespace_url}/persistentvolumeclaims/{pvc_name}"
        return await self._kube.get(url)

    async def get_pv(self, pv_name: str) -> dict[str, Any]:
        url = f"{self._kube.api_v1_url}/persistentvolumes/{pv_name}"
        return await self._kube.get(url)


class KubeVolumeResolver:

    def __init__(
        self,
        kube_api: KubeApi,
        path_resolver: StoragePathResolver,
        admission_controller_config: AdmissionControllerConfig,
    ):
        self._kube = kube_api
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
        pod_name = socket.gethostname()

        try:
            pod = await self._kube.get_pod(pod_name)
        except Exception as e:
            raise VolumeResolverError() from e

        await self._refresh_internal_state(pod=pod)

        if not self._local_fs_prefix_to_kube_volume:
            err = "No eligible volumes are mounted to this pod"
            raise VolumeResolverError(err)

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

        containers = pod["spec"]["containers"]

        # go over volumes to identify linked PVCs
        for volume in pod["spec"]["volumes"]:

            # host-path-based volume
            if VolumeBackend.HOST_PATH in volume:
                kube_volume_mapping = self._kube_volume_from_host_path(
                    volume, containers)

            # potentially might be an NFS volume
            elif "persistentVolumeClaim" in volume:
                kube_volume_mapping = await self._kube_volume_from_pvc(
                    volume, containers)

            else:
                logger.info("volume is unsupported")
                continue

            if not kube_volume_mapping:
                logger.info(
                    "volume did not produce any valid mapping: %s", volume)
                continue

            logger.info("created a volume mapping: %s", kube_volume_mapping)
            self._local_fs_prefix_to_kube_volume.update(kube_volume_mapping)

    async def resolve_to_mount_volume(
        self,
        path: str
    ) -> KubeVolume:
        """
        Resolves a path to a proper mount volume, so later it can be used
        in a kube spec of a POD.
        """
        normalized_path = PurePath(path.replace(SCHEMA_STORAGE, "/"))
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

            new_volume = deepcopy(kube_volume)
            new_volume.spec = dataclasses.replace(new_volume.spec, path=new_mount_path)
            return new_volume

        raise VolumeResolverError()

    @staticmethod
    def _kube_volume_from_host_path(
        volume: dict[str, Any],
        containers: list[dict[str, Any]],
    ) -> dict[str, KubeVolume]:
        volume_name = volume["name"]
        kube_volume_mapping = {}

        # now let's go over containers and figure out mount paths for volumes
        for container in containers:
            for volume_mount in container["volumeMounts"]:
                if volume_mount["name"] != volume_name:
                    continue

                local_path = volume_mount["mountPath"]
                kube_volume = KubeVolume(
                    backend=VolumeBackend.HOST_PATH,
                    spec=HostPathVolumeSpec.from_spec(spec=volume)
                )
                kube_volume_mapping[local_path] = kube_volume
        return kube_volume_mapping

    async def _kube_volume_from_pvc(
        self,
        volume: dict[str, Any],
        containers: list[dict[str, Any]],
    ) -> dict[str, KubeVolume]:
        kube_volume_mapping = {}

        # internal storage name to a PV and PVC names
        storage_name_to_pvc: dict[str, str] = {}
        storage_name_to_pv: dict[str, str] = {}

        # storage name to a local mounted path
        storage_name_to_local_path: dict[str, str] = {}

        pvc = volume["persistentVolumeClaim"]
        storage_name, pvc_name = volume["name"], pvc["claimName"]
        storage_name_to_pvc[storage_name] = pvc_name

        # now let's go over containers and figure out mount paths for volumes
        for container in containers:
            for volume_mount in container["volumeMounts"]:
                volume_name = volume_mount["name"]
                if volume_name not in storage_name_to_pvc:
                    continue
                mount_path = volume_mount["mountPath"]
                storage_name_to_local_path[volume_name] = mount_path

        # get PVs by claim names
        for storage_name, claim_name in storage_name_to_pvc.items():
            claim = await self._kube.get_pvc(pvc_name=claim_name)
            storage_name_to_pv[storage_name] = claim["spec"]["volumeName"]

        # finally, get real underlying storage paths
        for storage_name, pv_name in storage_name_to_pv.items():
            pv = await self._kube.get_pv(pv_name)

            if VolumeBackend.NFS not in pv["spec"]:
                logger.info(
                    "storage `%s` doesn't define supported volume backends",
                    storage_name
                )
                continue

            local_path = storage_name_to_local_path[storage_name]
            kube_volume = KubeVolume(
                backend=VolumeBackend.NFS,
                spec=NfsVolumeSpec.from_pv(pv=pv),
            )
            kube_volume_mapping[local_path] = kube_volume

        return kube_volume_mapping
