import dataclasses
import logging
import socket
from enum import Enum
from pathlib import Path, PurePath
from types import TracebackType
from typing import Any, Self, Union

from apolo_kube_client import (
    KubeClient,
    V1Container,
    V1HostPathVolumeSource,
    V1NFSVolumeSource,
    V1PersistentVolume,
    V1PersistentVolumeClaim,
    V1PersistentVolumeClaimVolumeSource,
    V1Pod,
)

from platform_storage_api.config import AdmissionControllerConfig
from platform_storage_api.storage import StoragePathResolver

logger = logging.getLogger(__name__)


class VolumeResolverError(Exception): ...


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
    def from_pv(cls, pv: V1NFSVolumeSource) -> Self:
        """
        Constructs an NFS volume spec from a PV object
        """
        return cls(
            server=pv.server,
            path=pv.path,
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
    def from_spec(cls, spec: V1HostPathVolumeSource) -> Self:
        """
        Constructs a host-path volume spec from a PV object
        """
        return cls(
            path=spec.path,
            type=HostPathType(spec.type)
            if spec.type is not None
            else HostPathType.EMPTY,
        )


VOLUME_BACKEND_SPEC = {
    VolumeBackend.NFS: NfsVolumeSpec,
    VolumeBackend.HOST_PATH: HostPathVolumeSpec,
}


T_VolumeSpec = Union[NfsVolumeSpec, HostPathVolumeSpec]  # noqa: UP007


@dataclasses.dataclass
class KubeVolume:
    backend: VolumeBackend
    spec: T_VolumeSpec

    def to_kube(self) -> dict[str, Any]:
        return {self.backend.value: self.spec.to_kube()}


@dataclasses.dataclass
class KubeVolumeMount:
    volume: KubeVolume
    sub_path: str


class KubeApi:
    """
    Kube methods used by a volume resolver
    """

    def __init__(self, kube_client: KubeClient):
        self._kube = kube_client

    async def get_pod(self, pod_name: str) -> V1Pod:
        return await self._kube.core_v1.pod.get(pod_name)

    async def get_pvc(self, pvc_name: str) -> V1PersistentVolumeClaim:
        return await self._kube.core_v1.persistent_volume_claim.get(pvc_name)

    async def get_pv(self, pv_name: str) -> V1PersistentVolume:
        return await self._kube.core_v1.persistent_volume.get(pv_name)


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

    async def __aenter__(self) -> Self:
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
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        return exc_type is None

    async def _refresh_internal_state(
        self,
        pod: V1Pod,
    ) -> None:
        """
        Refreshes an internal mapping of volumes based on a provided POD.
        This method expects that the POD will have the most up-to-date mapping
        of volumes
        """
        logger.info("refreshing internal state")
        assert pod.spec is not None

        containers = pod.spec.containers

        # go over volumes to identify linked PVCs
        for volume in pod.spec.volumes:
            # host-path-based volume
            if volume.host_path is not None:
                kube_volume_mapping = self._kube_volume_from_host_path(
                    volume.name, volume.host_path, containers
                )

            # potentially might be an NFS volume
            elif volume.persistent_volume_claim is not None:
                kube_volume_mapping = await self._kube_volume_from_pvc(
                    volume.name, volume.persistent_volume_claim, containers
                )

            else:
                logger.info("volume is unsupported")
                continue

            if not kube_volume_mapping:
                logger.info("volume did not produce any valid mapping: %s", volume)
                continue

            logger.info("created a volume mapping: %s", kube_volume_mapping)
            self._local_fs_prefix_to_kube_volume.update(kube_volume_mapping)

    async def to_local_path(self, storage_path: str) -> Path:
        """
        Resolves a storage path to a local path
        """
        return Path(await self._path_resolver.resolve_path(PurePath(storage_path)))

    async def resolve_volume_mount(self, path: str) -> KubeVolumeMount:
        """
        resolves a path to a proper mount volume, so later it can be used
        in a kube spec of a POD.
        :param path: an absolute path to a file
        """
        local_path = await self.to_local_path(storage_path=path)

        for fs_path_prefix, kube_volume in self._local_fs_prefix_to_kube_volume.items():
            try:
                sub_path = local_path.relative_to(fs_path_prefix)
            except ValueError:
                continue

            return KubeVolumeMount(
                volume=kube_volume,
                sub_path=str(sub_path),
            )

        raise VolumeResolverError()

    @staticmethod
    def _kube_volume_from_host_path(
        volume_name: str,
        host_path_source: V1HostPathVolumeSource,
        containers: list[V1Container],
    ) -> dict[str, KubeVolume]:
        kube_volume_mapping = {}

        # now let's go over containers and figure out mount paths for volumes
        for container in containers:
            for volume_mount in container.volume_mounts:
                if volume_mount.name != volume_name:
                    continue

                local_path = volume_mount.mount_path
                kube_volume = KubeVolume(
                    backend=VolumeBackend.HOST_PATH,
                    spec=HostPathVolumeSpec.from_spec(host_path_source),
                )
                kube_volume_mapping[local_path] = kube_volume
        return kube_volume_mapping

    async def _kube_volume_from_pvc(
        self,
        storage_name: str,
        pvc: V1PersistentVolumeClaimVolumeSource,
        containers: list[V1Container],
    ) -> dict[str, KubeVolume]:
        kube_volume_mapping = {}

        # internal storage name to a PV and PVC names
        storage_name_to_pvc: dict[str, str] = {}
        storage_name_to_pv: dict[str, str] = {}

        # storage name to a local mounted path
        storage_name_to_local_path: dict[str, str] = {}

        storage_name_to_pvc[storage_name] = pvc.claim_name

        # now let's go over containers and figure out mount paths for volumes
        for container in containers:
            for volume_mount in container.volume_mounts:
                volume_name = volume_mount.name
                if volume_name not in storage_name_to_pvc:
                    continue
                mount_path = volume_mount.mount_path
                storage_name_to_local_path[volume_name] = mount_path

        # get PVs by claim names
        for storage_name, claim_name in storage_name_to_pvc.items():
            claim = await self._kube.get_pvc(pvc_name=claim_name)
            assert claim.spec.volume_name is not None
            storage_name_to_pv[storage_name] = claim.spec.volume_name

        # finally, get real underlying storage paths
        for storage_name, pv_name in storage_name_to_pv.items():
            pv = await self._kube.get_pv(pv_name)

            if pv.spec.nfs is None:
                logger.info(
                    "storage `%s` doesn't define supported volume backends",
                    storage_name,
                )
                continue

            local_path = storage_name_to_local_path[storage_name]
            kube_volume = KubeVolume(
                backend=VolumeBackend.NFS,
                spec=NfsVolumeSpec.from_pv(pv=pv.spec.nfs),
            )
            kube_volume_mapping[local_path] = kube_volume

        return kube_volume_mapping
