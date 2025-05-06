import base64
import dataclasses
import json
from enum import Enum
from functools import cached_property
from pathlib import Path, PurePosixPath
from typing import Any

from aiohttp import web
from pydantic import BaseModel, TypeAdapter, field_validator


SCHEMA_STORAGE = "storage://"


class MountMode(str, Enum):
    READ_ONLY = "r"
    READ_WRITE = "rw"


class MountSchema(BaseModel):
    mount_path: str
    storage_uri: str
    mount_mode: MountMode = MountMode.READ_WRITE  # RW as a default

    @cached_property
    def _uri_parts(self) -> tuple[str, ...]:
        return Path(self.storage_uri).parts

    @cached_property
    def org(self) -> str:
        _, _, org, *_ = self._uri_parts
        return org

    @cached_property
    def project(self) -> str:
        _, _, _, project, *_ = self._uri_parts
        return project

    @cached_property
    def path_parts(self) -> list[str]:
        _, _, _, _, *parts = self._uri_parts
        return parts

    @cached_property
    def storage_path(self) -> str:
        return "/".join(["", self.org, self.project, *self.path_parts])

    @field_validator("mount_path", mode="after")
    @classmethod
    def is_mount_path(cls, value: str) -> str:
        if not Path(value).is_absolute():
            err = f"`{value}` is not an absolute path"
            raise ValueError(err)
        return value

    @field_validator("storage_uri", mode="after")
    @classmethod
    def is_storage_uri(cls, value: str) -> str:
        if not value.startswith(SCHEMA_STORAGE):
            err = f"`{value}` does not follow the {SCHEMA_STORAGE} schema"
            raise ValueError(err)
        path = PurePosixPath(value)
        if len(path.parts) < 4:
            err = (
                f"`{value}` is invalid. "
                "Cluster, org and project names must be present in the storage URI"
            )
            raise ValueError(err)
        return value


InjectionSchema = TypeAdapter(list[MountSchema])


class AdmissionReviewPatchType(str, Enum):
    JSON = "JSONPatch"


@dataclasses.dataclass
class AdmissionReviewResponse:
    uid: str
    patch: list[dict[str, Any]] | None = None

    def add_patch(self, path: str, value: Any) -> None:
        if self.patch is None:
            self.patch = []

        self.patch.append(
            {
                "op": "add",
                "path": path,
                "value": value,
            }
        )

    def allow(self) -> web.Response:
        response = {
            "uid": self.uid,
            "allowed": True,
        }
        if self.patch is not None:
            # convert patch changes to a b64
            dumped = json.dumps(self.patch).encode()
            patch = base64.b64encode(dumped).decode()
            response.update(
                {
                    "patch": patch,
                    "patchType": AdmissionReviewPatchType.JSON.value,
                }
            )

        return web.json_response(
            {
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": response,
            }
        )

    def decline(self, status_code: int, message: str) -> web.Response:
        return web.json_response(
            {
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": {
                    "uid": self.uid,
                    "allowed": False,
                    "status": {
                        "code": status_code,
                        "message": message,
                    },
                },
            }
        )
