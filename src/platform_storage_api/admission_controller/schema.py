import base64
import dataclasses
import json
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import Any, Optional

from aiohttp import web
from pydantic import BaseModel, TypeAdapter, field_validator


SCHEMA_STORAGE = "storage://"


class MountMode(str, Enum):
    READ_ONLY = "r"
    READ_WRITE = "rw"


class MountSchema(BaseModel):
    mount_path: str
    storage_path: str
    mount_mode: MountMode = MountMode.READ_WRITE  # RW as a default

    @field_validator('mount_path', mode='after')
    @classmethod
    def is_mount_path(cls, value: str) -> str:
        if not Path(value).is_absolute():
            err = f"`{value}` is not an absolute path"
            raise ValueError(err)
        return value

    @field_validator('storage_path', mode='after')
    @classmethod
    def is_storage_path(cls, value: str) -> str:
        if not value.startswith(SCHEMA_STORAGE):
            err = f"`{value}` does not follow the {SCHEMA_STORAGE} schema"
            raise ValueError(err)
        path = PurePosixPath(value)
        if len(path.parts) < 4:
            err = (
                f"`{value}` is invalid. "
                "Cluster, org and project names must be present in the storage path"
            )
            raise ValueError(err)
        return value


InjectionSchema = TypeAdapter(list[MountSchema])


class AdmissionReviewPatchType(str, Enum):
    JSON = "JSONPatch"


@dataclasses.dataclass
class AdmissionReviewResponse:
    uid: str
    patch: Optional[list[dict[str, Any]]] = None

    def add_patch(self, path: str, value: Any) -> None:
        if self.patch is None:
            self.patch = []

        self.patch.append({
            "op": "add",
            "path": path,
            "value": value,
        })

    def allow(self) -> web.Response:
        response = {
            "uid": self.uid,
            "allowed": True,
        }
        if self.patch is not None:
            # convert patch changes to a b64
            dumped = json.dumps(self.patch).encode()
            patch = base64.b64encode(dumped).decode()
            response.update({
                "patch": patch,
                "patchType": AdmissionReviewPatchType.JSON.value,
            })

        return web.json_response(
            {
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": response
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

                }
            }
        )
