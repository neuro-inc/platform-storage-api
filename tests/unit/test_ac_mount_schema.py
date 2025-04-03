import json
import os
import tempfile
from collections.abc import Iterator

import pytest
from pydantic import ValidationError

from platform_storage_api.admission_controller.schema import (
    SCHEMA_STORAGE,
    MountMode,
    MountSchema,
)


@pytest.fixture
def valid_mount_path() -> Iterator[str]:
    with tempfile.TemporaryDirectory() as d:
        yield os.path.realpath(d)


@pytest.fixture
def valid_storage_path() -> str:
    return "/org/proj"


@pytest.fixture
def valid_storage_uri(valid_storage_path: str) -> str:
    return f"{SCHEMA_STORAGE}default{valid_storage_path}"


@pytest.fixture
def valid_mount_mode() -> str:
    return MountMode.READ_WRITE.value


def test__not_an_absolute_mount_path(
    valid_storage_uri: str,
    valid_mount_mode: str,
) -> None:
    mount_path = 'tmp'

    with pytest.raises(ValidationError) as e:
        _deserialize(mount_path, valid_storage_uri, valid_mount_mode)

    expected_err = "`tmp` is not an absolute path"
    assert expected_err in str(e.value)


def test__invalid_storage_schema(
    valid_mount_path: str,
    valid_mount_mode: str,
) -> None:
    storage_path = '/org/proj'

    with pytest.raises(ValidationError) as e:
        _deserialize(valid_mount_path, storage_path, valid_mount_mode)

    expected_err = f"`{storage_path}` does not follow the {SCHEMA_STORAGE} schema"
    assert expected_err in str(e.value)


def test__storage_schema_doesnt_have_org_and_proj(
    valid_mount_path: str,
    valid_mount_mode: str,
) -> None:
    storage_uri = f'{SCHEMA_STORAGE}org'

    with pytest.raises(ValidationError) as e:
        _deserialize(valid_mount_path, storage_uri, valid_mount_mode)

    expected_err = (
        f"`{storage_uri}` is invalid. "
        f"Cluster, org and project names must be present in the storage URI"
    )
    assert expected_err in str(e.value)


def test__invalid_mount_mode(
    valid_mount_path: str,
    valid_storage_uri: str,
) -> None:
    mount_mode = 'X'

    with pytest.raises(ValidationError) as e:
        _deserialize(valid_mount_path, valid_storage_uri, mount_mode)

    expected_err = "Input should be 'r' or 'rw'"
    assert expected_err in str(e.value)


def test__valid_mount_schema(
    valid_mount_path: str,
    valid_storage_uri: str,
    valid_storage_path: str,
    valid_mount_mode: str,
) -> None:
    """
    Provide proper values and validate using both MountSchema,
    and a type-adapter version - InjectionSchema
    """
    deserialized = _deserialize(
        valid_mount_path, valid_storage_uri, valid_mount_mode
    )

    assert deserialized.mount_path == valid_mount_path
    assert deserialized.storage_uri == valid_storage_uri
    assert deserialized.mount_mode.value == valid_mount_mode
    assert deserialized.storage_path == valid_storage_path


def _deserialize(
    mount_path: str,
    storage_path: str,
    mount_mode: str
) -> MountSchema:
    return MountSchema.model_validate_json(
        json.dumps(
            {
                "mount_path": mount_path,
                "storage_uri": storage_path,
                "mount_mode": mount_mode
            }
        )
    )
