from datetime import UTC, datetime
from pathlib import PurePath
from typing import cast

from apolo_events_client import EventType, RecvEvent, StreamType, Tag

from platform_storage_api.project_deleter import ProjectDeleter
from platform_storage_api.storage import Storage


class FakeStorage:
    def __init__(self) -> None:
        self.removed: list[tuple[str, bool]] = []

    async def remove_notrace(
        self, path: PurePath | str, *, recursive: bool = False
    ) -> None:
        self.removed.append((str(path), recursive))


def make_event(cluster: str) -> RecvEvent:
    return RecvEvent(
        tag=Tag("1"),
        timestamp=datetime.now(tz=UTC),
        sender="platform-admin",
        stream=StreamType("platform-admin"),
        event_type=EventType("project-remove"),
        cluster=cluster,
        org="org",
        project="project",
        user="user",
    )


async def test_removes_project_of_own_cluster() -> None:
    storage = FakeStorage()
    deleter = ProjectDeleter(cast(Storage, storage), None, "apolo-main")

    await deleter._on_admin_event(make_event("apolo-main"))

    assert storage.removed == [("/org/project", True)]


async def test_ignores_project_of_other_cluster() -> None:
    storage = FakeStorage()
    deleter = ProjectDeleter(cast(Storage, storage), None, "apolo-main")

    await deleter._on_admin_event(make_event("alfa"))

    assert storage.removed == []
