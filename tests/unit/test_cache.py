import asyncio
import copy
from pathlib import PurePath
from typing import Any

import pytest
from aiohttp.test_utils import make_mocked_request
from aiohttp.web import HTTPNotFound, Request
from aiohttp_security.api import IDENTITY_KEY
from neuro_auth_client.client import ClientAccessSubTreeView
from neuro_auth_client.security import IdentityPolicy

from platform_storage_api.cache import (
    PermissionsCache,
    TimeFactory,
)
from platform_storage_api.security import AbstractPermissionChecker

P = PurePath


class MockPermissionChecker(AbstractPermissionChecker):
    def __init__(
        self,
        call_log: list[Any],
        permission_tree: ClientAccessSubTreeView,
        delay: float = 0.0,
    ) -> None:
        self.call_log = call_log
        self.permission_tree = permission_tree
        self.delay = delay

    async def _get_user_permissions_tree(
        self, request: Request, target_path: PurePath
    ) -> ClientAccessSubTreeView:
        await asyncio.sleep(self.delay)
        tree = self.permission_tree
        parts = target_path.parts
        assert parts[0] == "/"
        for part in parts[1:]:
            action = tree.action
            sub_tree = tree.children.get(part)
            if sub_tree is None:
                tree = ClientAccessSubTreeView(action=action, children={})
                break
            tree = sub_tree
        if not tree.can_list():
            raise HTTPNotFound
        return tree

    async def get_user_permissions_tree(
        self, request: Request, target_path: PurePath
    ) -> ClientAccessSubTreeView:
        self.call_log.append(("tree", target_path))
        return copy.deepcopy(
            await self._get_user_permissions_tree(request, target_path)
        )

    async def check_user_permissions(
        self, request: Request, target_path: PurePath, action: str
    ) -> None:
        self.call_log.append(("check", target_path))
        tree = await self._get_user_permissions_tree(request, target_path)
        if not tree.check_action_allowed(action):
            raise HTTPNotFound


@pytest.fixture
def permission_tree() -> ClientAccessSubTreeView:
    return ClientAccessSubTreeView(
        action="deny",
        children={
            "alice": ClientAccessSubTreeView(action="manage", children={}),
            "bob": ClientAccessSubTreeView(
                action="list",
                children={
                    "folder": ClientAccessSubTreeView(
                        action="read",
                        children={
                            "file": ClientAccessSubTreeView(action="write", children={})
                        },
                    )
                },
            ),
        },
    )


@pytest.fixture
def call_log() -> list[Any]:
    return []


@pytest.fixture
def mock_time() -> TimeFactory:
    def mock_time() -> float:
        return mock_time.time  # type: ignore

    mock_time.time = 0.0  # type: ignore
    return mock_time


@pytest.fixture
def cache(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: TimeFactory,
) -> PermissionsCache:
    return PermissionsCache(
        MockPermissionChecker(call_log, permission_tree),
        time_factory=mock_time,
        expiration_interval_s=100.0,
        forgetting_interval_s=1000.0,
    )


@pytest.fixture
def slow_cache(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: TimeFactory,
) -> PermissionsCache:
    return PermissionsCache(
        MockPermissionChecker(call_log, permission_tree, delay=0.5),
        time_factory=mock_time,
        expiration_interval_s=100.0,
        forgetting_interval_s=1000.0,
    )


@pytest.fixture
def webrequest() -> Request:
    webrequest = make_mocked_request(
        "GET", "/", headers={"Authorization": "Bearer authorization"}
    )
    webrequest.app[IDENTITY_KEY] = IdentityPolicy()
    return webrequest


async def test_cached_permissions(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    cache: PermissionsCache,
    webrequest: Request,
) -> None:
    # Warm up the cache
    tree = await cache.get_user_permissions_tree(webrequest, P("/alice/folder"))
    assert tree == ClientAccessSubTreeView("manage", {})
    assert call_log == [("tree", P("/alice/folder"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Use cached results
    mock_time.time = 99.0
    await cache.check_user_permissions(webrequest, P("/alice/folder"), "read")
    tree = await cache.get_user_permissions_tree(webrequest, P("/alice/folder"))
    assert tree == ClientAccessSubTreeView("manage", {})
    assert call_log == []

    await cache.check_user_permissions(webrequest, P("/bob/folder"), "read")
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == []


async def test_cached_parent_permissions(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    cache: PermissionsCache,
    webrequest: Request,
) -> None:
    # Warm up the cache
    tree = await cache.get_user_permissions_tree(webrequest, P("/alice/folder"))
    assert tree == ClientAccessSubTreeView("manage", {})
    assert call_log == [("tree", P("/alice/folder"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Use cached results
    mock_time.time = 99.0
    await cache.check_user_permissions(webrequest, P("/alice/folder/file"), "read")
    tree = await cache.get_user_permissions_tree(webrequest, P("/alice/folder/file"))
    assert tree == ClientAccessSubTreeView("manage", {})
    assert call_log == []

    await cache.check_user_permissions(webrequest, P("/bob/folder/file"), "read")
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder/file"))
    assert tree == ClientAccessSubTreeView("write", {})
    assert call_log == []

    await cache.check_user_permissions(webrequest, P("/bob/folder/otherfile"), "read")
    assert call_log == []


async def test_expired_permissions_check(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    cache: PermissionsCache,
    webrequest: Request,
) -> None:
    # Warm up the cache
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Expire cached permissions
    mock_time.time += 101.0
    # Trigger the repeat of the request by check_user_permissions() for child
    await cache.check_user_permissions(webrequest, P("/bob/folder/file"), "write")
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Use cached results
    await cache.check_user_permissions(webrequest, P("/bob/folder/file"), "write")
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder/file"))
    assert tree == ClientAccessSubTreeView("write", {})
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == []


async def test_expired_permissions_tree(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    cache: PermissionsCache,
    webrequest: Request,
) -> None:
    # Warm up the cache
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Expire cached permissions
    mock_time.time += 101.0
    # Trigger the repeat of the request by get_user_permissions_tree() for child
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder/file"))
    assert tree == ClientAccessSubTreeView("write", {})
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Use cached results
    await cache.check_user_permissions(webrequest, P("/bob/folder/file"), "write")
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder/file"))
    assert tree == ClientAccessSubTreeView("write", {})
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == []


async def test_expired_permissions_tree_concurrent(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    slow_cache: PermissionsCache,
    webrequest: Request,
) -> None:
    # Warm up the cache
    tree = await slow_cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Expire cached permissions
    mock_time.time += 101.0

    ready = asyncio.Semaphore(0)
    start = asyncio.Event()

    async def coro() -> None:
        ready.release()
        await start.wait()
        # Trigger the repeat of the request by get_user_permissions_tree() for child
        tree = await slow_cache.get_user_permissions_tree(
            webrequest, P("/bob/folder/file")
        )
        assert tree == ClientAccessSubTreeView("write", {})

    ntasks = 10
    tasks = [asyncio.create_task(coro()) for _ in range(ntasks)]
    for _ in range(ntasks):
        await ready.acquire()
    start.set()
    await asyncio.gather(*tasks)

    assert call_log == [("tree", P("/bob/folder"))]


async def test_forget_path(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    cache: PermissionsCache,
    webrequest: Request,
) -> None:
    # Warm up the cache
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Positive result is cached
    del permission_tree.children["bob"].children["folder"].children["file"]
    await cache.check_user_permissions(webrequest, P("/bob/folder/file"), "write")
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == []

    # Forget all cached values
    mock_time.time = 1001.0
    with pytest.raises(HTTPNotFound):
        await cache.check_user_permissions(webrequest, P("/bob/folder/file"), "write")
    assert call_log == [("check", P("/bob/folder/file"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView("read", {})
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()


async def test_forbidden_permissions_tree(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    cache: PermissionsCache,
    webrequest: Request,
) -> None:
    # Warm up the cache
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Expire cached permissions
    mock_time.time += 101.0

    # Forbid permissions
    del permission_tree.children["bob"]

    with pytest.raises(HTTPNotFound):
        await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert call_log == [("tree", P("/bob/folder"))]


async def test_cached_path(
    call_log: list[Any], mock_time: Any, cache: PermissionsCache, webrequest: Request
) -> None:
    # Warm up the cache
    tree = await cache.get_user_permissions_tree(webrequest, P("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        "read", {"file": ClientAccessSubTreeView("write", {})}
    )
    assert call_log == [("tree", P("/bob/folder"))]
    call_log.clear()

    # Keep the path in the cache
    for _ in range(10):
        # Expire cached permissions
        mock_time.time += 101.0
        await cache.check_user_permissions(webrequest, P("/bob/folder/file"), "write")
        assert call_log == [("tree", P("/bob/folder"))]
        call_log.clear()

    assert mock_time.time > 1000.0


async def test_access_denied(
    call_log: list[Any],
    permission_tree: ClientAccessSubTreeView,
    mock_time: Any,
    cache: PermissionsCache,
    webrequest: Request,
) -> None:
    with pytest.raises(HTTPNotFound):
        await cache.get_user_permissions_tree(webrequest, P("/charlie/folder"))
    assert call_log == [("tree", P("/charlie/folder"))]
    call_log.clear()

    # Negative result is not cached
    with pytest.raises(HTTPNotFound):
        await cache.check_user_permissions(webrequest, P("/charlie/folder"), "read")
    assert call_log == [("check", P("/charlie/folder"))]
    call_log.clear()

    with pytest.raises(HTTPNotFound):
        await cache.get_user_permissions_tree(webrequest, P("/charlie/folder"))
    assert call_log == [("tree", P("/charlie/folder"))]
    call_log.clear()

    # Grant permissions
    permission_tree.children["charlie"] = ClientAccessSubTreeView("read", {})
    await cache.check_user_permissions(webrequest, P("/charlie/folder"), "read")
    assert call_log == [("check", P("/charlie/folder"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(webrequest, P("/charlie/folder"))
    assert tree == ClientAccessSubTreeView("read", {})
    assert call_log == [("tree", P("/charlie/folder"))]
    call_log.clear()
