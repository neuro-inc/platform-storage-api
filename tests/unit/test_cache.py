import copy
from pathlib import PurePath
from typing import Any, List

import pytest
from aiohttp.test_utils import make_mocked_request
from aiohttp.web import HTTPNotFound, Request
from aiohttp_security.api import IDENTITY_KEY
from neuro_auth_client.client import ClientAccessSubTreeView
from neuro_auth_client.security import IdentityPolicy

from platform_storage_api.cache import AbstractPermissionChecker, PermissionsCache


_actions = ("deny", "list", "read", "write", "manage")


def _has_permissions(requested: str, permitted: str) -> bool:
    return _actions.index(requested) <= _actions.index(permitted)


class MockPermissionChecker(AbstractPermissionChecker):
    def __init__(
        self, call_log: List[Any], permission_tree: ClientAccessSubTreeView
    ) -> None:
        self.call_log = call_log
        self.permission_tree = permission_tree

    async def _get_user_permissions_tree(
        self, request: Request, target_path: PurePath
    ) -> ClientAccessSubTreeView:
        tree = self.permission_tree
        parts = target_path.parts
        assert parts[0] == "/"
        for part in parts[1:]:
            action = tree.action
            tree = tree.children.get(part)
            if tree is None:
                tree = ClientAccessSubTreeView(action=action, children={})
                break
        if tree.action == "deny":
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
        if not _has_permissions(action, tree.action):
            raise HTTPNotFound


@pytest.mark.asyncio
async def test_permission_cache() -> None:
    time: float = 0.0

    def mock_time() -> float:
        return time

    call_log: List[Any] = []

    permission_tree = ClientAccessSubTreeView(
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

    cache = PermissionsCache(
        MockPermissionChecker(call_log, permission_tree),
        time_factory=mock_time,
        expiration_interval_s=100.0,
        forgetting_interval_s=1000.0,
    )
    request = make_mocked_request(
        "GET", "/", headers={"Authorization": "Bearer authorization"}
    )
    request.app[IDENTITY_KEY] = IdentityPolicy()

    tree = await cache.get_user_permissions_tree(request, PurePath("/alice/folder"))
    assert tree == ClientAccessSubTreeView("manage", {})
    assert call_log == [("tree", PurePath("/alice/folder"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        action="read",
        children={"file": ClientAccessSubTreeView(action="write", children={})},
    )
    assert call_log == [("tree", PurePath("/bob/folder"))]
    call_log.clear()

    with pytest.raises(HTTPNotFound):
        await cache.get_user_permissions_tree(request, PurePath("/charlie/folder"))
    assert call_log == [("tree", PurePath("/charlie/folder"))]
    call_log.clear()

    # Cached result
    time = 99.0
    await cache.check_user_permissions(request, PurePath("/alice/folder"), "read")
    assert call_log == []

    await cache.check_user_permissions(request, PurePath("/alice/folder/file"), "read")
    assert call_log == []

    tree = await cache.get_user_permissions_tree(request, PurePath("/alice/folder"))
    assert tree == ClientAccessSubTreeView(action="manage", children={})
    assert call_log == []

    tree = await cache.get_user_permissions_tree(
        request, PurePath("/alice/folder/file")
    )
    assert tree == ClientAccessSubTreeView(action="manage", children={})
    assert call_log == []

    await cache.check_user_permissions(request, PurePath("/bob/folder"), "read")
    assert call_log == []

    await cache.check_user_permissions(request, PurePath("/bob/folder/file"), "read")
    assert call_log == []

    await cache.check_user_permissions(
        request, PurePath("/bob/folder/otherfile"), "read"
    )
    assert call_log == []

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        action="read",
        children={"file": ClientAccessSubTreeView(action="write", children={})},
    )
    assert call_log == []

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder/file"))
    assert tree == ClientAccessSubTreeView(action="write", children={})
    assert call_log == []

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/otherfolder"))
    assert tree == ClientAccessSubTreeView(action="list", children={})
    assert call_log == [("tree", PurePath("/bob/otherfolder"))]
    call_log.clear()

    with pytest.raises(HTTPNotFound):
        await cache.get_user_permissions_tree(request, PurePath("/charlie/folder"))
    assert call_log == [("tree", PurePath("/charlie/folder"))]
    call_log.clear()

    # Expired cache
    time = 101.0
    await cache.check_user_permissions(request, PurePath("/bob/folder/file"), "write")
    assert call_log == [("tree", PurePath("/bob/folder"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder/file"))
    assert tree == ClientAccessSubTreeView(action="write", children={})
    assert call_log == []

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        action="read",
        children={"file": ClientAccessSubTreeView(action="write", children={})},
    )
    assert call_log == []

    time = 202.0
    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder/file"))
    assert tree == ClientAccessSubTreeView(action="write", children={})
    assert call_log == [("tree", PurePath("/bob/folder"))]
    call_log.clear()

    await cache.check_user_permissions(request, PurePath("/bob/folder/file"), "write")
    assert call_log == []

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        action="read",
        children={"file": ClientAccessSubTreeView(action="write", children={})},
    )
    assert call_log == []

    # Positive result is cached
    del permission_tree.children["bob"].children["folder"].children["file"]

    await cache.check_user_permissions(request, PurePath("/bob/folder/file"), "write")
    assert call_log == []

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder"))
    assert tree == ClientAccessSubTreeView(
        action="read",
        children={"file": ClientAccessSubTreeView(action="write", children={})},
    )
    assert call_log == []

    # Forget all cached values
    time = 2000.0
    with pytest.raises(HTTPNotFound):
        await cache.check_user_permissions(
            request, PurePath("/bob/folder/file"), "write"
        )
    assert call_log == [("check", PurePath("/bob/folder/file"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(request, PurePath("/bob/folder"))
    assert tree == ClientAccessSubTreeView(action="read", children={})
    assert call_log == [("tree", PurePath("/bob/folder"))]
    call_log.clear()

    # Negative result is not cached
    with pytest.raises(HTTPNotFound):
        await cache.get_user_permissions_tree(request, PurePath("/charlie/folder"))
    assert call_log == [("tree", PurePath("/charlie/folder"))]
    call_log.clear()

    permission_tree.children["bob"].children["folder"].children[
        "file"
    ] = ClientAccessSubTreeView(action="write", children={})
    permission_tree.children["charlie"] = ClientAccessSubTreeView(
        action="list", children={}
    )
    await cache.check_user_permissions(request, PurePath("/bob/folder/file"), "write")
    assert call_log == [("check", PurePath("/bob/folder/file"))]
    call_log.clear()

    tree = await cache.get_user_permissions_tree(request, PurePath("/charlie/folder"))
    assert call_log == [("tree", PurePath("/charlie/folder"))]
    call_log.clear()
