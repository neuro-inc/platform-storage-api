import collections
import time
from dataclasses import dataclass
from pathlib import PurePath
from typing import Callable, Optional, Tuple

from aiohttp import web
from aiohttp.hdrs import AUTHORIZATION
from neuro_auth_client.client import ClientAccessSubTreeView

from .security import AbstractPermissionChecker


TimeFactory = Callable[[], float]
PermissionsCacheKey = Tuple[str, str]  # authorization, path


@dataclass(frozen=True)
class PermissionsCacheValue:
    tree: ClientAccessSubTreeView
    expired_at: float
    drop_at: float


class PermissionsCache(AbstractPermissionChecker):
    def __init__(
        self,
        checker: AbstractPermissionChecker,
        *,
        time_factory: TimeFactory = time.monotonic,
        expiration_interval_s: float = 5.0,
        forgetting_interval_s: float = 300.0,
    ) -> None:
        self._checker = checker
        self._time_factory = time_factory
        self._cache: """collections.OrderedDict[
            PermissionsCacheKey, PermissionsCacheValue
        ]""" = collections.OrderedDict()
        self.expiration_interval_s = expiration_interval_s
        self.forgetting_interval_s = forgetting_interval_s

    async def get_user_permissions_tree(
        self, request: web.Request, target_path: PurePath
    ) -> ClientAccessSubTreeView:
        tree = await self._get_user_permissions_tree_cached(request, target_path)
        if tree is not None:
            return tree

        now = self._time_factory()
        tree = await self._checker.get_user_permissions_tree(request, target_path)

        auth_header_value = request.headers.get(AUTHORIZATION)
        key = auth_header_value, str(target_path)
        expired_at = now + self.expiration_interval_s
        self._add_to_cache(key, tree, expired_at)
        return tree

    async def _get_user_permissions_tree_cached(
        self, request: web.Request, target_path: PurePath
    ) -> Optional[ClientAccessSubTreeView]:
        self._cleanup_cache()
        stack = []
        auth_header_value = request.headers.get(AUTHORIZATION)
        while True:
            key = auth_header_value, str(target_path)
            cached = self._cache.get(key, None)
            if cached is not None:
                break

            parent_path = target_path.parent
            if parent_path == target_path:
                return None
            stack.append(target_path.name)
            target_path = parent_path

        tree = cached.tree
        expired_at = cached.expired_at
        now = self._time_factory()
        if expired_at < now:
            if not stack:
                return None
            try:
                tree = await self._checker.get_user_permissions_tree(
                    request, target_path
                )
            except web.HTTPNotFound:
                self._cache.pop(key, None)
                return None
            expired_at = now + self.expiration_interval_s
        self._add_to_cache(key, tree, expired_at)

        while stack:
            action = tree.action
            tree = tree.children.get(stack.pop())
            if tree is None:
                return ClientAccessSubTreeView(action=action, children={})
        return tree

    def _add_to_cache(
        self, key: PermissionsCacheKey, tree: ClientAccessSubTreeView, expired_at: float
    ) -> None:
        drop_at = self._time_factory() + self.forgetting_interval_s
        self._cache[key] = PermissionsCacheValue(
            tree=tree, expired_at=expired_at, drop_at=drop_at
        )
        self._cache.move_to_end(key)

    async def check_user_permissions(
        self, request: web.Request, target_path: PurePath, action: str
    ) -> None:
        tree = await self._get_user_permissions_tree_cached(request, target_path)
        if tree and _has_permissions(action, tree.action):
            return

        await self._checker.check_user_permissions(request, target_path, action)

    def _cleanup_cache(self) -> None:
        # Remove expired cached entities
        now = self._time_factory()
        while self._cache:
            key, value = next(iter(self._cache.items()))
            if value.drop_at > now:
                break
            self._cache.pop(key, None)


_actions = ("deny", "list", "read", "write", "manage")


def _has_permissions(requested: str, permitted: str) -> bool:
    return _actions.index(requested) <= _actions.index(permitted)
