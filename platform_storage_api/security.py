import abc
import logging
from enum import Enum
from pathlib import PurePath

from aiohttp import web
from aiohttp_security import check_authorized, check_permission
from neuro_auth_client import AuthClient, Permission, User
from neuro_auth_client.client import ClientAccessSubTreeView

from .config import Config
from .trace import trace


logger = logging.getLogger(__name__)


class AuthAction(str, Enum):
    DENY = "deny"
    LIST = "list"
    READ = "read"
    WRITE = "write"


class AbstractPermissionChecker:
    @abc.abstractmethod
    async def get_user_permissions_tree(
        self, request: web.Request, target_path: PurePath
    ) -> ClientAccessSubTreeView:
        pass

    async def get_user_permissions(
        self, request: web.Request, target_path: PurePath
    ) -> str:
        tree = await self.get_user_permissions_tree(request, target_path)
        return tree.action

    @abc.abstractmethod
    async def check_user_permissions(
        self, request: web.Request, target_path: PurePath, action: str
    ) -> None:
        pass


class PermissionChecker(AbstractPermissionChecker):
    def __init__(self, app: web.Application, config: Config) -> None:
        self._app = app
        self._config = config

    def _path_to_uri(self, target_path: PurePath) -> str:
        assert str(target_path)[0] == "/"
        if self._config.cluster_name:
            return f"storage://{self._config.cluster_name}{target_path!s}"
        else:
            return f"storage:/{target_path!s}"

    @trace
    async def get_user_permissions_tree(
        self, request: web.Request, target_path: PurePath
    ) -> ClientAccessSubTreeView:
        username = await self._get_user_from_request(request)
        auth_client = self._get_auth_client()
        target_path_uri = self._path_to_uri(target_path)
        tree = await auth_client.get_permissions_tree(username.name, target_path_uri)
        if tree.sub_tree.action == AuthAction.DENY.value:
            raise web.HTTPNotFound
        return tree.sub_tree

    @trace
    async def check_user_permissions(
        self, request: web.Request, target_path: PurePath, action: str
    ) -> None:
        uri = self._path_to_uri(target_path)
        permission = Permission(uri=uri, action=action)
        logger.info(f"Checking {permission}")
        # TODO (Rafa Zubairov): test if user accessing his own data,
        # then use JWT token claims
        try:
            await check_permission(request, action, [permission])
        except web.HTTPUnauthorized:
            # TODO (Rafa Zubairov): Use tree based approach here
            self._raise_unauthorized()
        except web.HTTPForbidden:
            raise web.HTTPNotFound

    def _raise_unauthorized(self) -> None:
        raise web.HTTPUnauthorized(
            headers={"WWW-Authenticate": f'Bearer realm="{self._config.server.name}"'}
        )

    async def _get_user_from_request(self, request: web.Request) -> User:
        try:
            user_name = await check_authorized(request)
        except web.HTTPUnauthorized:
            self._raise_unauthorized()
        return User(name=user_name, cluster_name=self._config.cluster_name)

    def _get_auth_client(self) -> AuthClient:
        return self._app["auth_client"]
