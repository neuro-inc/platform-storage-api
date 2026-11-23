import abc
import dataclasses
import errno
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path, PurePath
from typing import Any, cast

from neuro_logging import trace, trace_cm

from .config import Config, StorageMode
from .fs.local import (
    DiskUsage,
    FileStatus,
    FileStatusType,
    FileSystem,
    RemoveListing,
    copy_streams,
)


class RootRenameError(OSError):
    def __init__(self, path: str) -> None:
        super().__init__(
            errno.EPERM, "Cannot move this entry to an org or project root", path
        )


class StoragePathResolver(abc.ABC):
    @abc.abstractmethod
    async def resolve_base_path(self, path: PurePath | None = None) -> PurePath:
        pass

    async def resolve_path(self, path: PurePath) -> PurePath:
        # TODO: (A Danshyn 04/23/18): validate paths
        base_path = await self.resolve_base_path(path)
        return PurePath(base_path, path.relative_to("/"))

    @abc.abstractmethod
    def get_root_path(self, real_path: PurePath) -> PurePath:
        pass


class SingleStoragePathResolver(StoragePathResolver):
    def __init__(self, base_path: PurePath | str) -> None:
        self._base_path = PurePath(base_path)

    async def resolve_base_path(self, path: PurePath | None = None) -> PurePath:
        return self._base_path

    def get_root_path(self, real_path: PurePath) -> PurePath:
        return self._base_path


class MultipleStoragePathResolver(StoragePathResolver):
    def __init__(
        self,
        fs: FileSystem,
        base_path: PurePath | str,
        default_path: PurePath | str,
    ) -> None:
        self._fs = fs
        self._base_path = PurePath(base_path)
        self._default_path = PurePath(default_path)

    async def resolve_base_path(self, path: PurePath | None = None) -> PurePath:
        if path is None or path == PurePath("/"):
            return self._base_path
        storage_folder = Path(self._base_path, path.relative_to("/").parts[0])
        if await self._fs.exists(storage_folder):
            return self._base_path
        return self._default_path

    def get_root_path(self, real_path: PurePath) -> PurePath:
        if real_path.is_relative_to(self._default_path):
            return self._default_path
        return self._base_path


class Storage:
    def __init__(self, path_resolver: StoragePathResolver, fs: FileSystem) -> None:
        self._fs = fs
        self._path_resolver = path_resolver

    def sanitize_path(self, path: str | os.PathLike[str]) -> PurePath:
        """
        Sanitize path - it shall in the end depend on the implementation of the
        underlying storage subsystem, while now put it here.
        :param path:
        :return: string which contains sanitized path
        """
        normpath = os.path.normpath(str(PurePath("/", path)))
        return PurePath(normpath)

    @trace
    async def store(
        self,
        outstream: Any,
        path: PurePath | str,
        offset: int = 0,
        size: int | None = None,
        *,
        create: bool = True,
    ) -> None:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        if create:
            await self._fs.mkdir(real_path.parent)
        async with self._fs.open(real_path, "wb" if create else "rb+") as f:
            if offset:
                await f.seek(offset)
            await copy_streams(outstream, f, size=size)

    @trace
    async def retrieve(
        self,
        instream: Any,
        path: PurePath | str,
        offset: int = 0,
        size: int | None = None,
    ) -> None:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        async with self._fs.open(real_path, "rb") as f:
            if offset:
                await f.seek(offset)
            await copy_streams(f, instream, size=size)

    @asynccontextmanager
    async def _open(self, path: PurePath | str) -> Any:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        try:
            async with self._fs.open(real_path, "rb+") as f:
                yield f
        except FileNotFoundError:
            await self._fs.mkdir(real_path.parent)
            async with self._fs.open(real_path, "xb+") as f:
                yield f

    @trace
    async def create(self, path: PurePath | str, size: int) -> None:
        async with self._open(path) as f:
            await f.truncate(size)

    @trace
    async def write(self, path: PurePath | str, offset: int, data: bytes) -> None:
        async with self._open(path) as f:
            await f.seek(offset)
            await f.write(data)

    @trace
    async def read(self, path: PurePath | str, offset: int, size: int) -> bytes:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        await self._fs.mkdir(real_path.parent)
        async with self._fs.open(real_path, "rb") as f:
            await f.seek(offset)
            return cast(bytes, await f.read(size))

    @asynccontextmanager
    async def iterstatus(
        self, path: PurePath | str
    ) -> AsyncIterator[AsyncIterator[FileStatus]]:
        async with trace_cm("Storage.iterstatus"):
            real_path = await self._path_resolver.resolve_path(PurePath(path))
            async with self._fs.iterstatus(real_path) as it:
                yield it

    @trace
    async def liststatus(self, path: PurePath | str) -> list[FileStatus]:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        return await self._fs.liststatus(real_path)

    @trace
    async def get_filestatus(self, path: PurePath | str) -> FileStatus:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        return await self._fs.get_filestatus(real_path)

    @trace
    async def exists(self, path: PurePath | str) -> bool:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        return await self._fs.exists(real_path)

    @trace
    async def get_real_path(self, path: PurePath | str) -> PurePath:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        return await self._fs.realpath(real_path)

    @trace
    async def get_real_base_path(self, path: PurePath | str) -> PurePath:
        base_path = await self._path_resolver.resolve_base_path(PurePath(path))
        return await self._fs.realpath(base_path)

    @trace
    async def is_symlink(self, path: PurePath | str) -> bool:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        return await self._fs.is_symlink(real_path)

    @trace
    async def mkdir(self, path: PurePath | str) -> None:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        await self._fs.mkdir(real_path)

    @trace
    async def remove(self, path: PurePath | str, *, recursive: bool = False) -> None:
        await self.remove_notrace(path, recursive=recursive)

    async def remove_notrace(
        self, path: PurePath | str, *, recursive: bool = False
    ) -> None:
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        await self._fs.remove(real_path, recursive=recursive)

    @trace
    async def iterremove(
        self, path: PurePath | str, *, recursive: bool = False
    ) -> AsyncIterator[RemoveListing]:
        base_path = await self._path_resolver.resolve_base_path(PurePath(path))
        real_path = await self._path_resolver.resolve_path(PurePath(path))
        return (
            dataclasses.replace(
                remove_listing,
                path=self.sanitize_path(remove_listing.path.relative_to(base_path)),
            )
            async for remove_listing in self._fs.iterremove(
                real_path, recursive=recursive
            )
        )

    @trace
    async def rename(self, old: PurePath | str, new: PurePath | str) -> None:
        real_old = await self._path_resolver.resolve_path(PurePath(old))
        real_new = await self._path_resolver.resolve_path(PurePath(new))
        old_depth = self._get_depth(real_old)
        new_depth = self._get_depth(real_new)
        if new_depth > 2 and old_depth > 1:
            await self._fs.rename(real_old, real_new)
            return
        if new_depth == 2 and old_depth > 2:
            if not await self._fs.move_file(real_old, real_new):
                raise RootRenameError(str(new))
            return
        if not 1 <= old_depth == new_depth <= 2:
            raise RootRenameError(str(new))
        status = await self._fs.get_filestatus(real_old)
        if status.type == FileStatusType.SYMLINK:
            raise RootRenameError(str(new))
        await self._fs.rename(real_old, real_new)

    def _get_depth(self, real_path: PurePath) -> int:
        root_path = self._path_resolver.get_root_path(real_path)
        return len(real_path.relative_to(root_path).parts)

    @trace
    async def disk_usage(self, path: PurePath | str | None = None) -> DiskUsage:
        real_path = await self._path_resolver.resolve_path(PurePath(path or "/"))
        return await self._fs.disk_usage(real_path)


def create_path_resolver(config: Config, fs: FileSystem) -> StoragePathResolver:
    if config.storage.mode == StorageMode.SINGLE:
        return SingleStoragePathResolver(config.storage.fs_local_base_path)
    return MultipleStoragePathResolver(
        fs,
        config.storage.fs_local_base_path,
        config.storage.fs_local_base_path / config.platform.cluster_name,
    )
