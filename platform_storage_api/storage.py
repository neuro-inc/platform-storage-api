import dataclasses
import os
from contextlib import asynccontextmanager
from pathlib import PurePath
from typing import Any, AsyncIterator, List, Union

import aiohttp
from aiohttp.abc import AbstractStreamWriter

from .fs.local import FileStatus, FileSystem, RemoveListing, copy_streams
from .trace import trace, tracing_cm


class Storage:
    def __init__(self, fs: FileSystem, base_path: Union[PurePath, str]) -> None:
        self._fs = fs
        self._base_path = PurePath(base_path)

        # TODO (A Danshyn 04/23/18): implement StoragePathResolver

    def _resolve_real_path(self, path: PurePath) -> PurePath:
        # TODO: (A Danshyn 04/23/18): validate paths
        return PurePath(self._base_path, path.relative_to("/"))

    def sanitize_path(self, path: Union[str, "os.PathLike[str]"]) -> PurePath:
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
        self, outstream: AbstractStreamWriter, path: Union[PurePath, str]
    ) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path.parent)
        async with self._fs.open(real_path, "wb") as f:
            await copy_streams(outstream, f)

    @trace
    async def retrieve(
        self, instream: aiohttp.StreamReader, path: Union[PurePath, str]
    ) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        async with self._fs.open(real_path, "rb") as f:
            await copy_streams(f, instream)

    @asynccontextmanager
    async def _open(self, path: Union[PurePath, str]) -> Any:
        real_path = self._resolve_real_path(PurePath(path))
        try:
            async with self._fs.open(real_path, "rb+") as f:
                yield f
        except FileNotFoundError:
            await self._fs.mkdir(real_path.parent)
            async with self._fs.open(real_path, "xb+") as f:
                yield f

    @trace
    async def create(self, path: Union[PurePath, str], size: int) -> None:
        async with self._open(path) as f:
            await f.truncate(size)

    @trace
    async def write(self, path: Union[PurePath, str], offset: int, data: bytes) -> None:
        async with self._open(path) as f:
            await f.seek(offset)
            await f.write(data)

    @trace
    async def read(self, path: Union[PurePath, str], offset: int, size: int) -> bytes:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path.parent)
        async with self._fs.open(real_path, "rb") as f:
            await f.seek(offset)
            return await f.read(size)

    @asynccontextmanager
    async def iterstatus(
        self, path: Union[PurePath, str]
    ) -> AsyncIterator[AsyncIterator[FileStatus]]:
        async with tracing_cm("iterstatus"):
            real_path = self._resolve_real_path(PurePath(path))
            async with self._fs.iterstatus(real_path) as it:
                yield it

    @trace
    async def liststatus(self, path: Union[PurePath, str]) -> List[FileStatus]:
        real_path = self._resolve_real_path(PurePath(path))
        return await self._fs.liststatus(real_path)

    @trace
    async def get_filestatus(self, path: Union[PurePath, str]) -> FileStatus:
        real_path = self._resolve_real_path(PurePath(path))
        return await self._fs.get_filestatus(real_path)

    @trace
    async def exists(self, path: Union[PurePath, str]) -> bool:
        real_path = self._resolve_real_path(PurePath(path))
        return await self._fs.exists(real_path)

    @trace
    async def mkdir(self, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path)

    @trace
    async def remove(self, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.remove(real_path)

    @trace
    async def iterremove(
        self, path: Union[PurePath, str]
    ) -> AsyncIterator[RemoveListing]:
        real_path = self._resolve_real_path(PurePath(path))
        return (
            dataclasses.replace(
                remove_listing,
                path=self.sanitize_path(
                    remove_listing.path.relative_to(self._base_path)
                ),
            )
            async for remove_listing in self._fs.iterremove(real_path)
        )

    @trace
    async def rename(
        self, old: Union[PurePath, str], new: Union[PurePath, str]
    ) -> None:
        real_old = self._resolve_real_path(PurePath(old))
        real_new = self._resolve_real_path(PurePath(new))
        await self._fs.rename(real_old, real_new)
