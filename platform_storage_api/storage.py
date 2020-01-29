import contextlib
import os
import tempfile
from pathlib import Path, PurePath
from typing import Any, List, Optional, Union

import aiohttp
from aiohttp.abc import AbstractStreamWriter

from .fs.local import DEFAULT_CHUNK_SIZE, FileStatus, FileSystem, copy_streams
from .trace import trace


class Storage:
    def __init__(
        self,
        fs: FileSystem,
        base_path: Union[PurePath, str],
        *,
        upload_tempdir: Optional[Union[PurePath, str]] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE
    ) -> None:
        self._fs = fs
        self._base_path = PurePath(base_path)
        self._chunk_size = chunk_size

        if upload_tempdir:
            self._upload_tempdir: Optional[Path] = Path(upload_tempdir)
            self._upload_tempdir.mkdir(exist_ok=True)
        else:
            self._upload_tempdir = None

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
        self,
        outstream: AbstractStreamWriter,
        path: Union[PurePath, str],
        size: Optional[int] = None,
    ) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path.parent)
        # Check that the destination is a file
        async with self._fs.open(real_path, "wb") as f:
            if self._upload_tempdir and (size is None or size > self._chunk_size):
                with tempfile.NamedTemporaryFile(dir=self._upload_tempdir) as tf1:
                    async with self._fs.open(PurePath(tf1.name), "w+b") as tf2:
                        await copy_streams(outstream, tf2, self._chunk_size)
                        await tf2.flush()
                        await self._fs.copyfile(tf2.fileno(), f.fileno())
            else:
                await copy_streams(outstream, f, self._chunk_size)

    @trace
    async def retrieve(
        self, instream: aiohttp.StreamReader, path: Union[PurePath, str]
    ) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        async with self._fs.open(real_path, "rb") as f:
            await copy_streams(f, instream, self._chunk_size)

    @contextlib.asynccontextmanager
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

    @trace
    async def liststatus(self, path: Union[PurePath, str]) -> List[FileStatus]:
        real_path = self._resolve_real_path(PurePath(path))
        return await self._fs.liststatus(real_path)

    @trace
    async def get_filestatus(self, path: Union[PurePath, str]) -> FileStatus:
        real_path = self._resolve_real_path(PurePath(path))
        return await self._fs.get_filestatus(real_path)

    @trace
    async def mkdir(self, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path)

    @trace
    async def remove(self, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.remove(real_path)

    @trace
    async def rename(
        self, old: Union[PurePath, str], new: Union[PurePath, str]
    ) -> None:
        real_old = self._resolve_real_path(PurePath(old))
        real_new = self._resolve_real_path(PurePath(new))
        await self._fs.rename(real_old, real_new)
