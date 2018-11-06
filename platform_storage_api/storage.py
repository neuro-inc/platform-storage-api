from pathlib import PurePath
from typing import List, Union

from .fs.local import FileStatus, FileSystem, copy_streams


class Storage:
    def __init__(self, fs: FileSystem, base_path: Union[PurePath, str]) -> None:
        self._fs = fs
        self._base_path = PurePath(base_path)

        # TODO (A Danshyn 04/23/18): implement StoragePathResolver

    def _resolve_real_path(self, path: PurePath) -> PurePath:
        # TODO: (A Danshyn 04/23/18): validate paths
        return PurePath(self._base_path, path.relative_to("/"))

    def sanitize_path(self, path: str) -> str:
        """
        Sanitize path - it shall in the end depend on the implementation of the
        underlying storage subsystem, while now put it here.
        :param path:
        :return: string which contains fixed / sanitized path
        """
        path_parts = path.split("/")
        parts_stack = []
        for part in path_parts:
            if part == "..":
                if parts_stack:
                    parts_stack.pop()
                else:
                    raise ValueError("Invalid path.")
            else:
                parts_stack.append(part)
        return "/".join(parts_stack) if parts_stack else ""

    async def store(self, outstream, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path.parent)
        async with self._fs.open(real_path, "wb") as f:
            await copy_streams(outstream, f)

    async def retrieve(self, instream, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        async with self._fs.open(real_path, "rb") as f:
            await copy_streams(f, instream)

    async def liststatus(self, path: Union[PurePath, str]) -> List[FileStatus]:
        real_path = self._resolve_real_path(PurePath(path))
        return await self._fs.liststatus(real_path)

    async def get_filestatus(self, path: Union[PurePath, str]) -> FileStatus:
        real_path = self._resolve_real_path(PurePath(path))
        return await self._fs.get_filestatus(real_path)

    async def mkdir(self, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path)

    async def remove(self, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.remove(real_path)
