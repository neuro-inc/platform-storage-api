from pathlib import PurePath
from typing import Union

from .fs.local import FileSystem, copy_streams


class Storage:
    def __init__(
            self, fs: FileSystem, base_path: Union[PurePath, str]) -> None:
        self._fs = fs
        self._base_path = PurePath(base_path)

        # TODO (A Danshyn 04/23/18): implement StoragePathResolver

    def _resolve_real_path(self, path: PurePath) -> PurePath:
        # TODO: (A Danshyn 04/23/18): validate paths
        return PurePath(self._base_path, path.relative_to('/'))

    async def store(self, outstream, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        await self._fs.mkdir(real_path.parent)
        async with self._fs.open(real_path, 'wb') as f:
            await copy_streams(outstream, f)

    async def retrieve(self, instream, path: Union[PurePath, str]) -> None:
        real_path = self._resolve_real_path(PurePath(path))
        async with self._fs.open(real_path, 'rb') as f:
            await copy_streams(f, instream)
