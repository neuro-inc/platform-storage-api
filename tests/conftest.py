import inspect
import os.path
import tempfile
import unittest.mock
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import aiohttp
import pytest

from platform_storage_api.fs.local import FileSystem, LocalFileSystem


@pytest.fixture
async def local_fs() -> AsyncIterator[FileSystem]:
    async with LocalFileSystem() as fs:
        yield fs


@pytest.fixture
def local_tmp_dir_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as d:
        yield Path(os.path.realpath(d))


_response_init = aiohttp.ClientResponse.__init__
if "stream_writer" in inspect.signature(_response_init).parameters:

    def _patched_response_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        kwargs.setdefault("stream_writer", unittest.mock.Mock(output_size=0))
        _response_init(self, *args, **kwargs)

    aiohttp.ClientResponse.__init__ = _patched_response_init  # type: ignore[method-assign]
