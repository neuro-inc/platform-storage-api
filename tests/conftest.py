import inspect
import os.path
import tempfile
import unittest.mock
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import aiohttp
import aioresponses
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


# aiohttp 3.14 added a required keyword-only ``stream_writer`` argument to
# ``ClientResponse.__init__``. aioresponses (<=0.7.8) builds mocked responses
# without it, so every mocked request raises ``TypeError: ... missing 1
# required keyword-only argument: 'stream_writer'``. aiohttp only reads
# ``stream_writer.output_size``, so a ``Mock(output_size=0)`` suffices.
#
# This mirrors the upstream fix (aioresponses#288, tracking aioresponses#289).
# The signature guard makes it a no-op on aiohttp < 3.14 and once aioresponses
# ships a release that supplies the argument itself; remove this shim then.
_response_init = aiohttp.ClientResponse.__init__
if "stream_writer" in inspect.signature(_response_init).parameters:
    if aioresponses.__version__ != "0.7.8":
        msg = "aioresponses was updated, remove this shim"
        raise RuntimeError(msg)

    def _patched_response_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        kwargs.setdefault("stream_writer", unittest.mock.Mock(output_size=0))
        _response_init(self, *args, **kwargs)

    aiohttp.ClientResponse.__init__ = _patched_response_init  # type: ignore[method-assign]
