import os.path
import tempfile
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

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
