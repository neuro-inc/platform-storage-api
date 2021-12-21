import asyncio
import tempfile
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from platform_storage_api.fs.local import FileSystem, LocalFileSystem


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)
    yield loop
    loop.close()


@pytest.fixture
async def local_fs() -> AsyncIterator[FileSystem]:
    async with LocalFileSystem() as fs:
        yield fs


@pytest.fixture
def local_tmp_dir_path() -> Iterator[Path]:
    with tempfile.TemporaryDirectory() as d:
        yield Path(d)
