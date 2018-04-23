import asyncio

import pytest

from platform_storage_api.fs.local import LocalFileSystem


@pytest.fixture(scope='session')
def event_loop():
    asyncio.set_event_loop_policy(asyncio.DefaultEventLoopPolicy())
    loop = asyncio.get_event_loop_policy().new_event_loop()
    loop.set_debug(True)
    yield loop
    loop.close()


@pytest.fixture
async def local_fs():
    async with LocalFileSystem() as fs:
        yield fs
