from pathlib import Path

import aiohttp

from .conftest import ApiConfig
from .conftest_auth import _UserFactory


async def test_deleter(
    api: ApiConfig,
    client: aiohttp.ClientSession,
    local_tmp_dir_path: Path,
    regular_user_factory: _UserFactory,
    server_url: str,
) -> None:
    # (local_tmp_dir_path/"org"/"proj").mkdir(parents=True)
    user = await regular_user_factory()
    headers = {"Authorization": "Bearer " + user.token}
    url = f"{server_url}/org/proj/path/to/file"
    payload = b"test content"

    async with client.put(url, headers=headers, data=payload) as response:
        assert response.status == 201

    async with client.head(url, headers=headers) as response:
        assert response.status == 404
