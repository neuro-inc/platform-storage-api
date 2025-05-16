from __future__ import annotations

from collections.abc import Iterator

import pytest
from aioresponses import aioresponses

pytest_plugins = [
    "tests.conftest_ac",
]


@pytest.fixture
def aiohttp_mock() -> Iterator[aioresponses]:
    with aioresponses() as mocked:
        yield mocked
