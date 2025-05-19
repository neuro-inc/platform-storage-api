from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

PYTEST_REUSE_DOCKER_OPT = "--reuse-docker"


def pytest_addoption(parser: Any) -> None:
    parser.addoption(
        PYTEST_REUSE_DOCKER_OPT,
        action="store_true",
        help="Reuse existing docker containers",
    )


@pytest.fixture(scope="session")
def reuse_docker(request: Any) -> bool:
    return bool(request.config.getoption(PYTEST_REUSE_DOCKER_OPT))


@pytest.fixture(scope="session")
def docker_compose_file() -> str:
    return str(Path(__file__).parent.resolve() / "docker/docker-compose.yml")


@pytest.fixture(scope="session")
def docker_setup(reuse_docker: bool) -> list[str]:  # noqa: FBT001
    if reuse_docker:
        return []
    return ["up --build --wait -d"]
