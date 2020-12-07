from pathlib import Path

from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiodns==2.0.0",
    "aiofiles==0.6.0",
    "aiohttp==3.7.3",
    "aiozipkin==1.0.0",
    "cbor==1.0.0",
    "cchardet==2.1.7",
    "neuro_auth_client==19.11.26",
    "uvloop==0.14.0",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
)

if Path(".git").exists():
    version_kwargs = {
        "use_scm_version": {
            "tag_regex": r"(artifactory/)?(?P<version>.*)",
            "git_describe_command": (
                "git describe --dirty --tags --long --match artifactory/*.*.*"
            ),
        },
    }
else:
    # Only used to install requirements in docker in separate step
    version_kwargs = {"version": "0.0.1"}

setup(
    name="platform-storage-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-storage-api",
    packages=find_packages(),
    setup_requires=setup_requires,
    install_requires=install_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": "platform-storage-api=platform_storage_api.api:main"
    },
)
