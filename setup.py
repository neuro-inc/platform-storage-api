from setuptools import setup, find_packages


install_requires = (
    "aiodns==2.0.0",
    "aiofiles==0.3.2",
    "aiohttp==3.4.4",
    "async-exit-stack==1.0.1",  # backport from 3.7 stdlib
    "cchardet==2.1.4",
    "dataclasses==0.6",  # backport from 3.7 stdlib
    "neuro_auth_client==0.0.1b5",
    # TODO (A Danshyn 04/20/18): add uvloop at some point
)

setup(
    name="platform-storage-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-storage-api",
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
        "console_scripts": "platform-storage-api=platform_storage_api.api:main"
    },
)
