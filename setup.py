from setuptools import setup, find_packages


install_requires = (
    "aiodns==2.0.0",
    "aiofiles==0.3.2",
    "aiohttp==3.5.4",
    "cbor==1.0.0",
    "cchardet==2.1.4",
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
