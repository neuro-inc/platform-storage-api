from setuptools import find_packages, setup


install_requires = (
    "aiodns==2.0.0",
    "aiofiles==0.4.0",
    "aiohttp==3.6.2",
    "aiozipkin==0.6.0",
    "cbor==1.0.0",
    "cchardet==2.1.4",
    "neuro_auth_client==1.0.10",
    "uvloop==0.13.0",
    "platform-logging==0.3",
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
