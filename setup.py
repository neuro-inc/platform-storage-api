from setuptools import find_packages, setup


install_requires = (
    "aiodns==2.0.0",
    "aiofiles==0.6.0",
    "aiohttp==3.7.2",
    "aiozipkin==0.7.1",
    "cbor==1.0.0",
    "cchardet==2.1.6",
    "neuro_auth_client==19.11.26",
    "uvloop==0.14.0",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
)

setup(
    name="platform-storage-api",
    version="0.0.1b1",
    url="https://github.com/neuromation/platform-storage-api",
    packages=find_packages(),
    install_requires=install_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": "platform-storage-api=platform_storage_api.api:main"
    },
)
