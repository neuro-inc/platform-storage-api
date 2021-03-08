from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiodns==2.0.0",
    "aiofiles==0.6.0",
    "aiohttp==3.7.4.post0",
    "aiozipkin==1.0.0",
    "cbor==1.0.0",
    "cchardet==2.1.7",
    "neuro_auth_client==21.1.6",
    "uvloop==0.15.2",
    "platform-logging==0.3",
    "aiohttp-cors==0.7.0",
    "sentry-sdk==1.0.0",
)

setup(
    name="platform-storage-api",
    url="https://github.com/neuromation/platform-storage-api",
    use_scm_version={
        "git_describe_command": "git describe --dirty --tags --long --match v*.*.*",
    },
    packages=find_packages(),
    setup_requires=setup_requires,
    install_requires=install_requires,
    python_requires=">=3.7",
    entry_points={
        "console_scripts": "platform-storage-api=platform_storage_api.api:main"
    },
)
