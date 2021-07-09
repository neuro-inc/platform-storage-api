from setuptools import find_packages, setup


setup_requires = ("setuptools_scm",)

install_requires = (
    "aiodns==3.0.0",
    "aiofiles==0.7.0",
    "aiohttp==3.7.4.post0",
    "aiozipkin==1.1.0",
    "cbor==1.0.0",
    "cchardet==2.1.7",
    "neuro_auth_client==21.6.15",
    "uvloop==0.15.2",
    "platform-logging==21.5.27",
    "aiohttp-cors==0.7.0",
    "sentry-sdk==1.3.0",
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
    python_requires=">=3.8",
    entry_points={
        "console_scripts": "platform-storage-api=platform_storage_api.api:main"
    },
)
