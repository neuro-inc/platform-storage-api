from setuptools import setup, find_packages


install_requires = (
    'aiofiles==0.3.2',
    'cchardet==2.1.1',
    'aiodns==1.1.1',
    'aiohttp==3.1.3',
    # TODO (A Danshyn 04/20/18): add uvloop at some point
)

setup(
    name='platform-storage-api',
    version='0.0.1b1',
    url='https://github.com/neuromation/platform-storage-api',
    packages=find_packages(),
    install_requires=install_requires,
    entry_points={
    },
)
