# Platform Storage API

This component is responsible for data ingestion, browsing, manipulation and
retrieval of files in the underlying backend FS.

## Local Development

### Preparing environment
```
python -m venv venv
source venv/bin/activate
pip install -e .
```

### Running unit tests locally
```
pip install -r requirements-test.txt
pytest -vv tests
```

### Running integration tests locally
Make sure you have docker installed.
To build docker image where test run, you should set the following ENV variables:

```
export IMAGE_TAG=latest
export IMAGE_REPO=%link-to-docker-repo%
```

The `%link-to-docker-repo%` links to repo with other services docker images.
It depends on current setup. Please refer to onboarding guide to get it.

Then you can run tests with next command:

```
make test_integration
```

### Reformatting code
```
make format
```

## How to release

Push new tag of form `vXX.XX.XX` where `XX.XX.XX` is semver version
(please just use the date, like 20.12.31 for 31 December 2020).
You can do this by using github "Create release" UI.
