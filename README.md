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

### Running tests locally
```
pip install -r requirements-test.txt
pytest -vv tests
```
