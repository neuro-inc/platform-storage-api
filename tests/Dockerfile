FROM platformstorageapi:latest

COPY setup.cfg requirements-test.txt pytest.ini ./
RUN pip install -r requirements-test.txt

COPY Makefile ./

COPY tests tests
