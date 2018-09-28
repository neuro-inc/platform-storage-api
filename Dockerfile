FROM python:3.6.5-stretch

WORKDIR /neuromation

ARG PIP_INDEX_URL

# installing dependencies ONLY
COPY setup.py ./
RUN \
    pip install -e . && \
    pip uninstall -y platform-storage-api

# installing platform-storage-api
COPY platform_storage_api platform_storage_api
RUN pip install -e .

ENV NP_STORAGE_API_PORT=8080
EXPOSE $NP_STORAGE_API_PORT

CMD platform-storage-api
