FROM python:3.7.4-stretch as requirements

WORKDIR /neuromation

ARG PIP_INDEX_URL

# installing dependencies ONLY
COPY setup.py ./
RUN \
    pip install --user -e . && \
    pip uninstall -y platform-storage-api

FROM python:3.7.4-stretch AS service

WORKDIR /neuromation

COPY setup.py ./
COPY --from=requirements /root/.local/ /root/.local/

# installing platform-storage-api
COPY platform_storage_api platform_storage_api
COPY .git .git
RUN pip install --user -e .
RUN rm -rf .git


ENV PATH=/root/.local/bin:$PATH
ENV NP_STORAGE_API_PORT=8080
EXPOSE $NP_STORAGE_API_PORT

CMD platform-storage-api
