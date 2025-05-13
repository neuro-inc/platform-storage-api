ARG PY_VERSION=3.11


FROM python:${PY_VERSION}-slim-bookworm AS installer

ENV PATH=/root/.local/bin:$PATH

# Copy to tmp folder to don't pollute home dir
RUN mkdir -p /tmp/dist
COPY dist /tmp/dist

RUN ls /tmp/dist
RUN pip install --user --find-links /tmp/dist platform-storage-api


FROM python:${PY_VERSION}-slim-bookworm as service

LABEL org.opencontainers.image.source = "https://github.com/neuro-inc/platform-storage-api"

WORKDIR /neuromation

COPY --from=installer /root/.local/ /root/.local/

ENV PATH=/root/.local/bin:$PATH
ENV NP_STORAGE_API_PORT=8080
EXPOSE $NP_STORAGE_API_PORT

CMD platform-storage-api
