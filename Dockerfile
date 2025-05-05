ARG PY_VERSION=3.9

FROM python:${PY_VERSION}-slim-bookworm AS builder

ENV PATH=/root/.local/bin:$PATH

WORKDIR /tmp
COPY requirements.txt /tmp/

RUN pip install --user --no-cache-dir -r requirements.txt

COPY dist /tmp/dist/
RUN pip install --no-cache-dir --find-links /tmp/dist platform-storage-api && \
    rm -rf /tmp/dist

FROM python:${PY_VERSION}-slim-bookworm AS runtime
LABEL org.opencontainers.image.source = "https://github.com/neuro-inc/platform-storage-api"

ARG SERVICE_NAME="platform-storage-api"
COPY --from=builder /root/.local /home/$SERVICE_NAME/.local

WORKDIR /home/$SERVICE_NAME

ENV PATH=/home/$SERVICE_NAME/.local/bin:$PATH
ENV NP_STORAGE_API_PORT=8080
EXPOSE $NP_STORAGE_API_PORT

ENTRYPOINT [ "platform-storage-api" ]
