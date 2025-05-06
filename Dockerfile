ARG PY_VERSION=3.13.3

FROM python:${PY_VERSION}-slim-bookworm AS builder

ENV PATH=/root/.local/bin:$PATH

WORKDIR /tmp
COPY requirements.txt /tmp/

RUN pip install --user --no-cache-dir -r requirements.txt

COPY dist /tmp/dist/
RUN pip install --user --no-cache-dir --find-links /tmp/dist platform-storage-api \
    && rm -rf /tmp/dist

FROM python:${PY_VERSION}-slim-bookworm AS runtime
LABEL org.opencontainers.image.source = "https://github.com/neuro-inc/platform-storage-api"

# Name of your service (folder under /home)
ARG SERVICE_NAME="platform-storage-api"

# Tell Python where the "user" site is
ENV HOME=/home/${SERVICE_NAME}
ENV PYTHONUSERBASE=/home/${SERVICE_NAME}/.local
ENV PATH=/home/${SERVICE_NAME}/.local/bin:$PATH

WORKDIR /home/${SERVICE_NAME}

# Copy everything from the builder’s user‐site into your service’s user‐site
COPY --from=builder /root/.local /home/${SERVICE_NAME}/.local

ENV NP_STORAGE_API_PORT=8080
EXPOSE $NP_STORAGE_API_PORT

ENTRYPOINT [ "platform-storage-api" ]
