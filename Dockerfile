FROM python:3.7.5-stretch as installer

ARG PIP_INDEX_URL
ARG DIST_FILENAME

# Separate step for requirements to speed up docker builds
COPY platform_storage_api.egg-info/requires.txt requires.txt
RUN pip install --user -r requires.txt

# Install service itself
COPY dist/${DIST_FILENAME} ${DIST_FILENAME}
RUN pip install --user $DIST_FILENAME

FROM python:3.7.4-stretch AS service

WORKDIR /neuromation

COPY --from=installer /root/.local/ /root/.local/

ENV PATH=/root/.local/bin:$PATH
ENV NP_STORAGE_API_PORT=8080
EXPOSE $NP_STORAGE_API_PORT

CMD platform-storage-api
