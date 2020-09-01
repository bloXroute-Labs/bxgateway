ARG PYTHON_VERSION=3.8.3-alpine3.11
ARG BASE=033969152235.dkr.ecr.us-east-1.amazonaws.com/bxbase:latest

FROM ${BASE} as builder
# Assumes this repo and bxcommon repo are at equal roots

RUN apk update \
 && apk add --no-cache linux-headers gcc libtool openssl-dev libffi \
 && apk add --no-cache --virtual .build_deps build-base libffi-dev \
 && pip install --upgrade pip

RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

COPY bxgateway/requirements.txt ./bxgateway_requirements.txt
COPY bxcommon/requirements.txt ./bxcommon_requirements.txt

RUN pip install -U pip wheel \
 && pip install -r ./bxgateway_requirements.txt \
                -r ./bxcommon_requirements.txt

FROM python:${PYTHON_VERSION}

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN addgroup -g 502 -S bxgateway \
 && adduser -u 502 -S -G bxgateway bxgateway \
 && mkdir -p /app/bxgateway/src \
 && mkdir -p /app/bxcommon/src \
 && mkdir -p /app/bxcommon-internal/src \
 && mkdir -p /app/bxextensions \
 && chown -R bxgateway:bxgateway /app/bxgateway /app/bxcommon /app/bxextensions

RUN apk update \
 && apk add --no-cache \
        'su-exec>=0.2' \
        tini \
        bash \
        gcc \
        openssl-dev \
 && pip install --upgrade pip

COPY --from=builder /opt/venv /opt/venv

COPY bxgateway/docker-entrypoint.sh /usr/local/bin/

COPY --chown=bxgateway:bxgateway bxgateway/src /app/bxgateway/src
COPY --chown=bxgateway:bxgateway bxcommon/src /app/bxcommon/src
COPY --chown=bxgateway:bxgateway bxcommon-internal/src /app/bxcommon-internal/src
COPY --chown=bxgateway:bxgateway bxextensions/release/alpine-3.11 /app/bxextensions

RUN chmod u+s /bin/ping

COPY bxgateway/docker-scripts/bloxroute-cli /bin/bloxroute-cli
RUN chmod u+x /bin/bloxroute-cli

WORKDIR /app/bxgateway
EXPOSE 28332 9001 1801
ENV PYTHONPATH=/app/bxcommon/src/:/app/bxcommon-internal/src/:/app/bxgateway/src/:/app/bxextensions/ \
    LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/app/bxextensions" \
    PATH="/opt/venv/bin:$PATH"
ENTRYPOINT ["/sbin/tini", "--", "/bin/sh", "/usr/local/bin/docker-entrypoint.sh"]
