FROM python:3.7.0-alpine3.8

# add our user and group first to make sure their IDs get assigned consistently, regardless of whatever dependencies get added
RUN addgroup -g 502 -S bxgateway \
 && adduser -u 502 -S -G bxgateway bxgateway \
 && mkdir -p /app/bxgateway/src \
 && mkdir -p /app/bxcommon/src \
 && mkdir -p /app/bxextensions \
 && chown -R bxgateway:bxgateway /app/bxgateway /app/bxcommon /app/bxextensions

RUN apk update \
 && apk add --no-cache \
# grab su-exec for easy step-down from root
        'su-exec>=0.2' \
# grab tini for process management
        tini \
# grab bash for the convenience
        bash \
        linux-headers \
	gcc libtool openssl-dev \
 && pip install --upgrade pip

# Assumes this repo and bxcommon repo are at equal roots
COPY --chown=bxgateway:bxgateway bxgateway/requirements.txt /app/bxgateway
COPY --chown=bxgateway:bxgateway bxcommon/requirements.txt /app/bxcommon

# We add .build_deps dependencies (to build PyNaCl) and then remove them after pip install completed
RUN apk add libffi \
 && apk add --no-cache --virtual .build_deps build-base libffi-dev linux-headers \
 && pip install -r /app/bxgateway/requirements.txt \
 && pip install -r /app/bxcommon/requirements.txt \
 && apk del .build_deps


COPY bxgateway/docker-entrypoint.sh /usr/local/bin/

COPY --chown=bxgateway:bxgateway bxgateway/src /app/bxgateway/src
COPY --chown=bxgateway:bxgateway bxcommon/src /app/bxcommon/src
COPY --chown=bxgateway:bxgateway bxextensions/release/alpine-3.8 /app/bxextensions

RUN chmod u+s /bin/ping

WORKDIR /app/bxgateway
ENV PYTHONPATH=/app/bxcommon/src/:/app/bxgateway/src/:/app/bxextensions/ \
    LD_LIBRARY_PATH="$LD_LIBRARY_PATH:/app/bxextensions"
ENTRYPOINT ["/sbin/tini", "--", "/bin/sh", "/usr/local/bin/docker-entrypoint.sh"]
