FROM python:3.7.0-alpine3.8

RUN apk update \
 && apk add --no-cache \
    bash gcc git libffi libtool  openssl-dev openssh-client \
 && apk add --no-cache --virtual .build_deps build-base libffi-dev \
 && pip install --upgrade pip \
 && pip install virtualenv

 CMD ["/bin/sh"]