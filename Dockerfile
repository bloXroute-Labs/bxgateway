FROM python:2.7.14-alpine3.7

RUN apk update
RUN apk add build-base
RUN apk add libffi-dev

# Assumes this repo and bxcommon repo are at equal roots
ADD bxgateway /app/bxgateway
ADD bxcommon /app/bxcommon
RUN pip install -r /app/bxgateway/requirements.txt
RUN pip install -r /app/bxcommon/requirements.txt
ENV PYTHONPATH=/app/bxcommon/src/:/app/bxgateway/src/

WORKDIR /app

RUN cd /app/bxgateway/src/bxgateway

ENTRYPOINT ["python","main.py"]
