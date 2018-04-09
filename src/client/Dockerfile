FROM python:2.7.14-alpine3.7

RUN apk update
RUN apk add build-base
RUN apk add libffi-dev

ADD requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt

ADD * /app/

WORKDIR /app

ENTRYPOINT ["python","main.py"]
