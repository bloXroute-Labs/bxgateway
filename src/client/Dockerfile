FROM python:2.7.14-alpine3.7

RUN pip install pympler
RUN apk update
RUN apk add build-base
RUN apk add libffi-dev
RUN pip install csiphash

ADD * /app/

WORKDIR /app

ENTRYPOINT ["python","main.py"]
