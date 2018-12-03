FROM python:2.7.14

# Assumes this repo and bxcommon repo are at equal roots
ADD bxgateway/src /app/bxgateway/src
ADD bxcommon/src /app/bxcommon/src

ADD bxgateway/requirements.txt /app/bxgateway/requirements.txt
ADD bxcommon/requirements.txt /app/bxcommon/requirements.txt

RUN pip install --upgrade pip
RUN pip install -r /app/bxgateway/requirements.txt
RUN pip install -r /app/bxcommon/requirements.txt
ENV PYTHONPATH=/app/bxcommon/src/:/app/bxgateway/src/

WORKDIR /app/bxgateway/src/bxgateway

ENTRYPOINT ["python","main.py"]
