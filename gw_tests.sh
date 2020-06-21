TAG=${1:-latest}
IMAGE=033969152235.dkr.ecr.us-east-1.amazonaws.com/bxgateway:$TAG
BASE_PATH=$PWD/../
docker run --rm -it \
  -e PYTHONPATH=/app/bxcommon/src/:/app/bxcommon-internal/src:/app/bxgateway/src/:/app/bxgateway-internal/src/:/app/bxextensions/ \
  -v $BASE_PATH/bxgateway/test:/app/bxgateway/test \
  -v $BASE_PATH/bxcommon/src:/app/bxcommon/src \
  -v $BASE_PATH/bxcommon-internal/src:/app/bxcommon-internal/src \
  -v $BASE_PATH/bxgateway/src:/app/bxgateway/src \
  -v $BASE_PATH/bxgateway-internal/src:/app/bxgateway-internal/src \
  -v $BASE_PATH/ssl_certificates/dev:/app/ssl_certificates \
  --entrypoint "" \
  $IMAGE /bin/sh -c "pip install mock websockets && python -m unittest discover"
