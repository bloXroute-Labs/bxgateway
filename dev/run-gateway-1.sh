#!/bin/sh

PORT=9001

docker run -t \
  --name gateway-1 \
  --volume $(pwd)/src:/app/bxgateway/src \
  --volume $(pwd)/../bxcommon/src:/app/bxcommon/src \
  --publish ${PORT}:${PORT} \
  bxgateway \
  --external-ip docker.for.mac.localhost --external-port ${PORT} \
  --sdn-url http://docker.for.mac.localhost:8080 \
  --blockchain-protocol Bitcoin --blockchain-network Mainnet \
  --blockchain-ip docker.for.mac.localhost --blockchain-port 9333 \
  --to-stdout true --log-flush-immediately true --log-level INFO \
  --connect-to-remote-blockchain false  --min-peer-gateways 0 \
  --split-relays false --encrypt-blocks false
