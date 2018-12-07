#!/bin/sh

python src/bxgateway/main.py --to-stdout 1 --external-ip "127.0.0.1" --external-port 9002 --blockchain-ip "127.0.0.1" \
    --blockchain-port 9335 --blockchain-net-magic 12345 --blockchain-services 0 --blockchain-version 70014 \
    --bloxroute-version bxtest --test-mode disable-encryption --peer-gateways "127.0.0.1:9001"
