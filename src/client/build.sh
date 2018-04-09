#!/usr/bin/env bash
echo "Building container..."
docker build . -q -t bloxroute-client
