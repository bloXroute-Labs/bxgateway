#!/usr/bin/env bash
echo "Building container..."
docker build --no-cache ../ -f Dockerfile -q -t bxgateway
