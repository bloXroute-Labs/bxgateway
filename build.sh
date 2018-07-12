#!/usr/bin/env bash
echo "Building container..."
docker build ../ -f Dockerfile -q -t bxgateway
