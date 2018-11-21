#!/usr/bin/env bash
echo "Building container..."
docker build ../ -f Dockerfile -t bxgateway
