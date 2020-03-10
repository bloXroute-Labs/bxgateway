#!/usr/bin/env bash
IMAGE=033969152235.dkr.ecr.us-east-1.amazonaws.com/bxgateway:${1:-latest}
echo "Building container... $IMAGE"
docker build ../ -f Dockerfile --rm=false -t $IMAGE
