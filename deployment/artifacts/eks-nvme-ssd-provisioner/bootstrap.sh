#!/bin/sh
aws ecr create-repository --repository-name eks-nvme-ssd-provisioner
docker buildx create --use --name fennelbuild
aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 030813887342.dkr.ecr.us-west-2.amazonaws.com
docker buildx build --platform linux/amd64,linux/arm64 -t 030813887342.dkr.ecr.us-west-2.amazonaws.com/eks-nvme-ssd-provisioner:latest --push -f Dockerfile.alpine .
