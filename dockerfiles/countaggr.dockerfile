# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 golang:1.19-bullseye AS builder
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build fennel/service/countaggr

FROM --platform=linux/amd64 golang:1.19-bullseye
WORKDIR /root/
COPY --from=builder /app/go/fennel/countaggr ./
RUN apt update && apt install -y redis-tools
# TODO(mohit.aditya): This should be removed
RUN apt-get update \
    && apt-get install -y python3-pip python3-dev \
    && cd /usr/local/bin \
    && ln -s /usr/bin/python3 python \
    && pip3 --no-cache-dir install --upgrade pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 --no-cache-dir install numpy pandas requests cloudpickle
CMD ["./countaggr"]