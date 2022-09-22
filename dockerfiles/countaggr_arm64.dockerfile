# syntax=docker/dockerfile:1
ARG platform

FROM --platform=$platform golang:1.19-bullseye AS builder
RUN apt -y update && apt -y install libssl-dev libzstd-dev
WORKDIR /kafka
RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /kafka/librdkafka
RUN ./configure
RUN make
RUN make install
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -tags dynamic fennel/service/countaggr

FROM --platform=$platform golang:1.19-bullseye
RUN touch /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /kafka/lib
COPY --from=builder /usr/local/lib .
RUN echo /kafka/lib >> /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /root/
COPY --from=builder /app/go/fennel/countaggr ./
RUN apt update && apt install -y redis-tools
RUN ldconfig
# TODO(mohit.aditya): This should be removed
RUN apt-get update \
    && apt-get install -y python3-pip python3-dev \
    && cd /usr/local/bin \
    && ln -s /usr/bin/python3 python \
    && pip3 --no-cache-dir install --upgrade pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 --no-cache-dir install numpy pandas requests cloudpickle
CMD ["./countaggr"]