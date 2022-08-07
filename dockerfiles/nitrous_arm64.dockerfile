# syntax=docker/dockerfile:1
ARG platform

FROM --platform=$platform golang:1.18-bullseye AS builder

RUN apt -y update && apt -y install libssl-dev autoconf

# Install Kafka
WORKDIR /kafka
RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /kafka/librdkafka
RUN ./configure && make && make install

# Install jemalloc
RUN git clone https://github.com/jemalloc/jemalloc.git
WORKDIR ./jemalloc
RUN ./autogen.sh --with-jemalloc-prefix="je_" && make && make install

# Build nitrous binary
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -tags dynamic,jemalloc -o nitrous fennel/service/nitrous

FROM --platform=$platform golang:1.18-bullseye
RUN touch /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /kafka/lib
COPY --from=builder /usr/local/lib .
RUN echo /kafka/lib >> /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /root/
COPY --from=builder /app/go/fennel/nitrous ./
RUN ldconfig
CMD ["./nitrous"]
