# syntax=docker/dockerfile:1
FROM --platform=linux/arm64 golang:1.18-bullseye AS builder
RUN apt -y update && apt -y install libssl-dev
WORKDIR /kafka
RUN git clone https://github.com/edenhill/librdkafka.git
WORKDIR /kafka/librdkafka
RUN ./configure
RUN make
RUN make install
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -tags dynamic -o server fennel/service/http

FROM --platform=linux/arm64 golang:1.18-bullseye
RUN touch /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /kafka/lib
COPY --from=builder /usr/local/lib .
RUN echo /kafka/lib >> /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /root/
COPY --from=builder /app/go/fennel/server ./
RUN ldconfig
CMD ["./server"]
