# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 golang:1.18-bullseye AS builder

# Install jemalloc
RUN apt-get update -y && apt-get install -y git autoconf
RUN git clone https://github.com/jemalloc/jemalloc.git
WORKDIR ./jemalloc
RUN ./autogen.sh --with-jemalloc-prefix="je_" && make && make install

# Build nitrous binary
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -o nitrous -tags jemalloc fennel/service/nitrous

FROM --platform=linux/amd64 golang:1.18-bullseye
WORKDIR /root/
COPY --from=builder /app/go/fennel/nitrous ./
CMD ["./nitrous"]
