# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 golang:1.17.6-bullseye AS builder
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build fennel/service/countaggr

FROM --platform=linux/amd64 golang:1.17.6-bullseye
WORKDIR /root/
COPY --from=builder /app/go/fennel/countaggr ./
CMD ["./countaggr"]
RUN apt update && apt install redis-cli