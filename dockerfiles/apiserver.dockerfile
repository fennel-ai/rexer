# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 golang:1.18-bullseye AS builder
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -o server -tags badger fennel/service/http

FROM --platform=linux/amd64 golang:1.18-bullseye
WORKDIR /root/
COPY --from=builder /app/go/fennel/server ./
CMD ["./server"]
