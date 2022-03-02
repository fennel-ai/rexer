# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 golang:1.17.6-bullseye AS builder
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build fennel/service/bridge

FROM --platform=linux/amd64 golang:1.17.6-bullseye
WORKDIR /root/
COPY --from=builder /app/go/fennel/bridge ./
CMD ["./bridge"]
