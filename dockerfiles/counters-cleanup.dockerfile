# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 golang:1.19-bullseye AS builder
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -o cleanup fennel/service/cleanup

FROM --platform=linux/amd64 golang:1.19-bullseye
WORKDIR /root/
COPY --from=builder /app/go/fennel/cleanup ./
CMD ["./cleanup"]
