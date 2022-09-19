# syntax=docker/dockerfile:1
FROM --platform=linux/amd64 golang:1.19-bullseye AS builder
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
ENV CGO_CFLAGS="-g -O2 -Wno-return-local-addr"
RUN go build fennel/service/bridge


FROM --platform=linux/amd64 node:16.15 AS webapp
WORKDIR /root
RUN mkdir /root/webapp
COPY webapp ./webapp
RUN cd ./webapp && npm install && npm run build

FROM --platform=linux/amd64 golang:1.19-bullseye
RUN mkdir /root/app
RUN mkdir /webapp
WORKDIR /root/app
COPY --from=builder /app/go/fennel/bridge ./
COPY --from=webapp /root/webapp /webapp
RUN mkdir ./mothership
COPY go/fennel/mothership/templates ./mothership/templates
CMD ["./bridge"]
