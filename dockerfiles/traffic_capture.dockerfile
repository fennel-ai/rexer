# syntax=docker/dockerfile:1
FROM --platform=linux/arm64 golang:1.18-bullseye AS builder
RUN apt update -y && apt install flex bison -y
RUN wget http://www.tcpdump.org/release/libpcap-1.7.4.tar.gz && tar xzf libpcap-1.7.4.tar.gz && cd libpcap-1.7.4 && ./configure && make install
WORKDIR /trafficcapture
RUN git clone https://github.com/buger/goreplay && cd goreplay && go get github.com/xdg-go/scram && go build -ldflags="-extldflags \"-static\"" -o gor
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -o server fennel/service/trafficcapture

FROM --platform=linux/arm64 golang:1.18-bullseye
WORKDIR /root/
COPY --from=builder /app/go/fennel/server ./
COPY --from=builder /trafficcapture/goreplay/gor ./
CMD ["./server"]
