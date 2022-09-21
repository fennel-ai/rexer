# syntax=docker/dockerfile:1

# builder stages
#
# - we configure two build stages here, one for arm64 and another for amd64
# - we duplicated the `go build` part across these two builds since for the arm64, it requires librdkafka to be
#   installed. if they were split into two builds, we would need to copy over a lot of generated files and dynamically
#   link them, which makes the solution brittle
#
# these build stages will act as the foundation for the executor build stages

# builder_arm64
FROM --platform=$TARGETPLATFORM golang:1.19-bullseye AS builder_arm64
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
RUN go build -tags dynamic -o server fennel/service/http

# builder_amd64
FROM --platform=$TARGETPLATFORM golang:1.19-bullseye AS builder_amd64
WORKDIR /app
COPY go/fennel/ ./
WORKDIR /app/go/fennel
RUN go build -o server fennel/service/http

# executor stages
#
# - we copy over all the required minimal information from the builder stage (to save/reduce image size) and
#   setup the execution environment as required
#
# these build stages are later used in the runner build stage

# executor_arm64
FROM --platform=$TARGETPLATFORM golang:1.19-bullseye AS executor_arm64
RUN touch /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /kafka/lib
COPY --from=builder_arm64 /usr/local/lib .
RUN echo /kafka/lib >> /etc/ld.so.conf.d/librdkafka.conf
WORKDIR /root/
COPY --from=builder_arm64 /app/go/fennel/server ./
RUN ldconfig

# executor_amd64
FROM --platform=$TARGETPLATFORM golang:1.19-bullseye AS executor_amd64
WORKDIR /root/
COPY --from=builder_amd64 /app/go/fennel/server ./

# runner stages
#
# - directly use the executor build stage based on the target architecture for which the container is being built.
#   This will trigger the build and setup the execution environment accordingly.
# - runs the binary as the last command
FROM executor_$TARGETARCH
# TODO(mohit.aditya): This should be removed
RUN apt-get update \
    && apt-get install -y python3-pip python3-dev \
    && cd /usr/local/bin \
    && ln -s /usr/bin/python3 python \
    && pip3 --no-cache-dir install --upgrade pip \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 --no-cache-dir install pandas requests cloudpickle
# the server binary is already copied over to the root directory in both executor build stages
CMD ["./server"]