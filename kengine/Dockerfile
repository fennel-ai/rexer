FROM azul/zulu-openjdk-alpine:16

WORKDIR /starql

COPY kengine-1.0-SNAPSHOT.tar .

RUN tar -xvf kengine-1.0-SNAPSHOT.tar

RUN rm kengine-1.0-SNAPSHOT.tar

CMD ["kengine-1.0-SNAPSHOT/bin/kengine"]
