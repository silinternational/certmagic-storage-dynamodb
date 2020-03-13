FROM golang

RUN apt-get update -y

COPY . /certmagic-storage-dynamodb

WORKDIR /certmagic-storage-dynamodb

RUN go get ./...

CMD ["bash"]