FROM golang:1.21

RUN apt-get update -y

COPY . /certmagic-storage-dynamodb

WORKDIR /certmagic-storage-dynamodb

RUN go get ./...

CMD ["bash"]
