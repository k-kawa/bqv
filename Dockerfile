FROM golang:1.11 AS builder

WORKDIR /go/src/app
COPY . .

ENV GO111MODULE=on
RUN go get ./...
RUN go install

## Runtime
FROM golang:1.11
COPY --from=builder /go/bin/bqv /go/bin/bqv
WORKDIR /root

CMD ["bqv"]