FROM golang:1.11

WORKDIR /go/src/app
COPY . .

ENV GO111MODULE=on
RUN go get ./...
RUN go install

CMD ["bqv"]