# syntax=docker/dockerfile:1.7

FROM golang:1.26.7-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -buildvcs=false -o /out/simq ./cmd/simq

FROM alpine:3.23

RUN addgroup -S simq && adduser -S -G simq simq \
    && mkdir -p /var/lib/simq \
    && chown -R simq:simq /var/lib/simq
COPY --from=build /out/simq /usr/local/bin/simq

USER simq
EXPOSE 7000 9324
VOLUME ["/var/lib/simq"]
ENTRYPOINT ["/usr/local/bin/simq"]
