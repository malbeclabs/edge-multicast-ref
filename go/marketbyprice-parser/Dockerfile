# syntax=docker/dockerfile:1.6
# Multi-stage build for dz-marketbyprice-parser. Assumes build context is the
# repo root (so the Go workspace is visible) — see docker-compose.yml.

FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache deps first
COPY go/go.work go/go.work.sum ./go/
COPY go/internal/go.mod go/internal/go.sum ./go/internal/
COPY go/marketbyprice-parser/go.mod go/marketbyprice-parser/go.sum ./go/marketbyprice-parser/

# Copy all workspace members referenced by go.work so `go mod download` works.
# We don't need the source of the other modules, just their go.mod/go.sum.
COPY go/kernel-receiver/go.mod go/kernel-receiver/go.sum ./go/kernel-receiver/
COPY go/xdp-receiver/go.mod go/xdp-receiver/go.sum ./go/xdp-receiver/
COPY go/topofbook-bot/go.mod go/topofbook-bot/go.sum ./go/topofbook-bot/
COPY go/topofbook-parser/go.mod go/topofbook-parser/go.sum ./go/topofbook-parser/
COPY go/marketbyorder-parser/go.mod ./go/marketbyorder-parser/
COPY go/marketbyorder-bot/go.mod ./go/marketbyorder-bot/

WORKDIR /src/go/marketbyprice-parser
RUN go mod download

# Now copy only the parser's source.
COPY go/marketbyprice-parser/ ./

ARG VERSION=dev
ARG COMMIT=none
RUN CGO_ENABLED=0 go build \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      -o /out/dz-marketbyprice-parser .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tini
COPY --from=build /out/dz-marketbyprice-parser /usr/local/bin/dz-marketbyprice-parser
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/dz-marketbyprice-parser"]
