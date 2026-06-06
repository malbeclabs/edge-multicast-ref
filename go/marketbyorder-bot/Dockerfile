# syntax=docker/dockerfile:1.6
# Multi-stage build for dz-marketbyorder-bot. Assumes build context is the
# repo root (so the Go workspace is visible) — see docker-compose.yml.

FROM golang:1.25-alpine AS build
WORKDIR /src

# Cache deps first — all workspace members' go.mod/go.sum so `go mod download`
# can resolve in workspace mode.
COPY go/go.work go/go.work.sum ./go/
COPY go/internal/go.mod go/internal/go.sum ./go/internal/
COPY go/topofbook-parser/go.mod go/topofbook-parser/go.sum ./go/topofbook-parser/
COPY go/kernel-receiver/go.mod go/kernel-receiver/go.sum ./go/kernel-receiver/
COPY go/xdp-receiver/go.mod go/xdp-receiver/go.sum ./go/xdp-receiver/
COPY go/topofbook-bot/go.mod go/topofbook-bot/go.sum ./go/topofbook-bot/
COPY go/marketbyorder-parser/go.mod ./go/marketbyorder-parser/
COPY go/marketbyorder-bot/go.mod ./go/marketbyorder-bot/

WORKDIR /src/go/marketbyorder-bot
RUN go mod download

COPY go/marketbyorder-bot/ ./

ARG VERSION=dev
ARG COMMIT=none
RUN CGO_ENABLED=0 go build \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      -o /out/dz-marketbyorder-bot .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tini
COPY --from=build /out/dz-marketbyorder-bot /usr/local/bin/dz-marketbyorder-bot
ENTRYPOINT ["/sbin/tini", "--", "/usr/local/bin/dz-marketbyorder-bot"]
