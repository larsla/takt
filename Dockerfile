# syntax=docker/dockerfile:1

# Generated code (templ, CSS, .gen) is committed, so the build needs only Go.
FROM --platform=$BUILDPLATFORM golang:1.26.10 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=dev
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -tags timetzdata \
      -ldflags "-s -w -X github.com/larsla/takt/internal/cli.buildVersion=${VERSION}" \
      -o /out/takt ./cmd/takt
# The data dir for embedded NATS, owned by the default non-root user.
RUN mkdir -p /out/data && chown 65532:65532 /out/data

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /out/takt /takt
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENV TAKT_UI_ADDR=:8088 \
    TAKT_METRICS_ADDR=:9090 \
    TAKT_DATA_DIR=/data
EXPOSE 8088 9090 4222
ENTRYPOINT ["/takt"]
CMD ["server"]
