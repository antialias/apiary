# apiary-proxy: the switch proxy, shared edition. Build context: proxy/.
# The proxy is stdlib-only Go, so the binary is cross-compiled on the build machine for each
# target platform (no emulation) and runs on distroless static, which carries CA certificates.
# Keep the Go version equal to go.mod's `go` line.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -buildvcs=false -ldflags='-s -w' -o /out/apiary-proxy .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/apiary-proxy /usr/local/bin/apiary-proxy
# The named Anthropic models the proxy advertises (registry.go). A mounted file can replace it.
COPY models.json /etc/apiary-proxy/models.json
ENV SWITCH_MODELS_FILE=/etc/apiary-proxy/models.json
# No Claude Code runs inside the container, so there is no picker cache to write.
ENV SWITCH_CLIENT_MODELS_CACHE=off
EXPOSE 8787
ENTRYPOINT ["/usr/local/bin/apiary-proxy"]
CMD ["-listen", "0.0.0.0:8787"]
