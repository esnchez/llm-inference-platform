# One image per command of the module: CMD=platform (the control plane, the
# default) or CMD=fakevllm (the fake vLLM server, Phase 6a).
# Built for the local stack (deploy/observability).

# The build stage uses the module's minimum Go version (go.mod), like the
# race tests.
FROM golang:1.23 AS build
ARG CMD=platform
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/app ./cmd/${CMD}

# Static binary on a minimal image: no shell, no package manager, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/app /app
EXPOSE 8080 8000
USER nonroot:nonroot
ENTRYPOINT ["/app"]
