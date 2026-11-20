# Control plane with simulated workers (cmd/platform).
# Built for the local observability stack (deploy/observability, Phase 5);
# the same image is the starting point for Phase 6.

# The build stage uses the module's minimum Go version (go.mod), like the
# race tests.
FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/platform ./cmd/platform

# Static binary on a minimal image: no shell, no package manager, non-root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/platform /platform
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/platform"]
CMD ["-addr", ":8080"]
