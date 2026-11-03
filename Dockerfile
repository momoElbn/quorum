# Multi-stage build: compile in a full Go image, then copy only the binary
# into a tiny runtime image. The final image has no compiler, shell or
# package manager, which keeps it small and leaves less to attack.

# ---- Stage 1: build ----
FROM golang:1.27.3-alpine AS build
WORKDIR /src

# Download modules before copying the code. Docker caches this layer, so
# editing a .go file doesn't download all dependencies again.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 gives a static binary that needs no C library, so it runs
# on the distroless "static" image. -trimpath and -s -w strip local paths
# and debug symbols to make the binary smaller.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/quorum-server ./cmd/quorum-server
# The checker ships in the same image, so it can run inside the cluster
# (scripts/run-checker.sh) without building a second image.
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/quorum-checker ./cmd/quorum-checker

# The data directory must belong to the non-root user. The runtime image
# has no shell to run mkdir/chown, so we create it here and copy it over.
# Docker copies this ownership into a new named volume mounted at /data.
RUN mkdir -p /out/data

# ---- Stage 2: run ----
# distroless/static has CA certificates, timezone data and a "nonroot"
# user (uid 65532), and almost nothing else.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/quorum-server /quorum-server
COPY --from=build /out/quorum-checker /quorum-checker
COPY --from=build --chown=65532:65532 /out/data /data

# Run as a normal user: if someone breaks into the process, they don't get
# root in the container.
USER 65532:65532

# 8080: HTTP API. 7000: Raft traffic between nodes.
EXPOSE 8080 7000
ENV HTTP_ADDR=:8080 DATA_DIR=/data
ENTRYPOINT ["/quorum-server"]
