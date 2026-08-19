# Three stages: the console is built with node, the binaries with the
# pinned Go toolchain, and the result is one image holding all of them.
# One image rather than two because the gateway and the nodes are the same
# program set deployed with different arguments, and keeping them together
# means a deployment can never run mismatched versions of the two.

FROM node:22-alpine AS console

WORKDIR /web
# Cached on the lockfile, so a change to the console's source doesn't
# reinstall its dependencies.
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -o /out/distkv-server ./cmd/distkv-server && \
    CGO_ENABLED=0 go build -o /out/distkv-cli ./cmd/distkv-cli && \
    CGO_ENABLED=0 go build -o /out/distkv-gateway ./cmd/distkv-gateway

FROM debian:bookworm-slim

RUN useradd --create-home --uid 10001 distkv
COPY --from=build /out/distkv-server /usr/local/bin/distkv-server
COPY --from=build /out/distkv-cli /usr/local/bin/distkv-cli
COPY --from=build /out/distkv-gateway /usr/local/bin/distkv-gateway
COPY --from=console /web/dist /srv/console

# The data directory holds the write-ahead log and snapshots; compose
# mounts a volume here so a restarted container recovers its state.
RUN mkdir -p /var/lib/distkv && chown distkv:distkv /var/lib/distkv
USER distkv
VOLUME ["/var/lib/distkv"]

EXPOSE 7070 8080
ENTRYPOINT ["distkv-server"]
