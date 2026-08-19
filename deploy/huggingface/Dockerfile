# The Space image: one container running the whole demo — five DistKV
# nodes as five separate processes talking to each other over real TCP on
# loopback, and the gateway serving the console in front of them.
#
# This is the same code the docker-compose.yml runs across five containers.
# The difference is only how many machines it is spread over, which the
# consensus protocol has no opinion about: the nodes find each other by the
# addresses in -peers either way.
#
# Hugging Face builds this file from the root of the Space repo, which
# deploy/huggingface/publish.sh assembles from this repository.

FROM node:22-alpine AS console

WORKDIR /web
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

COPY --from=build /out/distkv-server /usr/local/bin/distkv-server
COPY --from=build /out/distkv-cli /usr/local/bin/distkv-cli
COPY --from=build /out/distkv-gateway /usr/local/bin/distkv-gateway
COPY --from=console /web/dist /srv/console
COPY deploy/huggingface/start.sh /usr/local/bin/start.sh

# Spaces run as uid 1000 and only $HOME is writable, so that is where the
# five write-ahead logs go. The storage is ephemeral: a Space restart
# starts the cluster empty, which for a demo is a feature — it is also the
# only way to clear out whatever visitors have written.
RUN useradd --create-home --uid 1000 user && \
    chmod +x /usr/local/bin/start.sh
USER user
ENV HOME=/home/user
WORKDIR /home/user

# Spaces expose exactly one port and default to 7860.
EXPOSE 7860
CMD ["/usr/local/bin/start.sh"]
