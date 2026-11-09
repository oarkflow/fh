# Deploying `mira-tls-server` with Docker

Image: `scratch` + four static binaries (`mira-tls-server`, `mira-healthcheck`,
`mira-bench`, `mira-dev-certs`), about 32 MB, running as a non-root user.

## Build

```sh
# Anywhere with Go and Docker (compiles inside the build stage):
docker build -f Dockerfile.mira -t fh-mira:local .

# Or cross-compile and ship binaries to a host that should not compile:
for c in mira-tls-server mira-healthcheck mira-bench mira-dev-certs; do
  GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o deploy/mira/bin/$c ./cmd/$c
done
docker build -f deploy/mira/Dockerfile.prebuilt -t fh-mira:local deploy/mira
```

## Run

1. Put `server.pem`, `server.key`, `root.pem` in `certs/` (keys `chmod 600`,
   readable by the container user) and the permitted client identities, one per
   line, in `allow.txt`. **Use your own CA.** `mira-dev-certs` is for checks only.
2. `docker compose -f deploy/mira/compose.yml up -d`
3. `docker inspect -f '{{.State.Health.Status}}' mira-server` should be `healthy`.
4. Revoke or add identities by editing `allow.txt` in place and
   `docker kill -s HUP mira-server`.

Ports are published on **127.0.0.1** by default. Publishing `7447/udp` and
`7448/tcp` publicly is a deliberate step: confirm your CA, allow-list, limits
and firewall first, and read the status note in
[`examples/mira/README.md`](../../examples/mira/README.md).

## Smoke test

```sh
docker run --rm --network <same network> --user "$(id -u):$(id -g)" \
  -v "$PWD/certs:/certs:ro" --entrypoint /mira-bench fh-mira:local \
  -tcp mira-server:7448 -udp mira-server:7447 \
  -cert /certs/client.pem -key /certs/client.key -ca /certs/root.pem \
  -name server.local -clients 4 -messages 1000 -bytes 256
```
