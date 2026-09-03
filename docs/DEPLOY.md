# Deploying kabarcast

Notes from the production deployment behind Caddy, alongside an existing
application stack.

## Topology

kabarcast runs as its own compose project but joins the **existing proxy
network**, so the reverse proxy can reach it by service name. It publishes no
host ports: the only way in is through the proxy, which terminates TLS.

```
internet ──TLS──> Caddy ──docker network──> kabarcast:8080
```

## Deploy

```bash
# 1. Get the source onto the host
git clone git@github.com:BerieGithub/kabarcast.git ~/kabarcast
# (or, if the host's deploy key is scoped to another repo:
#  tar czf - --exclude=.git --exclude=node_modules . | ssh host 'mkdir -p ~/kabarcast && tar xzf - -C ~/kabarcast')

# 2. Secrets
cd ~/kabarcast
cat > .env <<EOF
KABARCAST_CLIENT_TOKEN_SECRET=$(openssl rand -hex 32)
KABARCAST_SERVICE_SECRET=$(openssl rand -hex 32)
KABARCAST_REDIS_URL=
KABARCAST_NETWORK=<your proxy network, e.g. myapp_default>
EOF
chmod 600 .env

# 3. Build and start
docker compose -f docker-compose.prod.yml up -d --build
```

`KABARCAST_REDIS_URL` is intentionally empty for a single instance: with one
replica, routing every event through Redis adds a hop and buys nothing. Set it
as soon as you run a second replica, or clients on the other instance will not
receive events.

## Routing

### Subdomain (preferred)

```caddyfile
kabarcast.example.com {
    reverse_proxy kabarcast:8080
}
```

Requires a DNS A record. Caddy provisions the certificate on first request.

### Path on an existing host (no DNS change needed)

Useful before a DNS record exists.

```caddyfile
handle /realtime/* {
    uri strip_prefix /realtime
    reverse_proxy kabarcast:8080
}
```

Place it alongside the other `handle` blocks; Caddy sorts them by path
specificity, so it takes precedence over a catch-all `handle`. Clients then use
`wss://example.com/realtime` as the hub URL, and the SDK appends `/v1/ws`.

`reverse_proxy` passes WebSocket upgrades through natively. No extra
configuration is required.

## Gotchas

**A single-file bind mount goes stale after an in-place rewrite.** If the
Caddyfile is mounted as a file (`./Caddyfile:/etc/caddy/Caddyfile`) rather than
a directory, editing it on the host may leave the container reading the old
content, and `caddy reload` will then reload the *old* config while reporting
success. Verify before trusting a reload:

```bash
md5sum ./Caddyfile
docker exec caddy md5sum /etc/caddy/Caddyfile   # must match
```

If they differ, restart the proxy container to re-establish the mount:

```bash
docker compose restart caddy
```

**WebSocket upgrades do not work over HTTP/2.** Testing the handshake with
curl will return `400` unless you force HTTP/1.1, because the `Upgrade`
mechanism is not valid in HTTP/2. Browsers handle this automatically.

```bash
curl -i --http1.1 \
  -H "Connection: Upgrade" -H "Upgrade: websocket" \
  -H "Sec-WebSocket-Version: 13" -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
  "https://example.com/realtime/v1/ws?token=$TOKEN"
# expect: HTTP/1.1 101 Switching Protocols
```

## Smoke test

```bash
B=https://example.com
curl -s $B/realtime/healthz                       # {"status":"ok"}
curl -s -o /dev/null -w "%{http_code}\n" $B/realtime/stats            # 401
curl -s -H "Authorization: Bearer $SERVICE_SECRET" $B/realtime/stats  # counters
curl -s -o /dev/null -w "%{http_code}\n" "$B/realtime/v1/ws?token=bad"  # 401
```

A `401` on the bad-token WebSocket request is the useful signal: it proves the
request reached the hub and was rejected by *its* auth, rather than being
swallowed by the proxy (which would show as 404 or 502).

## Updating

```bash
cd ~/kabarcast && git pull
docker compose -f docker-compose.prod.yml up -d --build
```

Connected clients are dropped on restart. The hub closes them with a proper
close frame (see `Hub.CloseAll`), and the TypeScript SDK reconnects with
jittered backoff, so a deploy is invisible to users beyond a brief gap.
