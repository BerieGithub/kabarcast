# kabarcast

> Realtime message broadcasting for multi-tenant applications.

[![npm](https://img.shields.io/npm/v/@diugemi/kabarcast-client)](https://www.npmjs.com/package/@diugemi/kabarcast-client)
[![license](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

Your backend does one HTTP `POST /v1/publish`; kabarcast fans it out over
WebSocket to every subscribed client, across as many instances as you run.
Clients authenticate with short-lived, channel-scoped tokens issued by *your*
app, so kabarcast never touches your database.

```bash
npm install @diugemi/kabarcast-client
```

---

## What is this? (plain English)

Imagine a web app where something happens on the server that a user should
know about immediately: a document gets approved, a task is assigned to them,
a delivery arrives.

**The usual way is for the browser to keep asking.** Every 30 seconds it
pings the server: *"anything new for me?"* Almost every time the answer is
"no". It is like ordering food, then walking up to the counter every 30
seconds to ask whether it is ready. Most trips are wasted, the staff get
interrupted constantly, and when your food *is* ready you might still stand
around for another 30 seconds before you find out.

**kabarcast is the pager they hand you instead.** You sit down. The moment
your order is ready, it buzzes. No repeated trips, and the news reaches you in
under a second rather than up to thirty.

A few things follow naturally from that picture:

- **Your pager only buzzes for your order.** Each user gets a pass that names
  exactly which messages they are allowed to receive, so one customer can
  never be alerted about another customer's order. In a system serving many
  companies, that separation is the whole ballgame.
- **The restaurant does not make the chefs run the pagers.** Handing out
  thousands of pagers and keeping them all connected is a completely different
  job from cooking. So it gets its own dedicated system, which is why
  kabarcast runs as a separate service rather than being bolted onto the app.
- **If the pager system breaks, you still get your food.** Nothing is lost,
  because the order was written down when it was placed. You just go back to
  walking up to the counter. kabarcast makes delivery *fast*; it is never the
  thing that makes delivery *happen*.

## What is this? (for engineers)

kabarcast is a standalone WebSocket fan-out service. Your application servers
stay stateless and keep doing short HTTP requests; kabarcast owns the
long-lived connections.

The problem it solves: application servers are poor at holding thousands of
persistent connections. Each connection pins memory and, on a synchronous
worker model, occupies a worker. Scaling connection count then forces you to
scale request throughput alongside it, even though the two are unrelated.
Splitting them lets each scale on its own axis.

How it works:

1. A browser asks **its own backend** for a short-lived JWT that lists the
   channels it may subscribe to.
2. It opens a WebSocket to kabarcast with that token. kabarcast **verifies the
   signature only** - it performs no I/O and never reads your database.
3. Your backend `POST`s an event to `/v1/publish` with a service secret.
4. kabarcast fans it out to every subscriber of that channel, across every
   instance, via Redis pub/sub.

Design commitments worth knowing before you adopt it:

- **Delivery is at-most-once and best effort.** The durable copy of anything
  that matters stays in your database. kabarcast is an accelerator, not a
  system of record, so a hub outage degrades latency rather than correctness.
- **Tenant isolation lives in the token.** A channel grant is only ever what
  your app signed, so a user cannot subscribe to another tenant's channel
  regardless of what their client sends. This is covered by tests.
- **Slow consumers get disconnected, not buffered.** Each connection has a
  bounded outbound queue; a client that cannot keep up is dropped and
  reconnects, rather than growing the process heap without bound.
- **No sticky sessions.** Any client can land on any instance.

Currently running in production behind Caddy, serving a multi-tenant
compliance platform.

## Architecture

```
  browsers / mobile
        |  1. ask their OWN backend for a short-lived channel token
        |  2. wss://kabarcast/v1/ws?token=...
        v
  +-----------+   +-----------+   +-----------+
  |  hub #1   |   |  hub #2   |   |  hub #3   |   stateless peers,
  | 5k conns  |   | 5k conns  |   | 5k conns  |   each holds its own sockets
  +-----------+   +-----------+   +-----------+
        \______________ | ______________/
                        v
                 Redis pub/sub            <- cross-instance fan-out
                        ^
                        |  3. POST /v1/publish  (service secret)
                 your backends
```

Three properties fall out of this design:

- **No shared database.** The hub verifies a signature; it never reads your
  users, tenants or permissions.
- **No sticky sessions.** Any client may land on any instance, because every
  instance receives every event from Redis. This is covered by a test that
  publishes to one instance and asserts delivery to a client held by another.
- **Fail soft.** If the hub is down, your app still works. The durable copy of
  anything that matters is already in your database; kabarcast only makes
  delivery immediate.

## Authentication

Two separate credentials, deliberately:

| Who | Credential | Grants |
|---|---|---|
| End users (browser, mobile) | Short-lived **channel token** (HS256 JWT) signed by *your* app | Subscribing to the channels named inside it |
| Your backends | **Service secret** (bearer) | Publishing to any channel |

The channel token is what enforces tenant isolation. A user's token names the
channels they may join, so a grower can never subscribe to another company's
channel no matter what their client sends.

```json
{
  "sub": "user-uuid",
  "channels": ["ssap:user:<uuid>", "ssap:company:<uuid>:*"],
  "exp": 1735689600
}
```

A grant ending in `*` is a prefix grant, so one token can cover a whole tenant
namespace. Keep the TTL short (60s to 5 min); clients refresh on reconnect.

## Channel naming

Namespacing is what makes multi-tenancy structural rather than incidental:

```
app:user:<user_id>                  personal notifications
app:org:<org_id>:documents          team-wide events
app:site:<site_id>:sensors          live telemetry
app:shipment:<shipment_id>          per-entity tracking
```

Pick a prefix per application so one hub can serve several products without
their channels colliding.

## Client protocol

Client to server:

```json
{"action": "subscribe",   "channel": "ssap:user:123"}
{"action": "unsubscribe", "channel": "ssap:user:123"}
{"action": "ping"}
```

Server to client:

```json
{"type": "ack",   "action": "subscribe", "channel": "ssap:user:123"}
{"type": "error", "message": "not authorized for this channel"}
{"channel": "ssap:user:123", "event": "notification.created",
 "data": {"...": "..."}, "ts": 1735689600123}
```

## HTTP API

```
GET  /healthz     liveness (public)
GET  /stats       connections, channels, delivered, slow-consumer drops
                  (Authorization: Bearer <service secret>)
GET  /v1/ws       WebSocket upgrade (?token=<channel token>)
POST /v1/publish  broadcast an event (Authorization: Bearer <service secret>)
```

```bash
curl -X POST http://localhost:8080/v1/publish \
  -H "Authorization: Bearer $KABARCAST_SERVICE_SECRET" \
  -H "Content-Type: application/json" \
  -d '{"channel":"ssap:user:123","event":"notification.created","data":{"title":"Remediation verified"}}'
```

`202 Accepted` means the event was accepted for delivery. Delivery is
**at-most-once and best effort** by design.

## Quick start

```bash
cp .env.example .env      # set the two secrets
go mod tidy
make run                  # or: make docker-up
```

Then, from a browser:

```ts
import { KabarcastClient } from '@diugemi/kabarcast-client';

const kabar = new KabarcastClient({
  url: 'ws://localhost:8080',
  getToken: () => fetch('/api/realtime/token').then(r => r.json()).then(d => d.token),
});
await kabar.connect();
await kabar.subscribe('user:123');
kabar.on('notification.created', (n) => console.log(n));
```

For production deployment behind a reverse proxy, see
[docs/DEPLOY.md](docs/DEPLOY.md).

## Integrating from your backend

Issue a channel token (Django or FastAPI, `pyjwt`):

```python
import jwt, time

def channel_token(user):
    return jwt.encode({
        "sub": str(user.id),
        "channels": [f"user:{user.id}", f"org:{user.org_id}:*"],  # "*" = prefix grant
        "exp": int(time.time()) + 300,
    }, settings.KABARCAST_CLIENT_TOKEN_SECRET, algorithm="HS256")
```

Publish an event:

```python
import httpx

def publish(channel: str, event: str, data: dict) -> None:
    # Fire and forget: a hub outage must never block a request.
    try:
        httpx.post(
            f"{settings.KABARCAST_PUBLISH_URL}/v1/publish",
            json={"channel": channel, "event": event, "data": data},
            headers={"Authorization": f"Bearer {settings.KABARCAST_SERVICE_SECRET}"},
            timeout=2.0,
        )
    except httpx.HTTPError:
        pass
```

## Backpressure

Every connection has a bounded outbound buffer (`KABARCAST_SEND_BUFFER`). If a
client cannot keep up and the buffer fills, it is **disconnected** rather than
allowed to grow the heap without bound. One slow consumer must never degrade
delivery for everyone else; the client reconnects and resumes.

## Clients

| Package | Location | Status |
|---|---|---|
| [`@diugemi/kabarcast-client`](https://www.npmjs.com/package/@diugemi/kabarcast-client) (TypeScript) | [`clients/typescript`](clients/typescript) | published |
| Python publisher | - | planned |
| Dart client (Flutter) | - | planned |

The TypeScript client handles token refresh, reconnect with jittered backoff
and automatic re-subscription. See its
[README](clients/typescript/README.md).

## Status and roadmap

**Working today**

- [x] WebSocket transport with channel-scoped token auth
- [x] Redis pub/sub fan-out across instances (covered by tests)
- [x] Heartbeats and slow-consumer backpressure
- [x] Graceful WebSocket drain on shutdown
- [x] TypeScript client SDK, [published](https://www.npmjs.com/package/@diugemi/kabarcast-client)
- [x] CI: gofmt, vet, race tests, SDK tests, Docker build
- [x] Running in production behind Caddy

**Next**

- [ ] Prometheus `/metrics` endpoint (connections, events, drops, fan-out latency)
- [ ] Published load test to 10k concurrent connections, with the numbers
- [ ] Python publisher SDK

**Deliberately not built yet**

These are real gaps, not oversights. Each waits for a use case, because
protocol surface added speculatively is protocol surface maintained forever.

- [ ] **Presence** (who is on a channel) - wanted for collaborative editing
      indicators; nothing needs it yet
- [ ] **Replay buffer** for brief reconnects - would add cursors and sequence
      numbers to the protocol. Consumers whose durable copy lives in their own
      database re-sync on reconnect anyway
- [ ] **Dart client** for Flutter - waiting on a mobile consumer

## Documentation

- [docs/DEPLOY.md](docs/DEPLOY.md) - production deployment, reverse-proxy
  routing, and the gotchas worth knowing beforehand
- [clients/typescript/README.md](clients/typescript/README.md) - the
  TypeScript client

## Development

```bash
go test ./... -race -cover     # hub, including Redis fan-out via miniredis
cd clients/typescript && npm ci && npm test
```

CI runs gofmt, `go vet`, race tests, the SDK suite, and a Docker image build
on every push and pull request.

## Contributing

Issues and pull requests are welcome. Please keep the test suite green; the
fan-out and authorisation tests in particular encode guarantees the design
depends on.

## License

MIT
