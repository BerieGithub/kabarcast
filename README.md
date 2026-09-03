# kabarcast

> Realtime message broadcasting for multi-tenant applications.

Your backend does one HTTP `POST /v1/publish`; kabarcast fans it out over
WebSocket to every subscribed client, across as many instances as you run.
Clients authenticate with short-lived, channel-scoped tokens issued by *your*
app, so kabarcast never touches your database.

## Why this exists

Application servers are bad at holding thousands of persistent connections.
Each one pins memory and, on a synchronous worker model, occupies a worker.
Scaling connection count then forces you to scale request throughput with it,
even though the two have nothing to do with each other.

kabarcast owns the stateful connections so your API stays stateless and the
two scale independently. Your app keeps doing what it is good at: short HTTP
requests and writing to its database.

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
  instance receives every event from Redis.
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
ssap:user:<user_id>                     personal notifications
ssap:company:<company_id>:assessments   team-wide events
rbst:mill:<mill_id>:weighbridge         live IoT sensor stream
rbst:shipment:<shipment_id>             logistics tracking
```

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
GET  /healthz     liveness
GET  /stats       connections, channels, delivered, slow-consumer drops
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

## Integrating from your backend

Issue a channel token (Django or FastAPI, `pyjwt`):

```python
import jwt, time

def channel_token(user):
    return jwt.encode({
        "sub": str(user.id),
        "channels": [f"ssap:user:{user.id}", f"ssap:company:{user.company_id}:*"],
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
| `@kabarcast/client` (TypeScript) | [`clients/typescript`](clients/typescript) | usable |
| `kabarcast` (Python publisher) | - | planned |
| Dart client (Flutter) | - | planned |

The TypeScript client handles token refresh, reconnect with jittered backoff
and automatic re-subscription. See its
[README](clients/typescript/README.md).

## Roadmap

- [x] WebSocket transport, channel-scoped token auth
- [x] Redis pub/sub fan-out across instances
- [x] Heartbeats, slow-consumer backpressure, graceful shutdown
- [ ] Presence (who is on a channel)
- [ ] Short replay buffer for brief reconnects
- [ ] Prometheus metrics endpoint
- [x] TypeScript client SDK (`@kabarcast/client`)
- [ ] Python publisher SDK (`kabarcast`)
- [ ] Dart client for Flutter
- [ ] Load test to 10k concurrent connections

## License

MIT
