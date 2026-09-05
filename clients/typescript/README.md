# @diugemi/kabarcast-client

TypeScript client for [kabarcast](https://github.com/BerieGithub/kabarcast) -
realtime message broadcasting with channel-scoped auth and automatic reconnect.

Works in the browser, and in Node 22+ (which ships a global `WebSocket`).

```bash
npm install @diugemi/kabarcast-client
```

## Usage

```ts
import { KabarcastClient } from '@diugemi/kabarcast-client';

const kabar = new KabarcastClient({
  url: import.meta.env.VITE_KABARCAST_URL,      // wss://kabarcast.example.com
  getToken: async () => {
    // Your own backend mints a short-lived, channel-scoped token.
    const { data } = await api.get('/realtime/token');
    return data.token;
  },
});

await kabar.connect();
await kabar.subscribe(`app:user:${userId}`);

kabar.on('notification.created', (n) => {
  showToast(n.title);
});
```

## What it handles for you

- **Token refresh.** `getToken()` is called on *every* connect attempt, so an
  expired short-lived token can never wedge a reconnect.
- **Reconnect with jittered backoff.** Full jitter, so a fleet of clients
  disconnected by a deploy does not stampede the hub when it returns.
- **Automatic re-subscription.** Channels you subscribed to are restored after
  a reconnect. Your application code does nothing.
- **Ack correlation.** `subscribe()` resolves when the hub acknowledges, and
  rejects if the channel is refused, so authorisation failures surface as
  errors instead of silence.
- **Refused channels are not retried.** A channel your token does not grant is
  dropped from the restore set rather than retried on every reconnect.

## API

### `new KabarcastClient(options)`

| Option | Default | Description |
|---|---|---|
| `url` | required | Hub base URL, e.g. `wss://kabarcast.example.com` |
| `getToken` | required | Returns a channel token (sync or async) |
| `minReconnectDelayMs` | `500` | Backoff floor |
| `maxReconnectDelayMs` | `30000` | Backoff ceiling |
| `maxReconnectAttempts` | `Infinity` | Give up after N consecutive failures |
| `ackTimeoutMs` | `10000` | How long to wait for a subscribe ack |
| `webSocketFactory` | - | Supply a WebSocket implementation (Node < 22) |
| `debug` | `false` | Log lifecycle to `console.debug` |

### Methods

```ts
await kabar.connect();                          // open the connection
const sub = await kabar.subscribe('channel');   // resolves on ack
await sub.unsubscribe();                        // or kabar.unsubscribe('channel')

const off = kabar.on('event.name', (data, meta) => {});
const offAll = kabar.on('*', (data, meta) => {});   // every event
off();                                              // remove handler

kabar.onStateChange((s) => console.log(s));
// 'idle' | 'connecting' | 'connected' | 'reconnecting' | 'closed'

kabar.connectionState;   // current state
kabar.close();           // close and stop reconnecting
```

Handlers receive `(data, meta)` where `meta` is
`{ channel, event, ts }`. Type the payload with a generic:

```ts
type Notification = { id: string; title: string };
kabar.on<Notification>('notification.created', (n) => n.title);
```

## React

One client per app, shared through context or a module singleton. Do not
create one per component.

```tsx
// realtime.ts
export const kabar = new KabarcastClient({
  url: import.meta.env.VITE_KABARCAST_URL,
  getToken: () => api.get('/realtime/token').then((r) => r.data.token),
});

// useChannel.ts
export function useChannel<T>(channel: string, event: string, onEvent: (d: T) => void) {
  const handler = useRef(onEvent);
  handler.current = onEvent;              // avoid resubscribing on every render

  useEffect(() => {
    let sub: Subscription | undefined;
    let cancelled = false;

    kabar.connect()
      .then(() => kabar.subscribe(channel))
      .then((s) => { if (cancelled) s.unsubscribe(); else sub = s; })
      .catch(console.error);

    const off = kabar.on<T>(event, (d) => handler.current(d));

    return () => { cancelled = true; off(); sub?.unsubscribe(); };
  }, [channel, event]);
}
```

```tsx
useChannel<Notification>(`app:user:${userId}`, 'notification.created', (n) => {
  queryClient.invalidateQueries({ queryKey: ['notifications'] });
});
```

## Node

Node 22+ works out of the box. On older Node, pass a factory:

```ts
import WebSocket from 'ws';

const kabar = new KabarcastClient({
  url: process.env.KABARCAST_URL!,
  getToken: () => mintToken(),
  webSocketFactory: (url) => new WebSocket(url) as any,
});
```

## Publishing is a server concern

This package only **receives**. Broadcasting requires the service secret,
which must never reach a browser. Publish from your backend with a plain HTTP
call to `POST /v1/publish` (see the
[main README](https://github.com/BerieGithub/kabarcast#integrating-from-your-backend)).

## Development

```bash
npm install
npm run typecheck
npm run build
npm test        # runs against a stand-in hub over real WebSockets
```

## License

MIT
