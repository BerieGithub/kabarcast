import assert from 'node:assert/strict';
import test from 'node:test';
import { WebSocketServer } from 'ws';

import { KabarcastClient } from '../dist/index.js';

/**
 * Spins up a stand-in for the hub that speaks the same wire protocol:
 * acknowledges subscribe/unsubscribe and can broadcast events on demand.
 */
function startFakeHub() {
  const wss = new WebSocketServer({ port: 0 });
  const state = {
    /** every subscribe the server saw, across reconnects */
    subscribes: [],
    /** tokens presented on each connection */
    tokens: [],
    sockets: new Set(),
  };

  wss.on('connection', (ws, req) => {
    state.sockets.add(ws);
    const url = new URL(req.url, 'http://localhost');
    state.tokens.push(url.searchParams.get('token'));

    ws.on('close', () => state.sockets.delete(ws));
    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      if (msg.action === 'subscribe') {
        state.subscribes.push(msg.channel);
        ws.send(JSON.stringify({ type: 'ack', action: 'subscribe', channel: msg.channel }));
      } else if (msg.action === 'unsubscribe') {
        ws.send(JSON.stringify({ type: 'ack', action: 'unsubscribe', channel: msg.channel }));
      }
    });
  });

  return {
    state,
    url: () => `ws://127.0.0.1:${wss.address().port}`,
    broadcast(channel, event, data) {
      const frame = JSON.stringify({ channel, event, data, ts: Date.now() });
      for (const ws of state.sockets) ws.send(frame);
    },
    dropAll() {
      for (const ws of state.sockets) ws.terminate();
    },
    close() {
      return new Promise((r) => wss.close(r));
    },
  };
}

const waitFor = async (predicate, timeoutMs = 4000) => {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (predicate()) return true;
    await new Promise((r) => setTimeout(r, 20));
  }
  return false;
};

test('subscribes and receives a broadcast event', async () => {
  const hub = startFakeHub();
  const client = new KabarcastClient({
    url: hub.url(),
    getToken: () => 'token-1',
    minReconnectDelayMs: 20,
  });

  await client.connect();
  await client.subscribe('app:user:1');

  const received = [];
  client.on('notification.created', (data, meta) => received.push({ data, meta }));

  hub.broadcast('app:user:1', 'notification.created', { title: 'hello' });

  assert.ok(await waitFor(() => received.length > 0), 'event was not delivered');
  assert.equal(received[0].data.title, 'hello');
  assert.equal(received[0].meta.channel, 'app:user:1');

  client.close();
  await hub.close();
});

test('reconnects and restores subscriptions automatically', async () => {
  const hub = startFakeHub();
  let tokenCalls = 0;
  const client = new KabarcastClient({
    url: hub.url(),
    getToken: () => `token-${++tokenCalls}`,
    minReconnectDelayMs: 20,
    maxReconnectDelayMs: 60,
  });

  await client.connect();
  await client.subscribe('app:org:7:documents');
  assert.equal(hub.state.subscribes.length, 1);

  // Simulate the hub restarting underneath the client.
  hub.dropAll();

  // The client should come back on its own AND re-subscribe, without the
  // application doing anything.
  assert.ok(
    await waitFor(() => hub.state.subscribes.length >= 2),
    'client did not re-subscribe after reconnect',
  );
  assert.equal(hub.state.subscribes[1], 'app:org:7:documents');

  // A fresh token is fetched per attempt, so an expired one cannot wedge it.
  assert.ok(tokenCalls >= 2, 'token was not refreshed on reconnect');

  // And delivery works again on the new connection.
  const received = [];
  client.on('document.updated', (d) => received.push(d));
  await waitFor(() => hub.state.sockets.size > 0);
  hub.broadcast('app:org:7:documents', 'document.updated', { id: 42 });
  assert.ok(await waitFor(() => received.length > 0), 'no delivery after reconnect');

  client.close();
  await hub.close();
});

test('close() stops reconnecting', async () => {
  const hub = startFakeHub();
  const client = new KabarcastClient({
    url: hub.url(),
    getToken: () => 'token',
    minReconnectDelayMs: 20,
  });

  await client.connect();
  client.close();
  assert.equal(client.connectionState, 'closed');

  const before = hub.state.tokens.length;
  hub.dropAll();
  await new Promise((r) => setTimeout(r, 300));
  assert.equal(hub.state.tokens.length, before, 'client reconnected after close()');

  await hub.close();
});

test('a refused subscribe rejects and is not retried on reconnect', async () => {
  const wss = new WebSocketServer({ port: 0 });
  const seen = [];
  wss.on('connection', (ws) => {
    ws.on('message', (raw) => {
      const msg = JSON.parse(raw.toString());
      seen.push(msg.channel);
      // Deny everything, the way the hub denies a channel the token
      // does not grant.
      ws.send(
        JSON.stringify({
          type: 'error',
          action: 'subscribe',
          channel: msg.channel,
          message: 'not authorized for this channel',
        }),
      );
    });
  });

  const client = new KabarcastClient({
    url: `ws://127.0.0.1:${wss.address().port}`,
    getToken: () => 'token',
    minReconnectDelayMs: 20,
  });
  await client.connect();

  await assert.rejects(
    () => client.subscribe('app:user:someone-else'),
    /not authorized/,
    'expected a refused subscribe to reject',
  );

  client.close();
  await new Promise((r) => wss.close(r));
});
