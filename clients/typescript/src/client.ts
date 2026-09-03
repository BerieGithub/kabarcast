import { backoffDelay } from './backoff.js';
import type {
  ConnectionState,
  EventHandler,
  KabarcastEvent,
  KabarcastOptions,
  ServerFrame,
  StateHandler,
  Subscription,
  WebSocketLike,
} from './types.js';

const OPEN = 1;

interface PendingAck {
  resolve: () => void;
  reject: (err: Error) => void;
  timer: ReturnType<typeof setTimeout>;
}

/**
 * Client for a kabarcast hub.
 *
 * Handles the parts you do not want to reimplement in every app: token
 * refresh, reconnect with jittered backoff, and restoring subscriptions after
 * a drop.
 *
 * ```ts
 * const kabar = new KabarcastClient({
 *   url: 'wss://kabarcast.example.com',
 *   getToken: () => api.get('/realtime/token').then(r => r.data.token),
 * });
 * await kabar.connect();
 * await kabar.subscribe('ssap:user:123');
 * kabar.on('notification.created', (n) => showToast(n));
 * ```
 */
export class KabarcastClient {
  private readonly opts: KabarcastOptions & {
    minReconnectDelayMs: number;
    maxReconnectDelayMs: number;
    maxReconnectAttempts: number;
    ackTimeoutMs: number;
    debug: boolean;
  };

  private ws: WebSocketLike | null = null;
  private state: ConnectionState = 'idle';

  /** Channels the caller wants; replayed after every reconnect. */
  private readonly desired = new Set<string>();
  private readonly pending = new Map<string, PendingAck>();
  private readonly handlers = new Map<string, Set<EventHandler<any>>>();
  private readonly stateHandlers = new Set<StateHandler>();

  private attempt = 0;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private closedByUser = false;
  private connecting: Promise<void> | null = null;

  constructor(options: KabarcastOptions) {
    this.opts = {
      minReconnectDelayMs: 500,
      maxReconnectDelayMs: 30_000,
      maxReconnectAttempts: Number.POSITIVE_INFINITY,
      ackTimeoutMs: 10_000,
      debug: false,
      ...options,
    };
  }

  /** Current connection state. */
  get connectionState(): ConnectionState {
    return this.state;
  }

  /** Opens the connection. Resolves once the socket is open. */
  connect(): Promise<void> {
    if (this.state === 'connected') return Promise.resolve();
    if (this.connecting) return this.connecting;

    this.closedByUser = false;
    this.connecting = this.open().finally(() => {
      this.connecting = null;
    });
    return this.connecting;
  }

  /**
   * Subscribes to a channel and resolves once the hub acknowledges it.
   *
   * The channel is remembered, so it is restored automatically after a
   * reconnect without the caller doing anything.
   */
  async subscribe(channel: string): Promise<Subscription> {
    this.desired.add(channel);
    if (this.state === 'connected') {
      await this.send('subscribe', channel);
    }
    return {
      channel,
      unsubscribe: () => this.unsubscribe(channel),
    };
  }

  /** Stops receiving events for a channel. */
  async unsubscribe(channel: string): Promise<void> {
    this.desired.delete(channel);
    if (this.state === 'connected') {
      await this.send('unsubscribe', channel);
    }
  }

  /**
   * Registers a handler for an event name. The name '*' receives every event.
   * Returns a function that removes the handler.
   */
  on<T = unknown>(event: string, handler: EventHandler<T>): () => void {
    let set = this.handlers.get(event);
    if (!set) {
      set = new Set();
      this.handlers.set(event, set);
    }
    set.add(handler as EventHandler<any>);
    return () => this.off(event, handler);
  }

  off<T = unknown>(event: string, handler: EventHandler<T>): void {
    this.handlers.get(event)?.delete(handler as EventHandler<any>);
  }

  /** Observes connection state changes. Returns an unsubscribe function. */
  onStateChange(handler: StateHandler): () => void {
    this.stateHandlers.add(handler);
    return () => {
      this.stateHandlers.delete(handler);
    };
  }

  /** Closes the connection and stops reconnecting. */
  close(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }
    this.failPending(new Error('kabarcast: client closed'));
    this.ws?.close(1000, 'client closed');
    this.ws = null;
    this.setState('closed');
  }

  // ---------------------------------------------------------------- internals

  private async open(): Promise<void> {
    this.setState(this.attempt === 0 ? 'connecting' : 'reconnecting');

    // Fetched on every attempt: channel tokens are short-lived, so a stale one
    // must never be the reason a reconnect fails.
    const token = await this.opts.getToken();
    const base = this.opts.url.replace(/\/$/, '');
    const url = `${base}/v1/ws?token=${encodeURIComponent(token)}`;
    const ws = this.createSocket(url);
    this.ws = ws;

    return new Promise<void>((resolve, reject) => {
      let settled = false;

      ws.onopen = () => {
        settled = true;
        this.attempt = 0;
        this.setState('connected');
        this.log('connected');
        // Restore everything the caller asked for before the drop.
        for (const channel of this.desired) {
          this.send('subscribe', channel).catch((e) =>
            this.log('resubscribe failed', channel, e),
          );
        }
        resolve();
      };

      ws.onmessage = (ev) => this.handleFrame(ev.data);

      ws.onerror = () => {
        if (!settled) {
          settled = true;
          reject(new Error('kabarcast: connection failed'));
        }
      };

      ws.onclose = () => {
        this.ws = null;
        this.failPending(new Error('kabarcast: connection closed'));
        if (this.closedByUser) {
          this.setState('closed');
          return;
        }
        this.scheduleReconnect();
      };
    });
  }

  private createSocket(url: string): WebSocketLike {
    if (this.opts.webSocketFactory) return this.opts.webSocketFactory(url);
    const g = globalThis as { WebSocket?: new (url: string) => WebSocketLike };
    if (!g.WebSocket) {
      throw new Error(
        'kabarcast: no WebSocket available. Pass options.webSocketFactory (for example the ws package on Node < 22).',
      );
    }
    return new g.WebSocket(url);
  }

  private scheduleReconnect(): void {
    if (this.attempt >= this.opts.maxReconnectAttempts) {
      this.log('giving up after', this.attempt, 'attempts');
      this.setState('closed');
      return;
    }
    this.attempt += 1;
    const delay = backoffDelay(
      this.attempt,
      this.opts.minReconnectDelayMs,
      this.opts.maxReconnectDelayMs,
    );
    this.setState('reconnecting');
    this.log(`reconnecting in ${delay}ms (attempt ${this.attempt})`);
    this.reconnectTimer = setTimeout(() => {
      this.open().catch((e) => {
        this.log('reconnect failed', e);
        this.scheduleReconnect();
      });
    }, delay);
  }

  private send(
    action: 'subscribe' | 'unsubscribe',
    channel: string,
  ): Promise<void> {
    const ws = this.ws;
    if (!ws || ws.readyState !== OPEN) {
      return Promise.reject(new Error('kabarcast: not connected'));
    }
    const key = `${action}:${channel}`;
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(key);
        reject(
          new Error(
            `kabarcast: timed out waiting for ${action} ack on ${channel}`,
          ),
        );
      }, this.opts.ackTimeoutMs);

      this.pending.set(key, { resolve, reject, timer });
      ws.send(JSON.stringify({ action, channel }));
    });
  }

  private handleFrame(raw: unknown): void {
    let frame: ServerFrame;
    try {
      frame = JSON.parse(typeof raw === 'string' ? raw : String(raw));
    } catch {
      this.log('dropped unparseable frame');
      return;
    }

    // Control frames carry `type`; broadcast events do not.
    if ('type' in frame) {
      if (frame.type === 'ack') {
        this.settle(`${frame.action}:${frame.channel}`, null);
      } else if (frame.type === 'error') {
        const err = new Error(`kabarcast: ${frame.message}`);
        if (frame.action && frame.channel) {
          // A refused subscribe must not be retried forever on reconnect.
          if (frame.action === 'subscribe') this.desired.delete(frame.channel);
          this.settle(`${frame.action}:${frame.channel}`, err);
        } else {
          this.log('server error', frame.message);
        }
      }
      return;
    }

    this.dispatch(frame);
  }

  private settle(key: string, err: Error | null): void {
    const p = this.pending.get(key);
    if (!p) return;
    clearTimeout(p.timer);
    this.pending.delete(key);
    if (err) {
      p.reject(err);
    } else {
      p.resolve();
    }
  }

  private failPending(err: Error): void {
    for (const [, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(err);
    }
    this.pending.clear();
  }

  private dispatch(ev: KabarcastEvent): void {
    const meta = { channel: ev.channel, event: ev.event, ts: ev.ts };
    for (const key of [ev.event, '*']) {
      const set = this.handlers.get(key);
      if (!set) continue;
      for (const h of set) {
        try {
          h(ev.data, meta);
        } catch (e) {
          this.log('handler threw', e);
        }
      }
    }
  }

  private setState(s: ConnectionState): void {
    if (this.state === s) return;
    this.state = s;
    for (const h of this.stateHandlers) {
      try {
        h(s);
      } catch {
        /* a bad observer must not break the client */
      }
    }
  }

  private log(...args: unknown[]): void {
    if (this.opts.debug) console.debug('[kabarcast]', ...args);
  }
}
