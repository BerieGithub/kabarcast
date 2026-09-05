/** An event broadcast by your backend and delivered to subscribers. */
export interface KabarcastEvent<T = unknown> {
  /** Channel the event was published to, e.g. "app:user:123". */
  channel: string;
  /** Event name, e.g. "notification.created". */
  event: string;
  /** Application payload. */
  data: T;
  /** Server timestamp, epoch milliseconds. */
  ts: number;
}

/** Control frames the server sends in response to client actions. */
export type ControlFrame =
  | { type: 'ack'; action: 'subscribe' | 'unsubscribe'; channel: string }
  | { type: 'error'; action?: string; channel?: string; message: string }
  | { type: 'pong' };

export type ServerFrame = ControlFrame | KabarcastEvent;

export type ConnectionState =
  | 'idle'
  | 'connecting'
  | 'connected'
  | 'reconnecting'
  | 'closed';

/** Minimal WebSocket surface, so browser, Node and `ws` all satisfy it. */
export interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((ev: any) => void) | null;
  onclose: ((ev: any) => void) | null;
  onerror: ((ev: any) => void) | null;
  onmessage: ((ev: { data: any }) => void) | null;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

export interface KabarcastOptions {
  /** Base URL of the hub, e.g. "wss://kabarcast.example.com". */
  url: string;

  /**
   * Returns a short-lived channel token minted by YOUR backend. Called on
   * every connect attempt, so an expired token never blocks a reconnect.
   */
  getToken: () => string | Promise<string>;

  /** Reconnect backoff floor. Default 500ms. */
  minReconnectDelayMs?: number;
  /** Reconnect backoff ceiling. Default 30_000ms. */
  maxReconnectDelayMs?: number;
  /** Give up after this many consecutive failures. Default Infinity. */
  maxReconnectAttempts?: number;
  /** How long to wait for a subscribe/unsubscribe ack. Default 10_000ms. */
  ackTimeoutMs?: number;
  /** Supply a WebSocket implementation (e.g. `ws` on older Node). */
  webSocketFactory?: WebSocketFactory;
  /** Log connection lifecycle to console. Default false. */
  debug?: boolean;
}

export type EventHandler<T = unknown> = (
  data: T,
  meta: { channel: string; event: string; ts: number },
) => void;

export type StateHandler = (state: ConnectionState) => void;

/** Handle returned by subscribe(); call unsubscribe() to stop listening. */
export interface Subscription {
  channel: string;
  unsubscribe: () => Promise<void>;
}
