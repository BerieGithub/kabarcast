package hub

import (
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/BerieGithub/kabarcast/internal/auth"
)

// Options carries the timing/limit knobs from config so the hub package does
// not import config directly.
type Options struct {
	SendBuffer     int
	WriteTimeout   time.Duration
	PongTimeout    time.Duration
	PingInterval   time.Duration
	MaxMessageSize int64
}

// Client is one WebSocket connection.
//
// Exactly two goroutines touch the socket: readPump reads, writePump writes.
// gorilla/websocket permits only one concurrent reader and one concurrent
// writer, so all outbound traffic is funnelled through the send channel.
type Client struct {
	ID     string
	conn   *websocket.Conn
	send   chan []byte
	subs   map[string]struct{}
	claims *auth.ChannelClaims
	hub    *Hub
	opts   Options
	log    *slog.Logger

	closeOnce sync.Once
}

func NewClient(id string, conn *websocket.Conn, claims *auth.ChannelClaims, h *Hub, o Options, log *slog.Logger) *Client {
	return &Client{
		ID:     id,
		conn:   conn,
		send:   make(chan []byte, o.SendBuffer),
		subs:   make(map[string]struct{}),
		claims: claims,
		hub:    h,
		opts:   o,
		log:    log,
	}
}

// trySend queues a payload without blocking. False means the outbound buffer
// is full - the client is too slow and will be disconnected.
func (c *Client) trySend(payload []byte) bool {
	select {
	case c.send <- payload:
		return true
	default:
		c.log.Warn("slow consumer dropped", "client", c.ID)
		c.closeSend()
		return false
	}
}

func (c *Client) closeSend() {
	c.closeOnce.Do(func() { close(c.send) })
}

// inbound is a control frame sent by the client.
type inbound struct {
	Action  string `json:"action"`  // subscribe | unsubscribe | ping
	Channel string `json:"channel"`
}

type outbound struct {
	Type    string `json:"type"`              // ack | error | pong
	Action  string `json:"action,omitempty"`
	Channel string `json:"channel,omitempty"`
	Message string `json:"message,omitempty"`
}

func (c *Client) reply(o outbound) {
	if b, err := json.Marshal(o); err == nil {
		c.trySend(b)
	}
}

// ReadPump consumes control frames until the peer disconnects. It owns the
// read side of the socket and must run in its own goroutine.
func (c *Client) ReadPump() {
	defer func() {
		c.hub.Remove(c)
		_ = c.conn.Close()
	}()

	c.conn.SetReadLimit(c.opts.MaxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.opts.PongTimeout))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(c.opts.PongTimeout))
	})

	for {
		_, raw, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				c.log.Debug("read error", "client", c.ID, "err", err)
			}
			return
		}

		var in inbound
		if err := json.Unmarshal(raw, &in); err != nil {
			c.reply(outbound{Type: "error", Message: "malformed frame"})
			continue
		}

		switch in.Action {
		case "subscribe":
			// Authorisation happens here, against the signed token - never
			// against anything the client asserts at connect time.
			if !c.claims.CanSubscribe(in.Channel) {
				c.reply(outbound{Type: "error", Action: "subscribe", Channel: in.Channel,
					Message: "not authorized for this channel"})
				continue
			}
			c.hub.Subscribe(c, in.Channel)
			c.reply(outbound{Type: "ack", Action: "subscribe", Channel: in.Channel})

		case "unsubscribe":
			c.hub.Unsubscribe(c, in.Channel)
			c.reply(outbound{Type: "ack", Action: "unsubscribe", Channel: in.Channel})

		case "ping":
			c.reply(outbound{Type: "pong"})

		default:
			c.reply(outbound{Type: "error", Message: "unknown action"})
		}
	}
}

// WritePump owns the write side: it drains the send channel and emits periodic
// pings so dead peers are detected (TCP alone will not tell us).
func (c *Client) WritePump() {
	ticker := time.NewTicker(c.opts.PingInterval)
	defer func() {
		ticker.Stop()
		_ = c.conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
			if !ok { // hub closed the channel
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(c.opts.WriteTimeout))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
