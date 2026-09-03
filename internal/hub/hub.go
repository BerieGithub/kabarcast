// Package hub keeps the in-memory registry of connections and the channels
// they subscribe to, and delivers events to them.
//
// One Hub instance serves the connections held by ONE process. Delivering an
// event to clients connected to *other* instances is the fan-out layer's job
// (see internal/fanout); the hub only ever touches its own local clients.
package hub

import (
	"encoding/json"
	"sync"
	"time"
)

// Event is the envelope delivered to subscribers and passed between instances.
type Event struct {
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Data    json.RawMessage `json:"data,omitempty"`
	TS      int64           `json:"ts"`
}

type Hub struct {
	mu sync.RWMutex

	// channel name -> set of locally connected subscribers
	channels map[string]map[*Client]struct{}
	// every locally connected client
	clients map[*Client]struct{}

	// counters exposed through Stats
	delivered uint64
	dropped   uint64
}

func New() *Hub {
	return &Hub{
		channels: make(map[string]map[*Client]struct{}),
		clients:  make(map[*Client]struct{}),
	}
}

func (h *Hub) Add(c *Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

// Remove drops the client and every subscription it held.
func (h *Hub) Remove(c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.clients[c]; !ok {
		return
	}
	delete(h.clients, c)
	for ch := range c.subs {
		if set, ok := h.channels[ch]; ok {
			delete(set, c)
			if len(set) == 0 {
				delete(h.channels, ch)
			}
		}
	}
	c.closeSend()
}

func (h *Hub) Subscribe(c *Client, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	set, ok := h.channels[channel]
	if !ok {
		set = make(map[*Client]struct{})
		h.channels[channel] = set
	}
	set[c] = struct{}{}
	c.subs[channel] = struct{}{}
}

func (h *Hub) Unsubscribe(c *Client, channel string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if set, ok := h.channels[channel]; ok {
		delete(set, c)
		if len(set) == 0 {
			delete(h.channels, channel)
		}
	}
	delete(c.subs, channel)
}

// Deliver sends an event to every LOCAL subscriber of the channel and returns
// how many received it.
//
// Backpressure: a client whose outbound buffer is full is not waited on. It is
// marked over-capacity and closed by its own write pump. Blocking here would
// let one slow consumer stall delivery for everyone.
func (h *Hub) Deliver(ev Event) int {
	if ev.TS == 0 {
		ev.TS = time.Now().UnixMilli()
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return 0
	}

	h.mu.RLock()
	subs := make([]*Client, 0, len(h.channels[ev.Channel]))
	for c := range h.channels[ev.Channel] {
		subs = append(subs, c)
	}
	h.mu.RUnlock()

	sent := 0
	for _, c := range subs {
		if c.trySend(payload) {
			sent++
		} else {
			h.mu.Lock()
			h.dropped++
			h.mu.Unlock()
		}
	}

	h.mu.Lock()
	h.delivered += uint64(sent)
	h.mu.Unlock()
	return sent
}

// CloseAll closes every local connection.
//
// Called during shutdown: Go's http.Server.Shutdown does NOT wait for or close
// hijacked connections, and a WebSocket upgrade hijacks. Without this, sockets
// are severed abruptly when the process exits instead of receiving a close
// frame, so clients cannot tell a deploy from a network failure.
func (h *Hub) CloseAll() {
	h.mu.Lock()
	clients := make([]*Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	// Closing the send channel makes each write pump emit a close frame and
	// exit; the read pump then unwinds and deregisters the client.
	for _, c := range clients {
		c.closeSend()
	}
}

type Stats struct {
	Connections int    `json:"connections"`
	Channels    int    `json:"channels"`
	Delivered   uint64 `json:"delivered"`
	DroppedSlow uint64 `json:"dropped_slow_consumers"`
}

func (h *Hub) Stats() Stats {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return Stats{
		Connections: len(h.clients),
		Channels:    len(h.channels),
		Delivered:   h.delivered,
		DroppedSlow: h.dropped,
	}
}
