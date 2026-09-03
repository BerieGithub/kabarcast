// Package fanout carries events between hub instances.
//
// Each instance only holds its own connections, so an event published to
// instance A must reach B and C for their subscribers to see it. Redis
// pub/sub is used because this broadcast is EPHEMERAL: every instance gets
// every message, and a message nobody is subscribed to is simply discarded.
//
// Durability is deliberately not this layer's job - the authoritative copy of
// anything that matters already lives in the publisher's own database.
package fanout

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/redis/go-redis/v9"

	"github.com/BerieGithub/kabarcast/internal/hub"
)

const topic = "kabarcast:events"

// Fanout publishes locally and, when Redis is configured, to sibling instances.
type Fanout struct {
	hub *hub.Hub
	rdb *redis.Client
	log *slog.Logger
}

// New returns a Fanout. An empty redisURL yields single-instance mode, where
// Publish delivers only to locally connected clients.
func New(redisURL string, h *hub.Hub, log *slog.Logger) (*Fanout, error) {
	f := &Fanout{hub: h, log: log}
	if redisURL == "" {
		log.Info("fanout: single-instance mode (no KABARCAST_REDIS_URL)")
		return f, nil
	}
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	f.rdb = redis.NewClient(opt)
	log.Info("fanout: redis enabled", "addr", opt.Addr)
	return f, nil
}

// Publish delivers an event. With Redis configured it is broadcast to every
// instance (including this one, via the subscription) so delivery happens in
// exactly one place. Without Redis it is delivered locally straight away.
func (f *Fanout) Publish(ctx context.Context, ev hub.Event) (int, error) {
	if f.rdb == nil {
		return f.hub.Deliver(ev), nil
	}
	payload, err := json.Marshal(ev)
	if err != nil {
		return 0, err
	}
	if err := f.rdb.Publish(ctx, topic, payload).Err(); err != nil {
		return 0, err
	}
	// Local subscriber count is reported by the Run loop, not here.
	return 0, nil
}

// Run subscribes to the Redis topic and delivers inbound events to local
// clients. It blocks until ctx is cancelled. No-op in single-instance mode.
func (f *Fanout) Run(ctx context.Context) {
	if f.rdb == nil {
		<-ctx.Done()
		return
	}
	sub := f.rdb.Subscribe(ctx, topic)
	defer func() { _ = sub.Close() }()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var ev hub.Event
			if err := json.Unmarshal([]byte(msg.Payload), &ev); err != nil {
				f.log.Warn("fanout: bad payload", "err", err)
				continue
			}
			f.hub.Deliver(ev)
		}
	}
}

func (f *Fanout) Close() error {
	if f.rdb != nil {
		return f.rdb.Close()
	}
	return nil
}
