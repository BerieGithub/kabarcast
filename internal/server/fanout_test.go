package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/BerieGithub/kabarcast/internal/fanout"
	"github.com/BerieGithub/kabarcast/internal/hub"
)

// instance is one hub process: its own connection registry and HTTP surface,
// sharing only Redis with its peers - exactly how it runs behind a load
// balancer.
type instance struct {
	srv *httptest.Server
	hub *hub.Hub
	fan *fanout.Fanout
}

func newInstance(t *testing.T, ctx context.Context, redisURL string) *instance {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	cfg := testConfig()
	cfg.RedisURL = redisURL

	h := hub.New()
	f, err := fanout.New(redisURL, h, log)
	if err != nil {
		t.Fatalf("fanout: %v", err)
	}
	go f.Run(ctx)

	srv := httptest.NewServer(New(cfg, h, f, log).Routes())
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = f.Close() })

	return &instance{srv: srv, hub: h, fan: f}
}

// TestRedisFanoutAcrossInstances is the property the whole design rests on:
// a client connected to instance B must receive an event published to
// instance A. Without it, horizontal scaling silently drops messages for
// every client that happens to land on a different instance.
func TestRedisFanoutAcrossInstances(t *testing.T) {
	mr := miniredis.RunT(t)
	redisURL := "redis://" + mr.Addr()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher := newInstance(t, ctx, redisURL) // A: receives the HTTP publish
	holder := newInstance(t, ctx, redisURL)    // B: holds the WebSocket client

	// Give both Run loops time to establish their Redis subscriptions before
	// anything is published, otherwise the event races the subscribe.
	time.Sleep(150 * time.Millisecond)

	const channel = "app:org:7:documents"

	c := dial(t, holder.srv, signToken(t, []string{channel}))
	defer c.Close()

	_ = c.WriteJSON(map[string]string{"action": "subscribe", "channel": channel})
	if got := readJSON(t, c); got["type"] != "ack" {
		t.Fatalf("want subscribe ack, got %v", got)
	}

	// Publish to A. The client is on B.
	body := `{"channel":"` + channel + `","event":"document.updated","data":{"id":42}}`
	req, _ := http.NewRequest(http.MethodPost, publisher.srv.URL+"/v1/publish", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+serviceSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202 from publisher instance, got %d", resp.StatusCode)
	}

	got := readJSON(t, c)
	if got["event"] != "document.updated" {
		t.Fatalf("event did not cross instances: %v", got)
	}
	if got["channel"] != channel {
		t.Fatalf("wrong channel delivered: %v", got)
	}
}

// TestRedisFanoutDeliversOnce guards against the event being delivered twice
// to a client on the instance that accepted the publish - once locally and
// once again off the Redis subscription.
func TestRedisFanoutDeliversOnce(t *testing.T) {
	mr := miniredis.RunT(t)
	redisURL := "redis://" + mr.Addr()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	inst := newInstance(t, ctx, redisURL)
	time.Sleep(150 * time.Millisecond)

	const channel = "app:user:1"
	c := dial(t, inst.srv, signToken(t, []string{channel}))
	defer c.Close()

	_ = c.WriteJSON(map[string]string{"action": "subscribe", "channel": channel})
	if got := readJSON(t, c); got["type"] != "ack" {
		t.Fatalf("want ack, got %v", got)
	}

	body := `{"channel":"` + channel + `","event":"notification.created","data":{"n":1}}`
	req, _ := http.NewRequest(http.MethodPost, inst.srv.URL+"/v1/publish", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+serviceSecret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()

	if got := readJSON(t, c); got["event"] != "notification.created" {
		t.Fatalf("unexpected first frame: %v", got)
	}

	// A second copy must not arrive.
	_ = c.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, extra, err := c.ReadMessage(); err == nil {
		t.Fatalf("event delivered twice: %s", extra)
	}
}
