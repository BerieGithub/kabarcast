package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"

	"github.com/BerieGithub/kabarcast/internal/config"
	"github.com/BerieGithub/kabarcast/internal/fanout"
	"github.com/BerieGithub/kabarcast/internal/hub"
)

const (
	clientSecret  = "test-client-secret"
	serviceSecret = "test-service-secret"
)

func testConfig() config.Config {
	c := config.Load()
	c.ClientTokenSecret = clientSecret
	c.ServiceSecret = serviceSecret
	c.RedisURL = "" // single-instance: deliver straight from memory
	return c
}

// signToken mints the kind of short-lived channel token your application
// issues to a logged-in user.
func signToken(t *testing.T, channels []string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":      "user-1",
		"channels": channels,
		"exp":      time.Now().Add(5 * time.Minute).Unix(),
	})
	s, err := tok.SignedString([]byte(clientSecret))
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return s
}

func newTestServer(t *testing.T) (*httptest.Server, *hub.Hub) {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := hub.New()
	f, err := fanout.New("", h, log)
	if err != nil {
		t.Fatalf("fanout: %v", err)
	}
	return httptest.NewServer(New(testConfig(), h, f, log).Routes()), h
}

func dial(t *testing.T, srv *httptest.Server, token string) *websocket.Conn {
	t.Helper()
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/ws?token=" + token
	c, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

func readJSON(t *testing.T, c *websocket.Conn) map[string]any {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, raw, err := c.ReadMessage()
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal %q: %v", raw, err)
	}
	return m
}

func TestRejectsUnsignedToken(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/ws?token=garbage"
	if _, resp, err := websocket.DefaultDialer.Dial(u, nil); err == nil {
		t.Fatal("expected dial to fail with an invalid token")
	} else if resp != nil && resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestSubscribeAuthorization(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	// Token grants only this user's own channel.
	c := dial(t, srv, signToken(t, []string{"app:user:1"}))
	defer c.Close()

	// Allowed channel -> ack
	_ = c.WriteJSON(map[string]string{"action": "subscribe", "channel": "app:user:1"})
	if got := readJSON(t, c); got["type"] != "ack" {
		t.Fatalf("want ack, got %v", got)
	}

	// Another tenant's channel -> refused. This is the isolation guarantee.
	_ = c.WriteJSON(map[string]string{"action": "subscribe", "channel": "app:user:2"})
	got := readJSON(t, c)
	if got["type"] != "error" {
		t.Fatalf("expected error for unauthorized channel, got %v", got)
	}
}

func TestWildcardGrantAndDelivery(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	// Prefix grant covers the whole company namespace.
	c := dial(t, srv, signToken(t, []string{"app:org:7:*"}))
	defer c.Close()

	_ = c.WriteJSON(map[string]string{"action": "subscribe", "channel": "app:org:7:documents"})
	if got := readJSON(t, c); got["type"] != "ack" {
		t.Fatalf("want ack for wildcard grant, got %v", got)
	}

	body := `{"channel":"app:org:7:documents","event":"document.updated","data":{"id":42}}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/publish", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer "+serviceSecret)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202, got %d", resp.StatusCode)
	}

	got := readJSON(t, c)
	if got["event"] != "document.updated" || got["channel"] != "app:org:7:documents" {
		t.Fatalf("unexpected event delivered: %v", got)
	}
}

func TestPublishRequiresServiceSecret(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/publish",
		bytes.NewBufferString(`{"channel":"c","event":"e"}`))
	req.Header.Set("Authorization", "Bearer wrong-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", resp.StatusCode)
	}
}

func TestStatsRequiresServiceSecret(t *testing.T) {
	srv, _ := newTestServer(t)
	defer srv.Close()

	// Unauthenticated: connection counts leak tenant activity, so this is
	// not a public endpoint.
	resp, err := http.Get(srv.URL + "/stats")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("want 401 without secret, got %d", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/stats", nil)
	req.Header.Set("Authorization", "Bearer "+serviceSecret)
	authed, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get authed: %v", err)
	}
	defer authed.Body.Close()
	if authed.StatusCode != http.StatusOK {
		t.Fatalf("want 200 with secret, got %d", authed.StatusCode)
	}
}
