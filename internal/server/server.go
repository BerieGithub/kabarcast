// Package server exposes the two public surfaces of kabarcast:
//
//	GET  /v1/ws?token=<channel token>   clients connect and subscribe
//	POST /v1/publish                    your backends broadcast an event
//
// plus /healthz and /stats for operations.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"

	"github.com/BerieGithub/kabarcast/internal/auth"
	"github.com/BerieGithub/kabarcast/internal/config"
	"github.com/BerieGithub/kabarcast/internal/fanout"
	"github.com/BerieGithub/kabarcast/internal/hub"
)

type Server struct {
	cfg config.Config
	hub *hub.Hub
	fan *fanout.Fanout
	log *slog.Logger
	up  websocket.Upgrader
}

func New(cfg config.Config, h *hub.Hub, f *fanout.Fanout, log *slog.Logger) *Server {
	return &Server{
		cfg: cfg, hub: h, fan: f, log: log,
		up: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// Browser origin is not a security boundary here: authorisation
			// comes from the signed channel token, not from where the page
			// was served. Tighten this if you need to restrict embedding.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /stats", s.handleStats)
	mux.HandleFunc("GET /v1/ws", s.handleWS)
	mux.HandleFunc("POST /v1/publish", s.handlePublish)
	return mux
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleStats reports connection and delivery counters. It requires the
// service secret: connection counts per channel leak tenant activity, so this
// is not public.
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	if !auth.ValidServiceSecret(bearer(r), s.cfg.ServiceSecret) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid service secret"})
		return
	}
	writeJSON(w, http.StatusOK, s.hub.Stats())
}

// handleWS upgrades a client connection after verifying its channel token.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		token = bearer(r)
	}
	claims, err := auth.VerifyChannelToken(token, s.cfg.ClientTokenSecret)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": err.Error()})
		return
	}

	conn, err := s.up.Upgrade(w, r, nil)
	if err != nil {
		s.log.Warn("upgrade failed", "err", err)
		return
	}

	c := hub.NewClient(newID(), conn, claims, s.hub, hub.Options{
		SendBuffer:     s.cfg.SendBuffer,
		WriteTimeout:   s.cfg.WriteTimeout,
		PongTimeout:    s.cfg.PongTimeout,
		PingInterval:   s.cfg.PingInterval,
		MaxMessageSize: s.cfg.MaxMessageSize,
	}, s.log)

	s.hub.Add(c)
	s.log.Debug("client connected", "client", c.ID, "sub", claims.Subject)

	go c.WritePump()
	go c.ReadPump()
}

type publishRequest struct {
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Data    json.RawMessage `json:"data"`
}

// handlePublish accepts an event from a trusted backend and fans it out.
func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	if !auth.ValidServiceSecret(bearer(r), s.cfg.ServiceSecret) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid service secret"})
		return
	}

	var req publishRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid json body"})
		return
	}
	if req.Channel == "" || req.Event == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "channel and event are required"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	ev := hub.Event{Channel: req.Channel, Event: req.Event, Data: req.Data, TS: time.Now().UnixMilli()}
	delivered, err := s.fan.Publish(ctx, ev)
	if err != nil {
		s.log.Error("publish failed", "err", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "fanout unavailable"})
		return
	}

	// 202: the event is accepted for delivery. Delivery is at-most-once and
	// best effort by design - the durable copy lives in the caller's database.
	writeJSON(w, http.StatusAccepted, map[string]any{
		"status":          "accepted",
		"local_delivered": delivered,
	})
}

func bearer(r *http.Request) string {
	h := r.Header.Get("Authorization")
	if strings.HasPrefix(strings.ToLower(h), "bearer ") {
		return h[7:]
	}
	return ""
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
