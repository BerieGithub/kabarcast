// Package config loads runtime configuration from the environment.
package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr string

	// ClientTokenSecret is the HMAC secret your applications
	// use to sign short-lived channel tokens. The hub only VERIFIES these
	// signatures, which is what keeps it decoupled from your databases.
	ClientTokenSecret string

	// ServiceSecret is the bearer token your backends present when calling
	// POST /v1/publish.
	ServiceSecret string

	// RedisURL, when set, enables cross-instance fan-out. Empty means a
	// single instance delivering from memory.
	RedisURL string

	// SendBuffer is the per-connection outbound queue depth. A client that
	// fills it is a slow consumer and gets disconnected instead of being
	// allowed to grow the process heap without bound.
	SendBuffer int

	WriteTimeout time.Duration
	PongTimeout  time.Duration
	PingInterval time.Duration
	MaxMessageSize int64
}

func Load() Config {
	c := Config{
		Addr:              env("KABARCAST_ADDR", ":8080"),
		ClientTokenSecret: env("KABARCAST_CLIENT_TOKEN_SECRET", ""),
		ServiceSecret:     env("KABARCAST_SERVICE_SECRET", ""),
		RedisURL:          env("KABARCAST_REDIS_URL", ""),
		SendBuffer:        envInt("KABARCAST_SEND_BUFFER", 256),
		WriteTimeout:      10 * time.Second,
		PongTimeout:       60 * time.Second,
		MaxMessageSize:    32 * 1024,
	}
	// Ping comfortably inside the pong deadline so a healthy but idle
	// connection is never mistaken for a dead one.
	c.PingInterval = (c.PongTimeout * 9) / 10
	return c
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
