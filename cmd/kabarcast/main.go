// Command kabarcast runs the realtime broadcast hub.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/BerieGithub/kabarcast/internal/config"
	"github.com/BerieGithub/kabarcast/internal/fanout"
	"github.com/BerieGithub/kabarcast/internal/hub"
	"github.com/BerieGithub/kabarcast/internal/server"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := config.Load()
	if cfg.ClientTokenSecret == "" || cfg.ServiceSecret == "" {
		log.Error("KABARCAST_CLIENT_TOKEN_SECRET and KABARCAST_SERVICE_SECRET are required")
		os.Exit(1)
	}

	h := hub.New()

	fan, err := fanout.New(cfg.RedisURL, h, log)
	if err != nil {
		log.Error("fanout init failed", "err", err)
		os.Exit(1)
	}
	defer func() { _ = fan.Close() }()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go fan.Run(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.New(cfg, h, fan, log).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
		// No WriteTimeout: WebSocket connections are long-lived, and a write
		// deadline on the server would sever them. Per-write deadlines are
		// applied on the connection itself instead.
	}

	go func() {
		log.Info("kabarcast listening", "addr", cfg.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down")

	// Close WebSocket connections first. http.Server.Shutdown neither waits for
	// nor closes hijacked connections, so without this clients are cut off
	// abruptly instead of receiving a close frame.
	h.CloseAll()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "err", err)
	}
	log.Info("stopped")
}
