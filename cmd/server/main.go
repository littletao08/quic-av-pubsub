package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/yourorg/quic-pubsub/pkg/broker"
	"github.com/yourorg/quic-pubsub/pkg/transport"
)

func main() {
	addr := flag.String("addr", ":4430", "QUIC 监听地址")
	cert := flag.String("cert", "./certs/server.crt", "TLS 证书")
	key := flag.String("key", "./certs/server.key", "TLS 私钥")
	verbose := flag.Bool("v", false, "详细日志")
	flag.Parse()

	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	b := broker.NewBroker(logger)
	b.StartStatsLogger(30 * time.Second)

	srv := transport.NewServer(transport.ServerConfig{
		ListenAddr: *addr,
		TLSCert:    *cert,
		TLSKey:     *key,
	}, b, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := srv.Start(ctx); err != nil {
		logger.Error("server start failed", "err", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	logger.Info("shutting down...")
	cancel()
	srv.Stop()
}
