package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"medelanden/broker"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	var (
		nodeID        = flag.String("id", "", "Node ID (required)")
		dataDir       = flag.String("data-dir", "./data", "Data directory")
		bindAddr      = flag.String("bind", "0.0.0.0:4222", "Client TCP bind address")
		peerAddr      = flag.String("peer-addr", "0.0.0.0:4223", "Peer-to-peer gRPC listen address")
		advertisePeer = flag.String("advertise-peer-addr", "", "Address advertised to peers for gRPC (defaults to peer-addr)")
		httpAddr      = flag.String("http", "0.0.0.0:8080", "HTTP health/metrics address")
		seeds         = flag.String("seeds", "", "Comma-separated list of seed peer addresses")
		streamDef     = flag.String("stream", "", "JSON stream definition to create on startup")
		logLevel        = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		shutdownTimeout = flag.String("shutdown-timeout", "15s", "Graceful shutdown timeout")
		maxConnections  = flag.Int("max-connections", 0, "Max client connections (0 = default 10000)")
		maxStreams      = flag.Int("max-streams", 0, "Max streams (0 = default 1024)")
		maxConsumers    = flag.Int("max-consumers", 0, "Max consumers (0 = default 4096)")
		maxMessageSize  = flag.Int64("max-message-size", 0, "Max message size bytes (0 = default 64MB)")
		tlsCert         = flag.String("tls-cert", "", "TLS certificate file")
		tlsKey          = flag.String("tls-key", "", "TLS key file")
		tlsCA           = flag.String("tls-ca", "", "TLS CA certificate file for mutual TLS")
		authKeys        = flag.String("auth-keys", "", "Comma-separated base64-encoded Ed25519 public keys")
	)
	flag.Parse()

	// Initialize logger
	level, err := zapcore.ParseLevel(*logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid log level %q: %v\n", *logLevel, err)
		os.Exit(1)
	}
	zapCfg := zap.NewProductionConfig()
	zapCfg.Level.SetLevel(level)
	logger, err := zapCfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to init logger: %v\n", err)
		os.Exit(1)
	}
	defer logger.Sync()

	if *nodeID == "" {
		hostname, _ := os.Hostname()
		if hostname == "" {
			hostname = "node-1"
		}
		*nodeID = hostname
	}

	var seedList []string
	if *seeds != "" {
		seedList = strings.Split(*seeds, ",")
	}

	cfg := broker.NodeConfig{
		ID:                *nodeID,
		DataDir:           *dataDir,
		BindAddr:          *bindAddr,
		PeerAddr:          *peerAddr,
		AdvertisePeerAddr: *advertisePeer,
		Seeds:             seedList,
		Logger:            logger,
	}

	if *maxConnections > 0 {
		cfg.Limits.MaxConnections = *maxConnections
	}
	if *maxStreams > 0 {
		cfg.Limits.MaxStreams = *maxStreams
	}
	if *maxConsumers > 0 {
		cfg.Limits.MaxConsumers = *maxConsumers
	}
	if *maxMessageSize > 0 {
		cfg.Limits.MaxMessageSize = *maxMessageSize
	}

	if *tlsCert != "" && *tlsKey != "" {
		cfg.TLS = broker.TLSConfig{
			Enabled:  true,
			CertFile: *tlsCert,
			KeyFile:  *tlsKey,
			CAFile:   *tlsCA,
		}
	}

	if *authKeys != "" {
		cfg.Auth = broker.AuthConfig{
			Enabled:        true,
			AuthorizedKeys: strings.Split(*authKeys, ","),
		}
	}

	node, err := broker.NewNode(cfg)
	if err != nil {
		logger.Fatal("failed to create node", zap.Error(err))
	}

	// Create stream from definition if provided
	if *streamDef != "" {
		var streamCfg broker.StreamConfig
		if err := json.Unmarshal([]byte(*streamDef), &streamCfg); err != nil {
			logger.Fatal("failed to parse stream definition", zap.Error(err))
		}
		if streamCfg.FsyncPolicy == "" {
			streamCfg.FsyncPolicy = broker.FsyncInterval
		}
		if streamCfg.FsyncInterval == 0 {
			streamCfg.FsyncInterval = 100 * time.Millisecond
		}
		if err := node.CreateStream(streamCfg); err != nil {
			logger.Fatal("failed to create stream", zap.Error(err))
		}
	}

	if err := node.Start(); err != nil {
		logger.Fatal("failed to start node", zap.Error(err))
	}

	// Start TCP server
	server := broker.NewServer(*bindAddr, node)
	if err := server.Start(); err != nil {
		logger.Fatal("failed to start TCP server", zap.Error(err))
	}
	logger.Info("TCP server listening", zap.String("addr", server.Addr()))

	// Start HTTP server
	if err := server.StartHTTP(*httpAddr); err != nil {
		logger.Fatal("failed to start HTTP server", zap.Error(err))
	}
	logger.Info("HTTP server listening", zap.String("addr", *httpAddr))

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logger.Info("received signal, shutting down", zap.String("signal", sig.String()))

	timeout, err := time.ParseDuration(*shutdownTimeout)
	if err != nil {
		timeout = 15 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	server.Stop(ctx)
	node.Stop(ctx)
	logger.Info("shutdown complete")
}
