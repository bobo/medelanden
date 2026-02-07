package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"medelanden/broker"
)

func main() {
	var (
		nodeID    = flag.String("id", "", "Node ID (required)")
		dataDir   = flag.String("data-dir", "./data", "Data directory")
		bindAddr  = flag.String("bind", "0.0.0.0:4222", "Client TCP bind address")
		peerAddr  = flag.String("peer-addr", "0.0.0.0:4223", "Peer-to-peer gRPC address")
		httpAddr  = flag.String("http", "0.0.0.0:8080", "HTTP health/metrics address")
		seeds     = flag.String("seeds", "", "Comma-separated list of seed peer addresses")
		streamDef = flag.String("stream", "", "JSON stream definition to create on startup")
	)
	flag.Parse()

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
		ID:       *nodeID,
		DataDir:  *dataDir,
		BindAddr: *bindAddr,
		PeerAddr: *peerAddr,
		Seeds:    seedList,
	}

	node, err := broker.NewNode(cfg)
	if err != nil {
		log.Fatalf("Failed to create node: %v", err)
	}

	// Create stream from definition if provided
	if *streamDef != "" {
		var streamCfg broker.StreamConfig
		if err := json.Unmarshal([]byte(*streamDef), &streamCfg); err != nil {
			log.Fatalf("Failed to parse stream definition: %v", err)
		}
		if streamCfg.FsyncPolicy == "" {
			streamCfg.FsyncPolicy = broker.FsyncInterval
		}
		if streamCfg.FsyncInterval == 0 {
			streamCfg.FsyncInterval = 100 * time.Millisecond
		}
		if err := node.CreateStream(streamCfg); err != nil {
			log.Fatalf("Failed to create stream: %v", err)
		}
		log.Printf("Created stream %q with subjects %v", streamCfg.Name, streamCfg.Subjects)
	}

	if err := node.Start(); err != nil {
		log.Fatalf("Failed to start node: %v", err)
	}

	// Start TCP server
	server := broker.NewServer(*bindAddr, node)
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start TCP server: %v", err)
	}
	log.Printf("TCP server listening on %s", server.Addr())

	// Start HTTP server
	if err := server.StartHTTP(*httpAddr); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
	log.Printf("HTTP server listening on %s", *httpAddr)

	log.Printf("Medelanden node %q started", *nodeID)

	// Wait for signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	fmt.Printf("\nReceived %v, shutting down...\n", sig)

	server.Stop()
	node.Stop()
	log.Println("Shutdown complete")
}
