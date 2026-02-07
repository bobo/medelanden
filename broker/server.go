package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server handles client TCP connections and the HTTP health/metrics endpoints.
type Server struct {
	mu       sync.Mutex
	node     *Node
	listener net.Listener
	addr     string
	conns    map[net.Conn]struct{}
	stopCh   chan struct{}
	stopped  bool

	httpServer *http.Server
	httpAddr   string
}

// NewServer creates a new TCP server for the wire protocol.
func NewServer(addr string, node *Node) *Server {
	return &Server{
		node:   node,
		addr:   addr,
		conns:  make(map[net.Conn]struct{}),
		stopCh: make(chan struct{}),
	}
}

// Start begins listening for client connections.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.addr, err)
	}
	s.listener = ln

	go s.acceptLoop()
	return nil
}

// StartHTTP starts the HTTP health/metrics server on the given address.
func (s *Server) StartHTTP(addr string) error {
	s.httpAddr = addr
	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", s.handleHealth)
	mux.HandleFunc("/metrics/json", s.handleMetrics)

	// Prometheus metrics endpoint: merge the node's custom registry with Go
	// runtime/process collectors via a gatherer that combines both.
	promHandler := promhttp.HandlerFor(
		prometheus.Gatherers{
			s.node.PrometheusRegistry(),
			prometheus.DefaultGatherer,
		},
		promhttp.HandlerOpts{},
	)
	mux.Handle("/metrics", promHandler)

	// Register replication endpoints if the node has a replicator
	if s.node.replicator != nil {
		s.node.replicator.RegisterHTTPHandlers(mux)
	}

	// Register cluster endpoints
	if s.node.cluster != nil {
		// The cluster HTTP server runs on peerAddr, not here.
		// But we expose a health summary here.
	}

	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := s.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.stopCh:
				return
			default:
				log.Printf("accept error: %v", err)
				continue
			}
		}

		s.mu.Lock()
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	pm := s.node.PromMetrics()
	pm.TCPConnections.Inc()
	defer func() {
		pm.TCPConnections.Dec()
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
	}()

	parser := NewProtocolParser(conn)
	writer := bufio.NewWriter(conn)

	// Active subscriptions for this connection
	subs := make(map[string]*Consumer)

	for {
		select {
		case <-s.stopCh:
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		cmd, err := parser.ParseCommand()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				// Send ping to keep alive
				writer.WriteString(FormatPong())
				writer.Flush()
				continue
			}
			return
		}

		switch c := cmd.(type) {
		case *PubCommand:
			pm.TCPCommandsTotal.WithLabelValues("PUB").Inc()
			s.handlePub(c, writer)
		case *MPubCommand:
			pm.TCPCommandsTotal.WithLabelValues("MPUB").Inc()
			s.handleMPub(c, writer)
		case *SubCommand:
			pm.TCPCommandsTotal.WithLabelValues("SUB").Inc()
			s.handleSub(c, writer, conn, subs)
		case *MSubCommand:
			pm.TCPCommandsTotal.WithLabelValues("MSUB").Inc()
			s.handleMSub(c, writer, conn, subs)
		case *AckCommand:
			pm.TCPCommandsTotal.WithLabelValues("ACK").Inc()
			s.handleAck(c, writer, subs)
		case *AckWindowCommand:
			pm.TCPCommandsTotal.WithLabelValues("ACKW").Inc()
			s.handleAckWindow(c, writer, subs)
		case *ResumeCommand:
			pm.TCPCommandsTotal.WithLabelValues("RESUME").Inc()
			s.handleResume(c, writer)
		case *PingCommand:
			pm.TCPCommandsTotal.WithLabelValues("PING").Inc()
			writer.WriteString(FormatPong())
		case *InfoCommand:
			pm.TCPCommandsTotal.WithLabelValues("INFO").Inc()
			s.handleInfo(writer)
		}
		writer.Flush()
	}
}

func (s *Server) handlePub(cmd *PubCommand, w *bufio.Writer) {
	msg := &Message{
		Subject:    cmd.Subject,
		Payload:    cmd.Payload,
		ProducerTS: cmd.ProducerTS,
		DedupKey:   cmd.DedupKey,
		MsgID:      GenerateID(),
	}

	seq, err := s.node.Publish(msg)
	if err != nil {
		w.WriteString(FormatError(err.Error()))
		return
	}
	w.WriteString(FormatOK(seq))
}

func (s *Server) handleMPub(cmd *MPubCommand, w *bufio.Writer) {
	msg := &Message{
		Subject:    cmd.Subject,
		Payload:    cmd.Payload,
		ProducerTS: cmd.ProducerTS,
		DedupKey:   cmd.DedupKey,
		MsgID:      cmd.MsgID,
	}

	seq, err := s.node.Publish(msg)
	if err != nil {
		w.WriteString(FormatError(err.Error()))
		return
	}
	w.WriteString(FormatOK(seq))
}

func (s *Server) handleSub(cmd *SubCommand, w *bufio.Writer, conn net.Conn, subs map[string]*Consumer) {
	streamName := s.node.FindStreamForSubject(cmd.Subject)
	if streamName == "" {
		w.WriteString(FormatError("no stream for subject"))
		return
	}

	cfg := DefaultConsumerConfig(cmd.ConsumerName, streamName)
	cfg.SubjectFilter = cmd.Subject
	cfg.WindowDuration = 0 // No windowing for simple SUB

	consumer, err := s.node.CreateConsumer(cfg)
	if err != nil {
		w.WriteString(FormatError(err.Error()))
		return
	}

	subs[cmd.ConsumerName] = consumer
	w.WriteString(fmt.Sprintf("+OK\r\n"))
	w.Flush()

	// Start delivery goroutine
	go s.deliverSimple(consumer, conn, cmd.ConsumerName)
}

func (s *Server) handleMSub(cmd *MSubCommand, w *bufio.Writer, conn net.Conn, subs map[string]*Consumer) {
	streamName := s.node.FindStreamForSubject(cmd.Subject)
	if streamName == "" {
		w.WriteString(FormatError("no stream for subject"))
		return
	}

	windowDur, _ := time.ParseDuration(cmd.WindowDuration)
	wmTimeout, _ := time.ParseDuration(cmd.WatermarkTimeout)

	cfg := DefaultConsumerConfig(cmd.ConsumerName, streamName)
	cfg.SubjectFilter = cmd.Subject
	cfg.WindowDuration = windowDur
	cfg.WatermarkTimeout = wmTimeout

	consumer, err := s.node.CreateConsumer(cfg)
	if err != nil {
		w.WriteString(FormatError(err.Error()))
		return
	}

	subs[cmd.ConsumerName] = consumer
	w.WriteString(fmt.Sprintf("+OK\r\n"))
	w.Flush()

	// Start delivery goroutine
	go s.deliverWindowed(consumer, conn, cmd.ConsumerName)
}

func (s *Server) handleAck(cmd *AckCommand, w *bufio.Writer, subs map[string]*Consumer) {
	// ACK is informational in this implementation
	w.WriteString(fmt.Sprintf("+OK\r\n"))
}

func (s *Server) handleAckWindow(cmd *AckWindowCommand, w *bufio.Writer, subs map[string]*Consumer) {
	w.WriteString(fmt.Sprintf("+OK\r\n"))
}

func (s *Server) handleResume(cmd *ResumeCommand, w *bufio.Writer) {
	consumer := s.node.GetConsumer(cmd.ConsumerName)
	if consumer == nil {
		w.WriteString(FormatError("consumer not found"))
		return
	}

	positions := consumer.Positions()
	watermark := consumer.Watermark()

	posMap := ConsumerPositionMap{
		Consumer:  cmd.ConsumerName,
		Positions: positions,
		Watermark: watermark,
	}
	data, _ := json.Marshal(posMap)
	w.WriteString(fmt.Sprintf("+POSITIONS %s\r\n", string(data)))
}

func (s *Server) handleInfo(w *bufio.Writer) {
	info := s.node.Info()
	data, _ := json.Marshal(info)
	w.WriteString(fmt.Sprintf("+INFO %s\r\n", string(data)))
}

func (s *Server) deliverSimple(consumer *Consumer, conn net.Conn, consumerName string) {
	ch := consumer.Output()
	writer := bufio.NewWriter(conn)

	for {
		select {
		case batch, ok := <-ch:
			if !ok {
				return
			}
			for _, msg := range batch.Messages {
				writer.WriteString(FormatMsg(msg.Subject, consumerName, msg.ProducerTS, msg.MsgID, msg.Payload))
			}
			writer.Flush()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) deliverWindowed(consumer *Consumer, conn net.Conn, consumerName string) {
	ch := consumer.Output()
	writer := bufio.NewWriter(conn)

	for {
		select {
		case batch, ok := <-ch:
			if !ok {
				return
			}
			writer.WriteString(FormatWindowBatchStart(consumerName, batch.WindowStart, batch.WindowEnd, len(batch.Messages)))
			for _, msg := range batch.Messages {
				writer.WriteString(FormatWindowBatchMsg(msg.Subject, msg.ProducerTS, msg.MsgID, msg.Payload))
			}
			writer.WriteString(FormatWindowBatchEnd())
			writer.Flush()
		case <-s.stopCh:
			return
		}
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	info := s.node.Info()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := s.node.Metrics()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

// Addr returns the listener address.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.addr
}

// Stop shuts down the server.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	s.mu.Unlock()

	close(s.stopCh)

	if s.listener != nil {
		s.listener.Close()
	}
	if s.httpServer != nil {
		s.httpServer.Close()
	}

	s.mu.Lock()
	for conn := range s.conns {
		conn.Close()
	}
	s.mu.Unlock()

	return nil
}
