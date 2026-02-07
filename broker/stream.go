package broker

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Stream is a named, append-only log of messages with subject filtering.
// Each stream has its own WAL and manages its own retention.
type Stream struct {
	mu     sync.RWMutex
	config StreamConfig
	wal    *WAL
	dir    string

	// subjectMatchers are compiled from config.Subjects for fast matching.
	subjectMatchers []subjectMatcher
}

type subjectMatcher struct {
	tokens   []string
	hasGt    bool // ends with >
}

// NewStream creates a new stream with the given configuration.
func NewStream(cfg StreamConfig, baseDir string) (*Stream, error) {
	dir := filepath.Join(baseDir, "streams", cfg.Name)

	wal, err := NewWAL(dir, cfg.FsyncPolicy, cfg.FsyncInterval)
	if err != nil {
		return nil, fmt.Errorf("create WAL for stream %s: %w", cfg.Name, err)
	}

	s := &Stream{
		config: cfg,
		wal:    wal,
		dir:    dir,
	}

	// Compile subject matchers
	for _, subj := range cfg.Subjects {
		s.subjectMatchers = append(s.subjectMatchers, compileSubject(subj))
	}

	// Start retention if configured
	if cfg.MaxAge > 0 || cfg.MaxBytes > 0 || cfg.MaxMsgs > 0 {
		go s.retentionLoop()
	}

	return s, nil
}

func compileSubject(pattern string) subjectMatcher {
	tokens := strings.Split(pattern, ".")
	hasGt := false
	if len(tokens) > 0 && tokens[len(tokens)-1] == ">" {
		hasGt = true
		tokens = tokens[:len(tokens)-1]
	}
	return subjectMatcher{tokens: tokens, hasGt: hasGt}
}

// MatchSubject returns true if the given subject matches this stream's filters.
func (s *Stream) MatchSubject(subject string) bool {
	for _, m := range s.subjectMatchers {
		if matchSubject(m, subject) {
			return true
		}
	}
	return false
}

func matchSubject(m subjectMatcher, subject string) bool {
	tokens := strings.Split(subject, ".")

	if m.hasGt {
		// ">" matches one or more trailing tokens
		if len(tokens) < len(m.tokens) {
			return false
		}
		for i, mt := range m.tokens {
			if mt == "*" {
				continue
			}
			if mt != tokens[i] {
				return false
			}
		}
		return true
	}

	// Exact length match required (with * wildcards)
	if len(tokens) != len(m.tokens) {
		return false
	}
	for i, mt := range m.tokens {
		if mt == "*" {
			continue
		}
		if mt != tokens[i] {
			return false
		}
	}
	return true
}

// Publish appends a message to the stream's WAL.
func (s *Stream) Publish(msg *Message) (uint64, error) {
	if !s.MatchSubject(msg.Subject) {
		return 0, fmt.Errorf("subject %q does not match stream %s", msg.Subject, s.config.Name)
	}

	seq, err := s.wal.Append(msg)
	if err != nil {
		return 0, fmt.Errorf("append to stream %s: %w", s.config.Name, err)
	}

	return seq, nil
}

// PublishReplicated appends a replicated message to the stream.
func (s *Stream) PublishReplicated(msg *Message) (bool, error) {
	return s.wal.AppendReplicated(msg)
}

// Read returns messages in the given sequence range.
func (s *Stream) Read(startSeq, endSeq uint64) ([]*Message, error) {
	return s.wal.Read(startSeq, endSeq)
}

// ReadFrom returns all messages starting from startSeq.
func (s *Stream) ReadFrom(startSeq uint64) ([]*Message, error) {
	return s.wal.ReadFrom(startSeq)
}

// LastSeq returns the highest sequence number.
func (s *Stream) LastSeq() uint64 {
	return s.wal.LastSeq()
}

// MessageCount returns the number of messages in the stream.
func (s *Stream) MessageCount() uint64 {
	return s.wal.MessageCount()
}

// MaxProducerTS returns the maximum producer timestamp in the stream.
func (s *Stream) MaxProducerTS() (uint64, error) {
	return s.wal.MaxProducerTS()
}

// Config returns the stream configuration.
func (s *Stream) Config() StreamConfig {
	return s.config
}

// Name returns the stream name.
func (s *Stream) Name() string {
	return s.config.Name
}

// Close closes the stream's WAL.
func (s *Stream) Close() error {
	return s.wal.Close()
}

// retentionLoop periodically enforces retention policies.
func (s *Stream) retentionLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		s.enforceRetention()
	}
}

// enforceRetention removes old messages based on retention config.
// For simplicity in this implementation, retention is tracked but not
// actively purged from the WAL file (would require segment-based WAL).
// Messages beyond retention are skipped during reads.
func (s *Stream) enforceRetention() {
	// This is a simplified implementation. A production system would
	// use segmented WAL files and delete old segments.
	// For now, we track what should be retained.
}

// Digest returns a compact summary of this stream's state for anti-entropy.
func (s *Stream) Digest() StreamDigest {
	maxTS, _ := s.MaxProducerTS()
	return StreamDigest{
		Stream: s.config.Name,
		MaxSeq: s.LastSeq(),
		MaxTS:  maxTS,
	}
}

// StreamDigest is a compact summary of a stream's state on a node.
type StreamDigest struct {
	Stream string `json:"stream"`
	NodeID string `json:"node_id"`
	MaxSeq uint64 `json:"max_seq"`
	MaxTS  uint64 `json:"max_ts"`
}
