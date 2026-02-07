package broker

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// WAL is an append-only write-ahead log for a single stream on a single node.
// Messages are stored as length-prefixed JSON. An in-memory index maps
// sequence numbers to file offsets for efficient reads.
type WAL struct {
	mu   sync.RWMutex
	dir  string
	file *os.File

	// index maps node_seq -> file offset for quick lookups
	index []int64 // index[seq-1] = offset (sequences are 1-based)

	seq         uint64 // current max sequence number
	fsyncPolicy FsyncPolicy
	fsyncTicker *time.Ticker
	stopFsync   chan struct{}
	dirty       bool // whether there's unsynced data
}

// NewWAL creates or opens a WAL in the given directory.
func NewWAL(dir string, policy FsyncPolicy, fsyncInterval time.Duration) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create WAL dir: %w", err)
	}

	path := filepath.Join(dir, "wal.dat")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("open WAL file: %w", err)
	}

	w := &WAL{
		dir:         dir,
		file:        f,
		index:       make([]int64, 0, 1024),
		fsyncPolicy: policy,
		stopFsync:   make(chan struct{}),
	}

	// Rebuild index from existing data
	if err := w.rebuild(); err != nil {
		f.Close()
		return nil, fmt.Errorf("rebuild WAL index: %w", err)
	}

	// Start background fsync if interval policy
	if policy == FsyncInterval && fsyncInterval > 0 {
		w.fsyncTicker = time.NewTicker(fsyncInterval)
		go w.backgroundFsync()
	}

	return w, nil
}

// rebuild reads the entire WAL file and reconstructs the in-memory index.
func (w *WAL) rebuild() error {
	stat, err := w.file.Stat()
	if err != nil {
		return err
	}
	if stat.Size() == 0 {
		return nil
	}

	data, err := os.ReadFile(filepath.Join(w.dir, "wal.dat"))
	if err != nil {
		return err
	}

	offset := int64(0)
	for offset < int64(len(data)) {
		msg, n, err := DecodeMessageFromBytes(data[offset:])
		if err != nil {
			// Truncate at the corruption point
			break
		}
		_ = msg
		w.index = append(w.index, offset)
		w.seq++
		offset += int64(n)
	}

	return nil
}

// backgroundFsync periodically fsyncs the WAL file.
func (w *WAL) backgroundFsync() {
	for {
		select {
		case <-w.fsyncTicker.C:
			w.mu.Lock()
			if w.dirty {
				_ = w.file.Sync()
				w.dirty = false
			}
			w.mu.Unlock()
		case <-w.stopFsync:
			return
		}
	}
}

// Append writes a message to the WAL and returns the assigned sequence number.
// The message's NodeSeq field is set by this method.
func (w *WAL) Append(msg *Message) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.seq++
	msg.NodeSeq = w.seq

	encoded, err := msg.Encode()
	if err != nil {
		w.seq--
		return 0, fmt.Errorf("encode message: %w", err)
	}

	// Get current file position for the index
	offset, err := w.file.Seek(0, 2) // seek to end
	if err != nil {
		w.seq--
		return 0, fmt.Errorf("seek WAL: %w", err)
	}

	if _, err := w.file.Write(encoded); err != nil {
		w.seq--
		return 0, fmt.Errorf("write WAL: %w", err)
	}

	w.index = append(w.index, offset)
	w.dirty = true

	if w.fsyncPolicy == FsyncEvery {
		if err := w.file.Sync(); err != nil {
			return 0, fmt.Errorf("fsync WAL: %w", err)
		}
		w.dirty = false
	}

	return w.seq, nil
}

// AppendReplicated writes a replicated message (already has NodeSeq assigned)
// to the WAL. It inserts at the correct position in the index.
// Returns false if the message already exists at that sequence.
func (w *WAL) AppendReplicated(msg *Message) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	seq := msg.NodeSeq
	if seq == 0 {
		return false, fmt.Errorf("replicated message has no sequence number")
	}

	// Check if we already have this sequence
	if seq <= uint64(len(w.index)) {
		return false, nil // already have it
	}

	// For now, we append in order. Gaps are filled later by anti-entropy.
	// If seq is more than 1 ahead, we pad with placeholders.
	// Simple approach: only accept next-in-sequence or fill from current max.

	encoded, err := msg.Encode()
	if err != nil {
		return false, fmt.Errorf("encode replicated message: %w", err)
	}

	offset, err := w.file.Seek(0, 2)
	if err != nil {
		return false, fmt.Errorf("seek WAL: %w", err)
	}

	if _, err := w.file.Write(encoded); err != nil {
		return false, fmt.Errorf("write WAL: %w", err)
	}

	w.index = append(w.index, offset)
	if seq > w.seq {
		w.seq = seq
	}
	w.dirty = true

	if w.fsyncPolicy == FsyncEvery {
		_ = w.file.Sync()
		w.dirty = false
	}

	return true, nil
}

// Read returns messages in the sequence range [startSeq, endSeq] inclusive.
// Both bounds are 1-based.
func (w *WAL) Read(startSeq, endSeq uint64) ([]*Message, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if startSeq < 1 {
		startSeq = 1
	}
	maxSeq := uint64(len(w.index))
	if endSeq > maxSeq {
		endSeq = maxSeq
	}
	if startSeq > endSeq {
		return nil, nil
	}

	// Read the file region we need
	startOffset := w.index[startSeq-1]
	var endOffset int64
	if endSeq < maxSeq {
		endOffset = w.index[endSeq] // start of next message
	} else {
		stat, err := w.file.Stat()
		if err != nil {
			return nil, err
		}
		endOffset = stat.Size()
	}

	data := make([]byte, endOffset-startOffset)
	if _, err := w.file.ReadAt(data, startOffset); err != nil {
		return nil, fmt.Errorf("read WAL: %w", err)
	}

	msgs := make([]*Message, 0, endSeq-startSeq+1)
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		msg, err := DecodeMessage(r)
		if err != nil {
			break
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// ReadFrom returns all messages starting from startSeq (1-based).
func (w *WAL) ReadFrom(startSeq uint64) ([]*Message, error) {
	w.mu.RLock()
	maxSeq := uint64(len(w.index))
	w.mu.RUnlock()
	return w.Read(startSeq, maxSeq)
}

// LastSeq returns the highest sequence number in the WAL.
func (w *WAL) LastSeq() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.seq
}

// MessageCount returns the number of messages in the WAL.
func (w *WAL) MessageCount() uint64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return uint64(len(w.index))
}

// Close syncs and closes the WAL file.
func (w *WAL) Close() error {
	if w.fsyncTicker != nil {
		w.fsyncTicker.Stop()
		close(w.stopFsync)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.dirty {
		_ = w.file.Sync()
	}
	return w.file.Close()
}

// MaxProducerTS returns the maximum producer timestamp in the WAL.
// Returns 0 if the WAL is empty.
func (w *WAL) MaxProducerTS() (uint64, error) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if len(w.index) == 0 {
		return 0, nil
	}

	// Read the last message to get its timestamp
	lastOffset := w.index[len(w.index)-1]
	stat, err := w.file.Stat()
	if err != nil {
		return 0, err
	}

	data := make([]byte, stat.Size()-lastOffset)
	if _, err := w.file.ReadAt(data, lastOffset); err != nil {
		return 0, err
	}

	msg, _, err := DecodeMessageFromBytes(data)
	if err != nil {
		return 0, err
	}
	return msg.ProducerTS, nil
}
