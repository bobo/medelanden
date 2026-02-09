package broker

import (
	"testing"
	"time"
)

func TestStreamConfigValidate(t *testing.T) {
	valid := DefaultStreamConfig("test", []string{"test.>"})

	if err := valid.Validate(); err != nil {
		t.Fatalf("default stream config should be valid: %v", err)
	}

	tests := []struct {
		name   string
		modify func(*StreamConfig)
	}{
		{"empty name", func(c *StreamConfig) { c.Name = "" }},
		{"no subjects", func(c *StreamConfig) { c.Subjects = nil }},
		{"empty subject", func(c *StreamConfig) { c.Subjects = []string{""} }},
		{"negative max_bytes", func(c *StreamConfig) { c.MaxBytes = -1 }},
		{"negative max_age", func(c *StreamConfig) { c.MaxAge = -1 }},
		{"negative max_msgs", func(c *StreamConfig) { c.MaxMsgs = -1 }},
		{"invalid fsync_policy", func(c *StreamConfig) { c.FsyncPolicy = "bogus" }},
		{"interval fsync without interval", func(c *StreamConfig) {
			c.FsyncPolicy = FsyncInterval
			c.FsyncInterval = 0
		}},
		{"negative replication_target", func(c *StreamConfig) { c.ReplicationTarget = -1 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultStreamConfig("test", []string{"test.>"})
			tt.modify(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestConsumerConfigValidate(t *testing.T) {
	valid := DefaultConsumerConfig("my-consumer", "test")

	if err := valid.Validate(); err != nil {
		t.Fatalf("default consumer config should be valid: %v", err)
	}

	tests := []struct {
		name   string
		modify func(*ConsumerConfig)
	}{
		{"empty name", func(c *ConsumerConfig) { c.Name = "" }},
		{"empty stream", func(c *ConsumerConfig) { c.Stream = "" }},
		{"invalid late_policy", func(c *ConsumerConfig) { c.LatePolicy = "bogus" }},
		{"invalid deliver_policy", func(c *ConsumerConfig) { c.DeliverPolicy = "bogus" }},
		{"negative window_duration", func(c *ConsumerConfig) { c.WindowDuration = -1 * time.Second }},
		{"negative watermark_timeout", func(c *ConsumerConfig) { c.WatermarkTimeout = -1 * time.Second }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConsumerConfig("my-consumer", "test")
			tt.modify(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestNodeConfigValidate(t *testing.T) {
	valid := NodeConfig{
		ID:       "node-1",
		DataDir:  "/tmp/data",
		BindAddr: "127.0.0.1:4222",
	}

	if err := valid.Validate(); err != nil {
		t.Fatalf("valid node config should pass: %v", err)
	}

	tests := []struct {
		name   string
		modify func(*NodeConfig)
	}{
		{"empty id", func(c *NodeConfig) { c.ID = "" }},
		{"empty data_dir", func(c *NodeConfig) { c.DataDir = "" }},
		{"empty bind_addr", func(c *NodeConfig) { c.BindAddr = "" }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := valid
			tt.modify(&cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("expected validation error")
			}
		})
	}
}
