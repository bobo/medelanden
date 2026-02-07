package broker

import (
	"strings"
	"testing"
)

func TestParsePub(t *testing.T) {
	input := "PUB aircraft.A1 mykey 1706900000000000 5\r\nhello\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	pub, ok := cmd.(*PubCommand)
	if !ok {
		t.Fatalf("expected PubCommand, got %T", cmd)
	}

	if pub.Subject != "aircraft.A1" {
		t.Errorf("subject: %q", pub.Subject)
	}
	if pub.DedupKey != "mykey" {
		t.Errorf("dedup_key: %q", pub.DedupKey)
	}
	if pub.ProducerTS != 1706900000000000 {
		t.Errorf("producer_ts: %d", pub.ProducerTS)
	}
	if pub.Size != 5 {
		t.Errorf("size: %d", pub.Size)
	}
	if string(pub.Payload) != "hello" {
		t.Errorf("payload: %q", pub.Payload)
	}
}

func TestParseMPub(t *testing.T) {
	input := "MPUB aircraft.A1 msg-123 mykey 1706900000000000 5\r\nhello\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	mpub, ok := cmd.(*MPubCommand)
	if !ok {
		t.Fatalf("expected MPubCommand, got %T", cmd)
	}

	if mpub.Subject != "aircraft.A1" {
		t.Errorf("subject: %q", mpub.Subject)
	}
	if mpub.MsgID != "msg-123" {
		t.Errorf("msg_id: %q", mpub.MsgID)
	}
	if mpub.DedupKey != "mykey" {
		t.Errorf("dedup_key: %q", mpub.DedupKey)
	}
	if mpub.ProducerTS != 1706900000000000 {
		t.Errorf("producer_ts: %d", mpub.ProducerTS)
	}
}

func TestParseSub(t *testing.T) {
	input := "SUB aircraft.> my-consumer\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	sub, ok := cmd.(*SubCommand)
	if !ok {
		t.Fatalf("expected SubCommand, got %T", cmd)
	}

	if sub.Subject != "aircraft.>" {
		t.Errorf("subject: %q", sub.Subject)
	}
	if sub.ConsumerName != "my-consumer" {
		t.Errorf("consumer_name: %q", sub.ConsumerName)
	}
}

func TestParseMSub(t *testing.T) {
	input := "MSUB aircraft.> my-consumer 2s 5s\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	msub, ok := cmd.(*MSubCommand)
	if !ok {
		t.Fatalf("expected MSubCommand, got %T", cmd)
	}

	if msub.Subject != "aircraft.>" {
		t.Errorf("subject: %q", msub.Subject)
	}
	if msub.WindowDuration != "2s" {
		t.Errorf("window_duration: %q", msub.WindowDuration)
	}
	if msub.WatermarkTimeout != "5s" {
		t.Errorf("watermark_timeout: %q", msub.WatermarkTimeout)
	}
}

func TestParseAck(t *testing.T) {
	input := "ACK my-consumer msg-456\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	ack, ok := cmd.(*AckCommand)
	if !ok {
		t.Fatalf("expected AckCommand, got %T", cmd)
	}

	if ack.ConsumerName != "my-consumer" {
		t.Errorf("consumer_name: %q", ack.ConsumerName)
	}
	if ack.MsgID != "msg-456" {
		t.Errorf("msg_id: %q", ack.MsgID)
	}
}

func TestParseAckWindow(t *testing.T) {
	input := "ACKW my-consumer 1706900000000000\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	ack, ok := cmd.(*AckWindowCommand)
	if !ok {
		t.Fatalf("expected AckWindowCommand, got %T", cmd)
	}

	if ack.ConsumerName != "my-consumer" {
		t.Errorf("consumer_name: %q", ack.ConsumerName)
	}
	if ack.WindowEndTS != 1706900000000000 {
		t.Errorf("window_end_ts: %d", ack.WindowEndTS)
	}
}

func TestParsePing(t *testing.T) {
	input := "PING\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := cmd.(*PingCommand); !ok {
		t.Fatalf("expected PingCommand, got %T", cmd)
	}
}

func TestParseResume(t *testing.T) {
	input := "RESUME my-consumer\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	resume, ok := cmd.(*ResumeCommand)
	if !ok {
		t.Fatalf("expected ResumeCommand, got %T", cmd)
	}

	if resume.ConsumerName != "my-consumer" {
		t.Errorf("consumer_name: %q", resume.ConsumerName)
	}
}

func TestParseMultipleCommands(t *testing.T) {
	input := "PING\r\nINFO\r\nPING\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	for i := 0; i < 3; i++ {
		cmd, err := parser.ParseCommand()
		if err != nil {
			t.Fatalf("command %d: %v", i, err)
		}
		if cmd == nil {
			t.Fatalf("command %d is nil", i)
		}
	}
}

func TestFormatOK(t *testing.T) {
	result := FormatOK(42)
	if result != "+OK 42\r\n" {
		t.Errorf("expected '+OK 42\\r\\n', got %q", result)
	}
}

func TestFormatError(t *testing.T) {
	result := FormatError("not found")
	if result != "-ERR not found\r\n" {
		t.Errorf("expected '-ERR not found\\r\\n', got %q", result)
	}
}

func TestFormatWindowBatch(t *testing.T) {
	header := FormatWindowBatchStart("consumer1", 1000, 2000, 3)
	if !strings.HasPrefix(header, "WBATCH consumer1") {
		t.Errorf("unexpected header: %q", header)
	}

	end := FormatWindowBatchEnd()
	if end != "WEND\r\n" {
		t.Errorf("expected 'WEND\\r\\n', got %q", end)
	}
}
