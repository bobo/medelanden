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

func TestParseStreamCreate(t *testing.T) {
	input := `STREAM CREATE {"name":"test","subjects":["test.>"]}` + "\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	sc, ok := cmd.(*StreamCreateCommand)
	if !ok {
		t.Fatalf("expected StreamCreateCommand, got %T", cmd)
	}
	if sc.Config.Name != "test" {
		t.Errorf("name: %q", sc.Config.Name)
	}
	if len(sc.Config.Subjects) != 1 || sc.Config.Subjects[0] != "test.>" {
		t.Errorf("subjects: %v", sc.Config.Subjects)
	}
}

func TestParseStreamDelete(t *testing.T) {
	input := "STREAM DELETE mystream\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	sd, ok := cmd.(*StreamDeleteCommand)
	if !ok {
		t.Fatalf("expected StreamDeleteCommand, got %T", cmd)
	}
	if sd.Name != "mystream" {
		t.Errorf("name: %q", sd.Name)
	}
}

func TestParseStreamList(t *testing.T) {
	input := "STREAM LIST\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	if _, ok := cmd.(*StreamListCommand); !ok {
		t.Fatalf("expected StreamListCommand, got %T", cmd)
	}
}

func TestParseStreamInfo(t *testing.T) {
	input := "STREAM INFO mystream\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	si, ok := cmd.(*StreamInfoCommand)
	if !ok {
		t.Fatalf("expected StreamInfoCommand, got %T", cmd)
	}
	if si.Name != "mystream" {
		t.Errorf("name: %q", si.Name)
	}
}

func TestParseConsumerCreate(t *testing.T) {
	input := `CONSUMER CREATE {"name":"my-consumer","stream":"test"}` + "\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	cc, ok := cmd.(*ConsumerCreateCommand)
	if !ok {
		t.Fatalf("expected ConsumerCreateCommand, got %T", cmd)
	}
	if cc.Config.Name != "my-consumer" {
		t.Errorf("name: %q", cc.Config.Name)
	}
	if cc.Config.Stream != "test" {
		t.Errorf("stream: %q", cc.Config.Stream)
	}
}

func TestParseConsumerDelete(t *testing.T) {
	input := "CONSUMER DELETE mystream my-consumer\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	cd, ok := cmd.(*ConsumerDeleteCommand)
	if !ok {
		t.Fatalf("expected ConsumerDeleteCommand, got %T", cmd)
	}
	if cd.Stream != "mystream" {
		t.Errorf("stream: %q", cd.Stream)
	}
	if cd.Name != "my-consumer" {
		t.Errorf("name: %q", cd.Name)
	}
}

func TestParseConsumerList(t *testing.T) {
	input := "CONSUMER LIST mystream\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	cl, ok := cmd.(*ConsumerListCommand)
	if !ok {
		t.Fatalf("expected ConsumerListCommand, got %T", cmd)
	}
	if cl.Stream != "mystream" {
		t.Errorf("stream: %q", cl.Stream)
	}
}

func TestParseConsumerInfo(t *testing.T) {
	input := "CONSUMER INFO mystream my-consumer\r\n"
	parser := NewProtocolParser(strings.NewReader(input))

	cmd, err := parser.ParseCommand()
	if err != nil {
		t.Fatal(err)
	}

	ci, ok := cmd.(*ConsumerInfoCommand)
	if !ok {
		t.Fatalf("expected ConsumerInfoCommand, got %T", cmd)
	}
	if ci.Stream != "mystream" {
		t.Errorf("stream: %q", ci.Stream)
	}
	if ci.Name != "my-consumer" {
		t.Errorf("name: %q", ci.Name)
	}
}
