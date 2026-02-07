package client_test

import (
	"fmt"
	"time"

	"medelanden/client"
)

func Example_publish() {
	c, err := client.Dial("localhost:4222")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer c.Close()

	seq, err := c.Publish("aircraft.N123AB", []byte(`{"alt":35000,"lat":40.6,"lon":-73.7}`),
		client.WithDedupKey("N123AB|1700000000"),
		client.WithProducerTS(1700000000000000000))
	if err != nil {
		fmt.Println("publish:", err)
		return
	}
	fmt.Println("published, seq:", seq)
}

func Example_publishWithMsgID() {
	c, err := client.Dial("localhost:4222")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer c.Close()

	seq, err := c.Publish("sensors.temp", []byte(`{"value":22.5}`),
		client.WithMsgID("custom-uuid-here"),
		client.WithDedupKey("sensor-1"))
	if err != nil {
		fmt.Println("publish:", err)
		return
	}
	fmt.Println("published with explicit ID, seq:", seq)
}

func Example_subscribe() {
	c, err := client.Dial("localhost:4222")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer c.Close()

	sub, err := c.Subscribe("aircraft.>", "position-display")
	if err != nil {
		fmt.Println("subscribe:", err)
		return
	}

	// Process messages
	for msg := range sub.Messages() {
		fmt.Printf("subject=%s ts=%d payload=%s\n",
			msg.Subject, msg.ProducerTS, msg.Payload)

		// Acknowledge each message
		if err := c.Ack("position-display", msg.MsgID); err != nil {
			fmt.Println("ack:", err)
		}
	}
}

func Example_subscribeWindowed() {
	c, err := client.Dial("localhost:4222")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer c.Close()

	sub, err := c.SubscribeWindowed("aircraft.>", "batch-processor",
		2*time.Second,  // window duration
		5*time.Second)  // watermark timeout
	if err != nil {
		fmt.Println("subscribe:", err)
		return
	}

	// Process ordered, deduplicated batches
	for batch := range sub.Batches() {
		fmt.Printf("window [%d, %d]: %d messages\n",
			batch.WindowStart, batch.WindowEnd, len(batch.Messages))

		for _, msg := range batch.Messages {
			fmt.Printf("  %s: %s\n", msg.Subject, msg.Payload)
		}

		// Acknowledge the entire window
		if err := c.AckWindow("batch-processor", batch.WindowEnd); err != nil {
			fmt.Println("ackw:", err)
		}
	}
}

func Example_resume() {
	c, err := client.Dial("localhost:4222")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer c.Close()

	// After failover, retrieve saved consumer positions
	pos, err := c.Resume("batch-processor")
	if err != nil {
		fmt.Println("resume:", err)
		return
	}

	fmt.Printf("consumer=%s watermark=%d\n", pos.Consumer, pos.Watermark)
	for nodeID, src := range pos.Positions {
		fmt.Printf("  %s: seq=%d ts=%d\n", nodeID, src.LastSeq, src.LastTS)
	}
}

func Example_healthCheck() {
	c, err := client.Dial("localhost:4222")
	if err != nil {
		fmt.Println("connect:", err)
		return
	}
	defer c.Close()

	// Ping checks connectivity
	if err := c.Ping(); err != nil {
		fmt.Println("broker is down:", err)
		return
	}
	fmt.Println("broker is alive")

	// Info returns detailed node state
	info, err := c.Info()
	if err != nil {
		fmt.Println("info:", err)
		return
	}
	fmt.Printf("node=%s status=%s streams=%d peers=%d\n",
		info.NodeID, info.Status, len(info.Streams), len(info.Peers))
}
