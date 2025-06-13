package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Payload struct {
	Event     string `json:"event"`
	UserID    string `json:"user_id"`
	Timestamp string `json:"timestamp"`
}

var topic string = "test-topic"

const (
	groupID          = "go-consumer-group"
	bootstrapServers = "localhost:9092,localhost:10092,localhost:11092"
	msgCount         = 5
	// bootstrapServers = "localhost:9092"
)

func main() {

	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.LUTC | log.Lshortfile)

	// connect to kafka
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"security.protocol": "PLAINTEXT",
	})

	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
		panic(err)
	} else {
		log.Printf("Producer %v created successfully", producer)
	}
	// Delivery report handler for produced messages
	done := make(chan struct{})

	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					log.Printf("Delivered to %v | Topic Partition: %v \n", ev.TopicPartition, ev.TopicPartition.Partition)
				}
			}
		}
		close(done) // Signal that delivery reports are done
	}()

	for i := 0; i < msgCount; i++ {
		// Produce a message to a topic
		payload := &Payload{
			Event:     "user_signup",
			UserID:    "abc123",
			Timestamp: time.Now().String(),
		}

		jsonBytes, err := json.Marshal(payload)
		if err != nil {
			log.Println("Error encoding JSON:", err)
			return
		}
		msg := &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          jsonBytes,
			Key:            []byte("user_signup_event"),
			Timestamp:      time.Now(),
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte("user_signup")},
				{Key: "user_id", Value: []byte("abc123")},
			},
		}
		err = producer.Produce(msg, nil)
		if err != nil {
			log.Fatal("Failed to produce message:", err)
			panic(err)
		}

	}
	// Wait for all messages to be delivered
	producer.Flush(10000)
	producer.Close()

	<-done

	wg := &sync.WaitGroup{}

	for i := 0; i < msgCount; i++ {
		wg.Add(1)
		go consume(topic, wg)
	}
	wg.Wait()
}

func consume(topic string, wg *sync.WaitGroup) {
	defer wg.Done()
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupID,
		"auto.offset.reset": "earliest",
		// "auto.offset.reset":       "latest",
		"session.timeout.ms":      15000, // Increased from 6s
		"heartbeat.interval.ms":   3000,  // Increased from 2s
		"max.poll.interval.ms":    300000,
		"enable.auto.commit":      true,
		"socket.keepalive.enable": true,
		"log.connection.close":    false,
		// "debug":                   "consumer,broker", // Enable debugging
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	defer consumer.Close()

	// Verify topic exists
	metadata, err := consumer.GetMetadata(&topic, false, 10*1000)
	if err != nil {
		log.Fatalf("Failed to get metadata: %v", err)
	}
	if len(metadata.Topics[topic].Partitions) == 0 {
		log.Fatalf("Topic %s doesn't exist or has no partitions", topic)
	}

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	log.Printf("Consuming messages from %v...\n", topic)

	run := true
	for run {
		// msg, err := consumer.ReadMessage(20 * time.Second)
		msg, err := consumer.ReadMessage(10 * time.Second)
		if err == nil {
			log.Printf("Consumed message: %s from %v\n", string(msg.Value), msg.TopicPartition)
		} else {
			// The client will automatically try to recover from all errors.
			log.Printf("Consumer error: %v (%v)\n", err, msg)
			run = false
		}
	}
}
