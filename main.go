package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {

	//connect to kafka
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"security.protocol": "PLAINTEXT",
	})

	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
		panic(err)
	}
	// Delivery report handler for produced messages
	// for i := 0; i <= 20; i++ {
	go func() {
		for e := range producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered to %v\n", ev.TopicPartition)
				}
			}
		}
	}()
	// }
	// Produce a message to a topic
	topic := "test-topic"
	msg := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte("Hello from Go!"),
	}

	for i := 0; i < 1000; i++ {
		err = producer.Produce(msg, nil)
		if err != nil {
			panic(err)
		}
	}
	// Wait for all messages to be delivered
	producer.Flush(1000)
	producer.Close()

	// --- Kafka Consumer (Subscriber) ---
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092, localhost:10092, localhost:11092",
		"group.id":          "go-consumer-group-1",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}

	err = consumer.Subscribe("test-topic", nil)
	if err != nil {
		log.Fatalf("Failed to subscribe to topic: %s", err)
	}

	fmt.Println("Consuming messages from test-topic...")
	run := true
	for run {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Consumed message: %s from %v\n", string(msg.Value), msg.TopicPartition)
		} else {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			run = false
		}
	}
	consumer.Close()

}
